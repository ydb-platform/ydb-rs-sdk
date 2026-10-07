//! Ownership states for one server-side query session.
//!
//! `CreatedSession` becomes `AttachedSession` after the AttachSession handshake. The pool wraps
//! an attached session as idle or leased state. Explicit lease return makes it idle again;
//! dropping any owning state submits DeleteSession cleanup.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use http::Uri;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::errors::{YdbError, YdbResult, YdbStatusError};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;
use crate::grpc_wrapper::raw_query_service::session_state::RawSessionState;
use ydb_grpc::ydb_proto::query::SessionState;
use ydb_grpc::ydb_proto::status_ids::StatusCode;

use super::pool::SessionPoolObserver;
use super::spawn_pool_release;

/// A server session returned by CreateSession but not attached yet.
pub(super) struct CreatedSession {
    resource: SessionResource,
}

impl CreatedSession {
    pub(super) fn new(
        session_id: String,
        node_uri: Uri,
        cleanup: SessionCleanup,
        pool: std::sync::Weak<super::pool::SessionPoolInner>,
    ) -> Self {
        Self {
            resource: SessionResource::new(cleanup, session_id, node_uri, pool),
        }
    }

    pub(super) async fn attach(
        self,
        client: &mut RawQueryClient,
        observer: SessionPoolObserver,
    ) -> YdbResult<AttachedSession> {
        let identity = self.resource.identity.clone();
        let mut stream = client.attach_session(&identity.session_id).await?;
        let first = stream
            .message()
            .await?
            .ok_or_else(|| YdbError::custom("attach session stream closed before initial state"))?;
        match RawSessionState::try_from(first)? {
            RawSessionState::Active => {}
            RawSessionState::SessionShutdown => {
                return Err(YdbError::custom("query session shutdown hint received"));
            }
            RawSessionState::NodeShutdown => {
                observer.node_shutdown(&identity.node_uri);
                return Err(YdbError::custom("query node shutdown hint received"));
            }
        }

        let health = Arc::new(SessionHealth::new());
        let listener_identity = identity.clone();
        let listener_health = health.clone();
        let listener = tokio::spawn(listen_attach_stream(
            stream,
            listener_identity,
            listener_health,
            observer,
        ));
        Ok(AttachedSession {
            _attach_listener: AttachSessionListener::Task(listener),
            resource: self.resource,
            health,
        })
    }
}

/// A session whose AttachSession handshake succeeded and which owns its stream listener.
pub(super) struct AttachedSession {
    // Fields drop in declaration order: request listener cancellation before `resource`
    // submits session cleanup.
    _attach_listener: AttachSessionListener,
    resource: SessionResource,
    health: Arc<SessionHealth>,
}

/// Why a session became unusable, distinguishing keep-alive (attach stream) failures
/// from other breaks for the close-reason metric.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SessionCloseCause {
    Healthy,
    Broken,
    KeepaliveFailed,
}

impl SessionCloseCause {
    /// Storage discriminants for the atomic close-cause cell in [`SessionHealth`];
    /// named so readers and writers never rely on raw numeric literals.
    const STORED_HEALTHY: u8 = Self::Healthy as u8;
    const STORED_BROKEN: u8 = Self::Broken as u8;
    const STORED_KEEPALIVE_FAILED: u8 = Self::KeepaliveFailed as u8;

    fn from_stored(value: u8) -> Self {
        match value {
            Self::STORED_BROKEN => Self::Broken,
            Self::STORED_KEEPALIVE_FAILED => Self::KeepaliveFailed,
            _ => Self::Healthy,
        }
    }
}

impl AttachedSession {
    pub(super) fn session_id(&self) -> &str {
        &self.resource.identity.session_id
    }

    pub(super) fn node_uri(&self) -> &Uri {
        &self.resource.identity.node_uri
    }

    pub(super) fn is_healthy(&self) -> bool {
        self.health.is_healthy()
    }

    pub(super) fn close_cause(&self) -> SessionCloseCause {
        self.health.close_cause()
    }

    #[cfg(test)]
    pub(super) fn invalidate(&self) {
        self.health.mark_broken();
    }

    pub(super) fn ensure_healthy(&self) -> YdbResult<()> {
        if self.is_healthy() {
            Ok(())
        } else {
            Err(YdbError::YdbStatusError(YdbStatusError::new(
                format!(
                    "query session {} is not healthy",
                    self.resource.identity.session_id
                ),
                StatusCode::BadSession as i32,
                Vec::new(),
            )))
        }
    }

    #[cfg(test)]
    pub(super) fn new_bench_stub(
        session_id: String,
        node_uri: Uri,
        cleanup: SessionCleanup,
        pool: std::sync::Weak<super::pool::SessionPoolInner>,
    ) -> Self {
        let resource = SessionResource::new(cleanup, session_id, node_uri, pool);
        Self {
            _attach_listener: AttachSessionListener::Stub,
            health: Arc::new(SessionHealth::new()),
            resource,
        }
    }

    /// Disarm the abnormal-close emission before an explicit pool close emits its
    /// own, more specific close reason.
    pub(super) fn disarm_close_emission(&mut self) {
        self.resource.close_emission.disarm();
    }
}

/// Sole owner of the server-side session. Dropping it submits session cleanup.
struct SessionResource {
    cleanup: SessionCleanup,
    identity: Arc<SessionIdentity>,
    /// Emits the close metric when the session record is dropped while the close
    /// emission is still armed — the abnormal lease drop path. Explicit pool close
    /// sites disarm it first and emit their own, more specific reason.
    close_emission: SessionCloseEmission,
}

impl SessionResource {
    fn new(
        cleanup: SessionCleanup,
        session_id: String,
        node_uri: Uri,
        pool: std::sync::Weak<super::pool::SessionPoolInner>,
    ) -> Self {
        Self {
            cleanup,
            identity: Arc::new(SessionIdentity {
                session_id,
                node_uri,
            }),
            close_emission: SessionCloseEmission::new(pool),
        }
    }
}

/// RAII guard counting an abnormal session close (lease dropped without returning
/// the session to the pool). Disarmed by every explicit close site.
struct SessionCloseEmission {
    pool: std::sync::Weak<super::pool::SessionPoolInner>,
    armed: bool,
}

impl SessionCloseEmission {
    fn new(pool: std::sync::Weak<super::pool::SessionPoolInner>) -> Self {
        Self { pool, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SessionCloseEmission {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(pool) = self.pool.upgrade() {
            pool.record_abnormal_session_close();
        }
    }
}

impl Drop for SessionResource {
    fn drop(&mut self) {
        self.cleanup.submit_delete(self.identity.clone());
    }
}

/// Immutable identity shared by the AttachSession listener and cleanup path.
struct SessionIdentity {
    session_id: String,
    node_uri: Uri,
}

/// One-way health signal shared with the AttachSession listener.
///
/// No other data is published through this flag, so relaxed ordering is sufficient.
struct SessionHealth {
    healthy: AtomicBool,
    /// Close cause recorded with the first break; the attach stream listener records
    /// keep-alive failures so the close metric can attribute them.
    close_cause: AtomicU8,
}

impl SessionHealth {
    fn new() -> Self {
        Self {
            healthy: AtomicBool::new(true),
            close_cause: AtomicU8::new(SessionCloseCause::STORED_HEALTHY),
        }
    }

    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    fn close_cause(&self) -> SessionCloseCause {
        let cause = SessionCloseCause::from_stored(self.close_cause.load(Ordering::Relaxed));
        match cause {
            SessionCloseCause::Healthy if !self.is_healthy() => SessionCloseCause::Broken,
            cause => cause,
        }
    }

    fn mark_broken(&self) {
        self.healthy.store(false, Ordering::Relaxed);
        // First cause wins: a later generic break must not overwrite a recorded
        // keep-alive failure, and a failed CAS only means a cause is already set.
        let _ = self.close_cause.compare_exchange(
            SessionCloseCause::STORED_HEALTHY,
            SessionCloseCause::STORED_BROKEN,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }

    fn mark_keepalive_failed(&self) {
        self.healthy.store(false, Ordering::Relaxed);
        self.close_cause.store(
            SessionCloseCause::STORED_KEEPALIVE_FAILED,
            Ordering::Relaxed,
        );
    }
}

/// Owns the task that monitors the AttachSession stream.
enum AttachSessionListener {
    Task(JoinHandle<()>),
    #[cfg(test)]
    Stub,
}

impl Drop for AttachSessionListener {
    fn drop(&mut self) {
        match self {
            Self::Task(task) => task.abort(),
            #[cfg(test)]
            Self::Stub => {}
        }
    }
}

/// Submits best-effort deletion for a server-side session resource.
#[derive(Clone)]
pub(super) struct SessionCleanup {
    connection_manager: GrpcConnectionManager,
    delete_timeout: Duration,
}

impl SessionCleanup {
    pub(super) fn new(connection_manager: GrpcConnectionManager, delete_timeout: Duration) -> Self {
        Self {
            connection_manager,
            delete_timeout,
        }
    }

    fn submit_delete(&self, identity: Arc<SessionIdentity>) {
        if cfg!(test) {
            // Cleanup submission remains suppressed in test binaries until it is routed through
            // the cleanup worker's testable command channel.
            return;
        }

        let connection_manager = self.connection_manager.clone();
        let delete_timeout = self.delete_timeout;
        spawn_pool_release(async move {
            let mut client = match connection_manager
                .get_auth_service_to_node(RawQueryClient::new, &identity.node_uri)
                .await
            {
                Ok(client) => client,
                Err(err) => {
                    warn!(session_id = %identity.session_id, error = %err, "failed to connect for DeleteSession");
                    return;
                }
            };

            match tokio::time::timeout(delete_timeout, client.delete_session(&identity.session_id))
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    warn!(session_id = %identity.session_id, error = %err, "DeleteSession failed")
                }
                Err(_) => {
                    warn!(session_id = %identity.session_id, ?delete_timeout, "DeleteSession timed out")
                }
            }
        });
    }
}

async fn listen_attach_stream(
    stream: tonic::Streaming<SessionState>,
    identity: Arc<SessionIdentity>,
    health: Arc<SessionHealth>,
    observer: SessionPoolObserver,
) {
    let mut events = stream.map(|message| {
        message
            .map_err(YdbError::from)
            .and_then(|message| RawSessionState::try_from(message).map_err(YdbError::from))
    });
    let mut keepalive_failed = false;
    let node_shutdown = loop {
        match events.next().await {
            Some(Ok(RawSessionState::Active)) => {}
            Some(Ok(RawSessionState::SessionShutdown)) => {
                warn!(
                    session_id = %identity.session_id,
                    hint = ?RawSessionState::SessionShutdown,
                    "query session attach listener received a shutdown hint"
                );
                break false;
            }
            Some(Ok(RawSessionState::NodeShutdown)) => {
                warn!(
                    session_id = %identity.session_id,
                    hint = ?RawSessionState::NodeShutdown,
                    "query session attach listener received a shutdown hint"
                );
                break true;
            }
            Some(Err(err)) => {
                warn!(
                    session_id = %identity.session_id,
                    error = %err,
                    "query session attach listener failed"
                );
                keepalive_failed = true;
                break false;
            }
            None => {
                warn!(
                    session_id = %identity.session_id,
                    "query session attach stream closed"
                );
                break false;
            }
        }
    };

    observer.session_keepalive_finished(!keepalive_failed);
    if keepalive_failed {
        // The liveness stream failed: record it as a keep-alive failure so the
        // session close metric can attribute the close to it.
        health.mark_keepalive_failed();
    }
    health.mark_broken();
    if node_shutdown {
        observer.node_shutdown(&identity.node_uri);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GrpcOptions;
    use crate::client_metrics::DefaultMetricsRecorder;
    use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
    use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};

    fn connection_manager() -> GrpcConnectionManager {
        GrpcConnectionManager::new(
            SharedLoadBalancer::new_with_balancer(Box::new(StaticLoadBalancer::new(
                Uri::from_static("http://127.0.0.1/bench"),
            ))),
            "bench".to_string(),
            MultiInterceptor::new(),
            GrpcOptions::default(),
            Arc::new(DefaultMetricsRecorder::new()),
        )
    }

    #[tokio::test]
    async fn attached_session_drop_aborts_its_listener() {
        struct NotifyOnDrop(Option<tokio::sync::oneshot::Sender<()>>);

        impl Drop for NotifyOnDrop {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let cleanup = SessionCleanup::new(connection_manager(), Duration::ZERO);
        let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
        let (dropped_sender, dropped_receiver) = tokio::sync::oneshot::channel();
        let listener = tokio::spawn(async move {
            let _notify = NotifyOnDrop(Some(dropped_sender));
            let _ = ready_sender.send(());
            std::future::pending::<()>().await;
        });
        ready_receiver.await.expect("listener must start");
        let resource = SessionResource::new(
            cleanup,
            "attached".to_string(),
            Uri::from_static("http://127.0.0.1/bench"),
            std::sync::Weak::new(),
        );
        let session = AttachedSession {
            _attach_listener: AttachSessionListener::Task(listener),
            health: Arc::new(SessionHealth::new()),
            resource,
        };

        drop(session);

        tokio::time::timeout(Duration::from_secs(1), dropped_receiver)
            .await
            .expect("listener must be aborted")
            .expect("listener drop notification must arrive");
    }
}
