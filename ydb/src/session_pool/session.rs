//! Ownership states for one server-side query session.
//!
//! `CreatedSession` becomes `AttachedSession` after the AttachSession handshake. The pool wraps
//! an attached session as idle or leased state. Explicit lease return makes it idle again;
//! dropping any owning state submits DeleteSession cleanup.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures_util::StreamExt;
use http::Uri;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::errors::{YdbError, YdbResult, YdbStatusError};
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;
use crate::grpc_wrapper::raw_query_service::session_state::RawSessionState;
use ydb_grpc::ydb_proto::query::SessionState;
use ydb_grpc::ydb_proto::status_ids::StatusCode;

use super::cleanup_worker::SessionCleanup;
use super::pool::SessionPoolObserver;

/// Immutable identity shared by the session owner, AttachSession listener, and cleanup worker.
#[derive(Debug)]
pub(super) struct SessionIdentity {
    pub(super) session_id: String,
    pub(super) node_uri: Uri,
}

/// A server session returned by CreateSession but not attached yet.
pub(super) struct CreatedSession {
    resource: SessionResource,
}

impl CreatedSession {
    pub(super) fn new(session_id: String, node_uri: Uri, cleanup: SessionCleanup) -> Self {
        Self {
            resource: SessionResource::new(cleanup, session_id, node_uri),
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

        let health = Arc::new(SessionHealth {
            healthy: AtomicBool::new(true),
        });
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
    ) -> Self {
        let resource = SessionResource::new(cleanup, session_id, node_uri);
        Self {
            _attach_listener: AttachSessionListener::Stub,
            health: Arc::new(SessionHealth {
                healthy: AtomicBool::new(true),
            }),
            resource,
        }
    }
}

/// Sole owner of the server-side session. Dropping it submits session cleanup.
struct SessionResource {
    cleanup: SessionCleanup,
    identity: Arc<SessionIdentity>,
}

impl SessionResource {
    fn new(cleanup: SessionCleanup, session_id: String, node_uri: Uri) -> Self {
        Self {
            cleanup,
            identity: Arc::new(SessionIdentity {
                session_id,
                node_uri,
            }),
        }
    }
}

impl Drop for SessionResource {
    fn drop(&mut self) {
        self.cleanup.submit_delete(self.identity.clone());
    }
}

/// One-way health signal shared with the AttachSession listener.
///
/// No other data is published through this flag, so relaxed ordering is sufficient.
struct SessionHealth {
    healthy: AtomicBool,
}

impl SessionHealth {
    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    fn mark_broken(&self) {
        self.healthy.store(false, Ordering::Relaxed);
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

    health.mark_broken();
    if node_shutdown {
        observer.node_shutdown(&identity.node_uri);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_pool::cleanup_worker::start_noop_session_cleanup_worker;
    use std::time::Duration;

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

        let cleanup = start_noop_session_cleanup_worker();
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
        );
        let session = AttachedSession {
            _attach_listener: AttachSessionListener::Task(listener),
            health: Arc::new(SessionHealth {
                healthy: AtomicBool::new(true),
            }),
            resource,
        };

        drop(session);

        tokio::time::timeout(Duration::from_secs(1), dropped_receiver)
            .await
            .expect("listener must be aborted")
            .expect("listener drop notification must arrive");
    }
}
