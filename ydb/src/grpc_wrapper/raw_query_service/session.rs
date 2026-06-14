use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use http::Uri;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tracing::warn;

use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;
use crate::grpc_wrapper::raw_query_service::status::check_status;
use ydb_grpc::ydb_proto::query::session_state::SessionHint;
use ydb_grpc::ydb_proto::query::SessionState;

/// Explicit Query Service session: CreateSession + AttachSession stream kept alive.
#[derive(Clone)]
pub(crate) struct AttachedQuerySession {
    inner: Arc<AttachedQuerySessionInner>,
}

struct AttachedQuerySessionInner {
    session_id: String,
    node_uri: Uri,
    node_id: u64,
    in_use: AtomicUsize,
    alive: AtomicBool,
    attach_task: Mutex<Option<JoinHandle<()>>>,
    explicitly_closed: AtomicBool,
    on_node_shutdown: Arc<dyn Fn(Uri) + Send + Sync>,
    delete_timeout: Duration,
}

impl Drop for AttachedQuerySession {
    fn drop(&mut self) {
        if !self.inner.explicitly_closed.load(Ordering::Acquire)
            && Arc::strong_count(&self.inner) == 1
        {
            warn!(
                session_id = %self.inner.session_id,
                "query session dropped without explicit close; server-side session may leak until idle timeout"
            );
        }
    }
}

impl AttachedQuerySession {
    pub async fn create_and_open(
        client: &mut RawQueryClient,
        node_uri: Uri,
        on_node_shutdown: Arc<dyn Fn(Uri) + Send + Sync>,
        delete_timeout: Duration,
    ) -> RawResult<Self> {
        let created = client.create_session().await?;
        Self::open(
            client,
            node_uri,
            created.node_id,
            created.session_id,
            on_node_shutdown,
            delete_timeout,
        )
        .await
    }

    pub async fn open(
        client: &mut RawQueryClient,
        node_uri: Uri,
        node_id: u64,
        session_id: String,
        on_node_shutdown: Arc<dyn Fn(Uri) + Send + Sync>,
        delete_timeout: Duration,
    ) -> RawResult<Self> {
        let mut attach_stream = client.attach_session(&session_id).await?;
        let first = attach_stream
            .message()
            .await?
            .ok_or_else(|| RawError::custom("attach session stream closed"))?;
        if let Some(hint) = shutdown_hint(&first)? {
            return Err(shutdown_hint_error(hint));
        }
        check_attach_state(&first)?;

        let inner = Arc::new(AttachedQuerySessionInner {
            session_id,
            node_uri,
            node_id,
            in_use: AtomicUsize::new(0),
            alive: AtomicBool::new(true),
            attach_task: Mutex::new(None),
            explicitly_closed: AtomicBool::new(false),
            on_node_shutdown,
            delete_timeout,
        });

        let session_for_task = AttachedQuerySession {
            inner: inner.clone(),
        };
        let attach_task = tokio::spawn(async move {
            session_for_task.listen_attach_stream(attach_stream).await;
        });
        *inner.attach_task.lock().await = Some(attach_task);

        Ok(Self { inner })
    }

    pub fn session_id(&self) -> &str {
        &self.inner.session_id
    }

    pub fn node_uri(&self) -> &Uri {
        &self.inner.node_uri
    }

    pub fn node_id(&self) -> u64 {
        self.inner.node_id
    }

    pub fn begin_use(&self) {
        self.inner.in_use.fetch_add(1, Ordering::SeqCst);
    }

    pub fn end_use(&self) {
        self.inner.in_use.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn is_alive(&self) -> bool {
        self.inner.alive.load(Ordering::Acquire)
    }

    pub fn ensure_alive(&self) -> RawResult<()> {
        if self.is_alive() {
            Ok(())
        } else {
            Err(RawError::custom(
                "query session is not alive; acquire a new session from the pool",
            ))
        }
    }

    async fn listen_attach_stream(&self, mut attach_stream: tonic::Streaming<SessionState>) {
        while self.is_alive() {
            match attach_stream.message().await {
                Ok(Some(state)) => match shutdown_hint(&state) {
                    Ok(Some(hint)) => {
                        self.on_shutdown_hint(hint);
                        break;
                    }
                    Ok(None) => {
                        if check_attach_state(&state).is_err() {
                            self.mark_not_alive();
                            break;
                        }
                    }
                    Err(_) => {
                        self.mark_not_alive();
                        break;
                    }
                },
                Ok(None) | Err(_) => {
                    self.mark_not_alive();
                    break;
                }
            }
        }
    }

    fn on_shutdown_hint(&self, hint: ShutdownHint) {
        if hint == ShutdownHint::NodeShutdown {
            (self.inner.on_node_shutdown)(self.inner.node_uri.clone());
        }
        self.mark_not_alive();
    }

    fn mark_not_alive(&self) {
        self.inner.alive.store(false, Ordering::Release);
    }

    async fn wait_not_in_use(&self) {
        let drain_timeout = self.inner.delete_timeout;
        let _ = timeout(drain_timeout, async {
            while self.inner.in_use.load(Ordering::Acquire) > 0 {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
    }

    pub async fn close(self, client: &mut RawQueryClient) {
        self.inner.explicitly_closed.store(true, Ordering::Release);
        self.mark_not_alive();
        if let Some(task) = self.inner.attach_task.lock().await.take() {
            task.abort();
        }
        self.wait_not_in_use().await;
        let delete_timeout = self.inner.delete_timeout;
        let session_id = self.inner.session_id.clone();
        let _ = timeout(delete_timeout, client.delete_session(&session_id)).await;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ShutdownHint {
    SessionShutdown,
    NodeShutdown,
}

fn shutdown_hint(state: &SessionState) -> RawResult<Option<ShutdownHint>> {
    match &state.session_hint {
        Some(SessionHint::SessionShutdown(_)) => Ok(Some(ShutdownHint::SessionShutdown)),
        Some(SessionHint::NodeShutdown(_)) => Ok(Some(ShutdownHint::NodeShutdown)),
        None => Ok(None),
    }
}

fn shutdown_hint_error(hint: ShutdownHint) -> RawError {
    match hint {
        ShutdownHint::SessionShutdown => RawError::custom("query session shutdown hint received"),
        ShutdownHint::NodeShutdown => RawError::custom("query node shutdown hint received"),
    }
}

fn check_attach_state(state: &SessionState) -> RawResult<()> {
    check_status(state.status, &state.issues)
}

/// Implicit pool item: no CreateSession/AttachSession; ExecuteQuery uses an empty session id.
pub(crate) struct ImplicitQuerySession {
    in_use: AtomicUsize,
    alive: AtomicBool,
}

impl Default for ImplicitQuerySession {
    fn default() -> Self {
        Self::new()
    }
}

impl ImplicitQuerySession {
    pub fn new() -> Self {
        Self {
            in_use: AtomicUsize::new(0),
            alive: AtomicBool::new(true),
        }
    }

    pub fn session_id(&self) -> &str {
        ""
    }

    pub fn begin_use(&self) {
        self.in_use.fetch_add(1, Ordering::SeqCst);
    }

    pub fn end_use(&self) {
        self.in_use.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    pub fn ensure_alive(&self) -> RawResult<()> {
        if self.is_alive() {
            Ok(())
        } else {
            Err(RawError::custom("implicit query session is closed"))
        }
    }

    pub fn close(&self) {
        self.alive.store(false, Ordering::Release);
    }
}
