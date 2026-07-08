use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use http::Uri;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::warn;

use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;
use crate::grpc_wrapper::raw_query_service::status::check_status;
use ydb_grpc::ydb_proto::query::SessionState;
use ydb_grpc::ydb_proto::query::session_state::SessionHint;

/// Explicit Query Service session: CreateSession + AttachSession stream kept alive.
#[derive(Clone)]
pub(crate) struct AttachedQuerySession {
    inner: Arc<AttachedQuerySessionInner>,
}

struct AttachedQuerySessionInner {
    session_id: String,
    node_uri: Uri,
    in_use: AtomicUsize,
    alive: AtomicBool,
    attach_task: Mutex<Option<JoinHandle<()>>>,
    explicitly_closed: AtomicBool,
    on_node_shutdown: Arc<dyn Fn(Uri) + Send + Sync>,
    delete_timeout: Duration,
    not_in_use: Notify,
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
    pub async fn open(
        client: &mut RawQueryClient,
        node_uri: Uri,
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
            in_use: AtomicUsize::new(0),
            alive: AtomicBool::new(true),
            attach_task: Mutex::new(None),
            explicitly_closed: AtomicBool::new(false),
            on_node_shutdown,
            delete_timeout,
            not_in_use: Notify::new(),
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

    pub fn begin_use(&self) {
        self.inner.in_use.fetch_add(1, Ordering::SeqCst);
    }

    pub fn end_use(&self) {
        let prev = self.inner.in_use.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(prev > 0, "end_use called when in_use is already 0");
        if prev == 1 {
            self.inner.not_in_use.notify_waiters();
        }
    }

    pub fn is_alive(&self) -> bool {
        self.inner.alive.load(Ordering::Acquire)
    }

    pub(crate) fn invalidate(&self) {
        self.mark_not_alive();
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
        // Bounded wait for in-flight RPCs; `close()` may then spend up to another
        // `delete_timeout` on DeleteSession (worst case ≈ 2× delete_timeout).
        let drain_timeout = self.inner.delete_timeout;
        let _ = timeout(drain_timeout, async {
            loop {
                if self.inner.in_use.load(Ordering::Acquire) == 0 {
                    break;
                }
                let notified = self.inner.not_in_use.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.inner.in_use.load(Ordering::Acquire) == 0 {
                    break;
                }
                notified.await;
            }
        })
        .await;
    }

    /// Abort the attach listener without `DeleteSession` (used when we cannot reach the node).
    pub async fn abort_without_delete(self) {
        self.inner.explicitly_closed.store(true, Ordering::Release);
        self.mark_not_alive();
        if let Some(task) = self.inner.attach_task.lock().await.take() {
            task.abort();
        }
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

#[cfg(test)]
impl AttachedQuerySession {
    /// In-memory session for pool benchmarks: no CreateSession, AttachStream, or DeleteSession.
    pub(crate) fn new_bench_stub(session_id: String, node_uri: Uri) -> Self {
        Self {
            inner: Arc::new(AttachedQuerySessionInner {
                session_id,
                node_uri,
                in_use: AtomicUsize::new(0),
                alive: AtomicBool::new(true),
                attach_task: Mutex::new(None),
                explicitly_closed: AtomicBool::new(false),
                on_node_shutdown: Arc::new(|_| {}),
                delete_timeout: Duration::ZERO,
                not_in_use: Notify::new(),
            }),
        }
    }

    pub(crate) fn bench_close(self) {
        self.inner.explicitly_closed.store(true, Ordering::Release);
        self.inner.alive.store(false, Ordering::Release);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ShutdownHint {
    SessionShutdown,
    NodeShutdown,
}

/// Returns `RawResult` so future hint variants can fail validation without changing call sites.
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
