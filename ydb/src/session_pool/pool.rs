use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use http::Uri;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{instrument, trace, warn};

use crate::client_metrics::{
    MetricsRecorder, SessionPoolAcquireResult, SessionPoolCloseReason, SessionPoolGaugeSnapshot,
};
use crate::discovery::Discovery;
use crate::errors::{YdbError, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;
use crate::grpc_wrapper::raw_services::Service;

use super::session::{AttachedSession, CreatedSession, SessionCleanup, SessionCloseCause};

/// Default pool size for [`SessionPoolSettings::default()`] and the driver built-in pool.
///
/// Matches ydb-go-sdk `pool.DefaultLimit` (50). The legacy table-only session pool
/// defaulted to 1000; callers migrating from that capacity should set
/// `SessionPoolSettings::new().with_limit(1000)` explicitly.
pub(crate) const DEFAULT_POOL_LIMIT: usize = 50;
/// Max time for CreateSession + AttachSession before the attempt is abandoned.
///
/// Matches ydb-go-sdk `table.DefaultSessionPoolCreateSessionTimeout` (5s). The previous
/// 500ms budget was tight enough that an ordinary latency spike — a cold connection, a
/// loaded or virtualized server — failed session creation instead of waiting it out.
pub(crate) const DEFAULT_SESSION_CREATE_TIMEOUT: Duration = Duration::from_secs(5);
/// Max time for the best-effort session cleanup RPC. Cleanup runs off the caller's path
/// and must not hold resources, so it keeps the short budget.
pub(crate) const DEFAULT_SESSION_DELETE_TIMEOUT: Duration = Duration::from_millis(500);
/// Default max wait when acquiring a session from the pool.
pub(crate) const DEFAULT_POOL_ACQUIRE_TIMEOUT: Duration = Duration::ZERO;

/// Ensures `create_in_progress` is decremented when the outer future is dropped
/// (e.g. per-call `with_operation_timeout` cancelling pool acquire + create).
struct CreateInProgressGuard<'a> {
    inner: &'a SessionPoolInner,
}

impl Drop for CreateInProgressGuard<'_> {
    fn drop(&mut self) {
        self.inner.create_in_progress.fetch_sub(1, Ordering::SeqCst);
        self.inner.update_session_pool_gauges();
    }
}

/// Pairs with the manual `pending_acquires` increment in `acquire_permit`; keeps the
/// pending gauge correct when the wait is cancelled or times out.
struct PendingAcquireGuard<'a> {
    inner: &'a SessionPoolInner,
}

impl Drop for PendingAcquireGuard<'_> {
    fn drop(&mut self) {
        self.inner.pending_acquires.fetch_sub(1, Ordering::SeqCst);
        self.inner.update_session_pool_gauges();
    }
}

fn normalize_pool_settings(mut settings: SessionPoolSettings) -> SessionPoolSettings {
    settings.limit = settings.limit.max(1);
    settings.warm_up = settings.warm_up.min(settings.limit);
    settings
}

pub(crate) fn spawn_pool_release<F>(future: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            handle.spawn(future);
        }
        Err(_) => {
            warn!("no active tokio runtime; skipping async session pool release during shutdown");
        }
    }
}

/// Settings for the driver session pool (CreateSession + AttachSession).
#[derive(Clone, Debug)]
pub struct SessionPoolSettings {
    /// Maximum concurrent sessions (pool size limit).
    ///
    /// Default is **50** (ydb-go-sdk parity). The legacy table-only pool defaulted to **1000**;
    /// after upgrading, callers that relied on the old default should set
    /// `SessionPoolSettings::new().with_limit(1000)` explicitly or tune via
    /// [`crate::Client::with_session_pool`].
    ///
    /// Normalized to at least 1 when a pool is created (`with_limit` and pool constructors
    /// apply the same rule).
    pub limit: usize,
    /// Minimum sessions to pre-create at pool initialization (warm-up).
    pub warm_up: usize,
    /// Close a session after this many uses (0 = unlimited).
    pub item_usage_limit: u64,
    /// Close a session after this wall-clock lifetime (0 = unlimited).
    pub item_usage_ttl: Duration,
    /// Close idle sessions after this duration (0 = unlimited).
    pub idle_ttl: Duration,
    pub session_create_timeout: Duration,
    /// Maximum time for a best-effort session cleanup RPC, including `DeleteSession` and
    /// transaction rollback performed before a session can be reused.
    pub session_delete_timeout: Duration,
    /// Max wait when [`SessionPool::acquire_explicit`] blocks on the pool semaphore.
    pub acquire_timeout: Duration,
}

impl Default for SessionPoolSettings {
    fn default() -> Self {
        Self {
            limit: DEFAULT_POOL_LIMIT,
            warm_up: 0,
            item_usage_limit: 0,
            item_usage_ttl: Duration::ZERO,
            idle_ttl: Duration::ZERO,
            session_create_timeout: DEFAULT_SESSION_CREATE_TIMEOUT,
            session_delete_timeout: DEFAULT_SESSION_DELETE_TIMEOUT,
            acquire_timeout: DEFAULT_POOL_ACQUIRE_TIMEOUT,
        }
    }
}

/// Snapshot of session pool counters (aligned with go-sdk `pool.Stats`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionPoolStats {
    /// Configured maximum concurrent in-flight sessions (`limit`).
    pub limit: usize,
    /// Configured warm-up target (`warm_up`).
    pub warm_up: usize,
    /// Total live sessions: idle + in_use.
    pub size: usize,
    /// Sessions waiting in the idle stack.
    pub idle: usize,
    /// Sessions currently leased to callers (holding a semaphore permit).
    pub in_use: usize,
    /// CreateSession RPCs in progress.
    pub create_in_progress: usize,
    /// Total successful explicit session creations (CreateSession + Attach) since pool init.
    pub sessions_created: u64,
}

impl SessionPoolSettings {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit.max(1);
        self
    }

    pub fn with_warm_up(mut self, warm_up: usize) -> Self {
        self.warm_up = warm_up;
        self
    }

    pub fn with_item_usage_limit(mut self, limit: u64) -> Self {
        self.item_usage_limit = limit;
        self
    }

    pub fn with_item_usage_ttl(mut self, ttl: Duration) -> Self {
        self.item_usage_ttl = ttl;
        self
    }

    pub fn with_idle_ttl(mut self, ttl: Duration) -> Self {
        self.idle_ttl = ttl;
        self
    }

    /// Maximum time for CreateSession + AttachSession when the pool creates a session.
    pub fn with_session_create_timeout(mut self, timeout: Duration) -> Self {
        self.session_create_timeout = timeout;
        self
    }

    /// Maximum time for a best-effort session cleanup RPC.
    pub fn with_session_delete_timeout(mut self, timeout: Duration) -> Self {
        self.session_delete_timeout = timeout;
        self
    }

    /// Maximum time to wait for a free session when the pool is at capacity.
    pub fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }
}

/// Pooled explicit session lease. Not concurrent-safe: one logical owner at a time.
///
/// Call [`Self::return_to_pool`] to make a healthy session reusable. Dropping a lease without
/// returning it schedules session cleanup (see `SessionResource`).
///
/// The payload is boxed so holding a lease (in transaction and stream states) stays small.
pub(crate) struct SessionPoolLease {
    inner: Box<LeaseInner>,
}

struct LeaseInner {
    /// One permit represents this lease's exclusive use of one pool-capacity slot.
    permit: OwnedSemaphorePermit,
    record: SessionRecord,
    pool: Arc<SessionPoolInner>,
}

impl SessionPoolLease {
    fn new(
        session: IdleSession,
        permit: OwnedSemaphorePermit,
        pool: Arc<SessionPoolInner>,
    ) -> Self {
        let mut record = session.record;
        // The session-use metric measures from the lease start; `last_used` doubles as
        // the use-start marker until the release path recomputes it.
        record.last_used = Instant::now();
        Self {
            inner: Box::new(LeaseInner {
                permit,
                record,
                pool,
            }),
        }
    }

    pub fn session_id(&self) -> &str {
        self.inner.record.session.session_id()
    }

    pub fn node_uri(&self) -> &Uri {
        self.inner.record.session.node_uri()
    }

    pub fn ensure_healthy(&self) -> YdbResult<()> {
        self.inner.record.session.ensure_healthy()
    }

    pub(crate) fn cleanup_timeout(&self) -> Duration {
        self.inner.pool.settings.session_delete_timeout
    }

    /// Consume this lease and offer its session back to the pool. Pool policy may still discard
    /// an unhealthy, expired, or excess session.
    #[instrument(name = "ydb.SessionPool.ReturnSession", skip_all, fields(db.system.name = "ydb"))]
    pub fn return_to_pool(self) {
        let LeaseInner {
            permit,
            record,
            pool,
        } = *self.inner;
        pool.release_explicit_session(record, permit);
    }

    /// Resolve this lease after a terminal operation and return the operation result unchanged.
    pub(crate) fn finish<T>(self, result: YdbResult<T>) -> YdbResult<T> {
        match result {
            Ok(value) => {
                self.return_to_pool();
                Ok(value)
            }
            Err(err) => {
                if !err.requires_session_discard() {
                    self.return_to_pool();
                }
                Err(err)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn invalidate(&mut self) {
        self.inner.record.session.invalidate();
    }
}

#[derive(Clone)]
pub(crate) struct SessionPool {
    inner: Arc<SessionPoolInner>,
}

/// Session data shared by the pool ownership states below.
struct SessionRecord {
    pub(crate) session: AttachedSession,
    created: Instant,
    last_used: Instant,
    use_count: u64,
}

/// A session owned by the pool and available for checkout.
struct IdleSession {
    record: SessionRecord,
}

/// Receives AttachSession lifecycle events without giving listener tasks ownership of the pool.
#[derive(Clone)]
pub(super) struct SessionPoolObserver {
    discovery: Arc<dyn Discovery>,
    /// Weak ownership avoids a cycle through `SessionPoolInner::observer`.
    pool: Weak<SessionPoolInner>,
}

impl SessionPoolObserver {
    /// Weak handle to the owning pool, shared with created sessions so abnormal
    /// session drops can emit close metrics.
    pub(super) fn pool_weak(&self) -> std::sync::Weak<SessionPoolInner> {
        self.pool.clone()
    }

    pub(super) fn node_shutdown(&self, node_uri: &Uri) {
        self.discovery.pessimization(node_uri);
        let Some(pool) = self.pool.upgrade() else {
            return;
        };
        pool.drain_idle_for_node(node_uri);
    }

    /// Report the outcome of a session's attach-stream (liveness) watcher.
    pub(super) fn session_keepalive_finished(&self, ok: bool) {
        if let Some(pool) = self.pool.upgrade() {
            pool.session_keepalive_recorded(ok);
        }
    }
}

pub(super) struct SessionPoolInner {
    settings: SessionPoolSettings,
    acquire_timeout_ms: AtomicU64,
    connection_manager: GrpcConnectionManager,
    semaphore: Arc<Semaphore>,
    explicit_idle: Mutex<Vec<IdleSession>>,
    cleanup: SessionCleanup,
    observer: SessionPoolObserver,
    create_in_progress: AtomicUsize,
    sessions_created: AtomicU64,
    /// Callers currently waiting for a free session (pending gauge).
    pending_acquires: AtomicUsize,
    pub(super) metrics: Arc<dyn MetricsRecorder>,
    /// Stub create/close paths without RPC (see `session_pool_bench` and regression tests).
    #[cfg(test)]
    bench_mode: bool,
    #[cfg(test)]
    bench_create_failures_remaining: AtomicUsize,
}

impl Drop for SessionPoolInner {
    fn drop(&mut self) {
        // Pool teardown: every session still idle is closed with the `shutdown`
        // reason (spec §5 maps pool drop to `shutdown`). Leases hold an `Arc` to
        // this struct, so by the time it drops no lease exists and the only
        // remaining sessions are the idle ones owned by `explicit_idle`; they are
        // dropped with the struct below and their close emissions are disarmed so
        // only the shutdown reason is counted. Gauges are not re-emitted: the pool
        // no longer exists, so its gauges carry no further meaning.
        let remaining = {
            let mut idle = self
                .explicit_idle
                // A poisoned lock must not panic during teardown unwinding; the
                // guard contents stay valid and are still drained.
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            idle.drain(..).collect::<Vec<_>>()
        };
        for mut item in remaining {
            item.record.session.disarm_close_emission();
            self.metrics
                .session_pool_session_closed(SessionPoolCloseReason::Shutdown);
        }
    }
}

impl SessionPool {
    pub fn new_explicit_sync(
        connection_manager: GrpcConnectionManager,
        discovery: Arc<dyn Discovery>,
        settings: SessionPoolSettings,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        let settings = normalize_pool_settings(settings);
        let limit = settings.limit;
        metrics.session_pool_gauges(SessionPoolGaugeSnapshot::initial(limit));
        let inner = Arc::new_cyclic(
            |weak: &std::sync::Weak<SessionPoolInner>| SessionPoolInner {
                settings: settings.clone(),
                // Settings-based (not zero) so tests can exercise acquire-timeout paths.
                acquire_timeout_ms: AtomicU64::new(settings.acquire_timeout.as_millis() as u64),
                connection_manager: connection_manager.clone(),
                semaphore: Arc::new(Semaphore::new(limit)),
                explicit_idle: Mutex::new(Vec::new()),
                cleanup: SessionCleanup::new(
                    connection_manager.clone(),
                    settings.session_delete_timeout,
                ),
                observer: SessionPoolObserver {
                    discovery: discovery.clone(),
                    pool: weak.clone(),
                },
                create_in_progress: AtomicUsize::new(0),
                sessions_created: AtomicU64::new(0),
                pending_acquires: AtomicUsize::new(0),
                metrics,
                #[cfg(test)]
                bench_mode: false,
                #[cfg(test)]
                bench_create_failures_remaining: AtomicUsize::new(0),
            },
        );

        Self { inner }
    }

    #[instrument(name = "ydb.SessionPool.Initialize", skip_all, fields(db.system.name = "ydb"), err)]
    pub async fn new_explicit(
        connection_manager: GrpcConnectionManager,
        discovery: Arc<dyn Discovery>,
        settings: SessionPoolSettings,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> YdbResult<Self> {
        let settings = normalize_pool_settings(settings);
        let warm_up = settings.warm_up;
        let pool = Self::new_explicit_sync(connection_manager, discovery, settings, metrics);

        if warm_up > 0 {
            SessionPoolInner::warm_up_parallel(pool.inner.clone(), warm_up).await?;
        }

        Ok(pool)
    }

    pub fn stats(&self) -> SessionPoolStats {
        self.inner.stats()
    }

    #[instrument(name = "ydb.SessionPool.AcquireSession", skip_all, fields(db.system.name = "ydb"), err)]
    pub async fn acquire_explicit(&self) -> YdbResult<SessionPoolLease> {
        let start = Instant::now();
        let result = self.acquire_explicit_inner().await;
        let outcome = match &result {
            Ok(_) => SessionPoolAcquireResult::Ok,
            Err(AcquireSessionError::Timeout(_)) => SessionPoolAcquireResult::Timeout,
            Err(AcquireSessionError::Other(_)) => SessionPoolAcquireResult::Error,
        };
        self.inner
            .metrics
            .session_pool_acquire(outcome, start.elapsed());
        result.map_err(|err| match err {
            AcquireSessionError::Timeout(error) | AcquireSessionError::Other(error) => error,
        })
    }

    async fn acquire_explicit_inner(&self) -> Result<SessionPoolLease, AcquireSessionError> {
        let permit = self
            .inner
            .acquire_permit()
            .await
            .map_err(AcquireSessionError::from)?;

        while let Some(item) = self.inner.pop_explicit_idle() {
            if let Some(reason) = self.inner.session_close_reason(&item.record) {
                let mut item = item;
                item.record.session.disarm_close_emission();
                self.inner.metrics.session_pool_session_closed(reason);
                drop(item);
                continue;
            }
            trace!(
                session_id = item.record.session.session_id(),
                "got query session from pool"
            );
            self.inner.update_session_pool_gauges();
            return Ok(SessionPoolLease::new(item, permit, self.inner.clone()));
        }

        match self.inner.create_explicit_session().await {
            Ok(item) => {
                trace!(
                    session_id = item.record.session.session_id(),
                    "created query session for pool"
                );
                self.inner.update_session_pool_gauges();
                Ok(SessionPoolLease::new(item, permit, self.inner.clone()))
            }
            Err(error) => {
                self.inner.update_session_pool_gauges();
                Err(AcquireSessionError::Other(error))
            }
        }
    }
}

/// Typed semaphore wait failure: distinguishes timeout from other errors for the
/// acquire metrics without matching on error message text.
enum AcquirePermitError {
    Timeout(YdbError),
    Closed,
}

/// Adds the create-session failure case on top of [`AcquirePermitError`].
enum AcquireSessionError {
    Timeout(YdbError),
    Other(YdbError),
}

impl From<AcquirePermitError> for AcquireSessionError {
    fn from(value: AcquirePermitError) -> Self {
        match value {
            AcquirePermitError::Timeout(error) => Self::Timeout(error),
            AcquirePermitError::Closed => {
                Self::Other(YdbError::Transport("session pool closed".to_string()))
            }
        }
    }
}

impl SessionPoolInner {
    /// Count a session close that was not handled by an explicit pool close site:
    /// the session record was dropped while its close emission was armed.
    pub(super) fn record_abnormal_session_close(&self) {
        self.metrics
            .session_pool_session_closed(SessionPoolCloseReason::BadSession);
        self.update_session_pool_gauges();
    }

    /// Record the outcome of a session's attach-stream (liveness) watcher.
    pub(super) fn session_keepalive_recorded(&self, ok: bool) {
        self.metrics.session_pool_keepalive(ok);
    }
    fn acquire_timeout(&self) -> Duration {
        Duration::from_millis(self.acquire_timeout_ms.load(Ordering::Relaxed))
    }

    async fn acquire_permit(&self) -> Result<OwnedSemaphorePermit, AcquirePermitError> {
        let acquire_timeout = self.acquire_timeout();
        let acquire = self.semaphore.clone().acquire_owned();

        self.pending_acquires.fetch_add(1, Ordering::SeqCst);
        self.update_session_pool_gauges();
        // RAII pairing: the decrement must run even when the acquire future is
        // cancelled mid-await (dropped `acquire_explicit`), otherwise the pending
        // gauge leaks upward forever. Binding (not `let _ =`) keeps the guard alive
        // until scope end.
        let _pending = PendingAcquireGuard { inner: self };
        let waited = if acquire_timeout.is_zero() {
            acquire.await
        } else {
            match tokio::time::timeout(acquire_timeout, acquire).await {
                Ok(permit) => permit,
                Err(_) => {
                    return Err(AcquirePermitError::Timeout(YdbError::Transport(format!(
                        "acquire session from pool timed out after {acquire_timeout:?}"
                    ))));
                }
            }
        };
        // `pending` (and the gauge decrement) drops at scope end, after the wait is over.
        waited.map_err(|_| AcquirePermitError::Closed)
    }

    /// Re-emit the session pool gauges from the current internal counters.
    ///
    /// Called synchronously at every pool mutation site so gauge values never lag
    /// the actual pool state; no timer-based snapshots are involved.
    pub(super) fn update_session_pool_gauges(&self) {
        let stats = self.stats();
        self.metrics.session_pool_gauges(SessionPoolGaugeSnapshot {
            idle: stats.idle as f64,
            active: stats.in_use as f64,
            creating: stats.create_in_progress as f64,
            limit: stats.limit as f64,
            pending: self.pending_acquires.load(Ordering::SeqCst) as f64,
        });
    }

    fn stats(&self) -> SessionPoolStats {
        let idle = self.explicit_idle.lock().expect("explicit idle lock").len();
        let permits_held = self
            .settings
            .limit
            .saturating_sub(self.semaphore.available_permits());
        let create_in_progress = self.create_in_progress.load(Ordering::Acquire);
        // Permits held during post-acquire CreateSession are not live sessions yet (go-sdk
        // tracks Size separately from CreateInProgress). Warm-up creates do not hold permits.
        // `in_use` may briefly over-count by 1 while a just-acquired permit has not yet
        // incremented `create_in_progress`.
        let creates_with_permit = create_in_progress.min(permits_held);
        let in_use = permits_held.saturating_sub(creates_with_permit);
        SessionPoolStats {
            limit: self.settings.limit,
            warm_up: self.settings.warm_up,
            size: idle + in_use,
            idle,
            in_use,
            create_in_progress,
            sessions_created: self.sessions_created.load(Ordering::Relaxed),
        }
    }

    async fn warm_up_parallel(inner: Arc<Self>, count: usize) -> YdbResult<()> {
        let mut tasks = Vec::with_capacity(count);
        for _ in 0..count {
            let inner = inner.clone();
            tasks.push(tokio::spawn(async move {
                inner.create_explicit_session().await
            }));
        }

        let mut created = Vec::with_capacity(count);
        let mut first_err: Option<YdbError> = None;
        for task in tasks {
            match task.await {
                Ok(Ok(item)) => created.push(item),
                Ok(Err(err)) if first_err.is_none() => first_err = Some(err),
                Ok(Err(_)) => {}
                Err(join_err) if first_err.is_none() => {
                    first_err = Some(YdbError::Transport(format!(
                        "session pool warm-up task failed: {join_err}"
                    )));
                }
                Err(_) => {}
            }
        }

        if created.is_empty() {
            return Err(first_err.unwrap_or_else(|| {
                YdbError::Transport("session pool warm-up produced no sessions".to_string())
            }));
        }

        if let Some(err) = &first_err {
            warn!(
                requested = count,
                warmed = created.len(),
                error = %err,
                "session pool warm-up completed partially; remaining sessions will be created on demand"
            );
        }

        for item in created {
            let overflow = {
                let mut idle = inner.explicit_idle.lock().expect("explicit idle lock");
                if idle.len() < inner.settings.limit {
                    idle.push(item);
                    None
                } else {
                    Some(item)
                }
            };
            if let Some(mut item) = overflow {
                item.record.session.disarm_close_emission();
                inner
                    .metrics
                    .session_pool_session_closed(SessionPoolCloseReason::UsageLimit);
                drop(item);
            }
        }
        inner.update_session_pool_gauges();
        Ok(())
    }

    /// Close every idle session attached to `node_uri` (pessimization / node shutdown).
    ///
    /// Drained sessions are counted with the `shutdown` close reason — the plan's
    /// five-value reason list does not have a separate `node_shutdown` value, so node
    /// draining maps onto it (the injection doc's `node_shutdown` label is an alias).
    fn drain_idle_for_node(&self, node_uri: &Uri) {
        let drained = {
            let mut idle = self.explicit_idle.lock().expect("explicit idle lock");
            let mut drained = Vec::new();
            let mut i = 0;
            while i < idle.len() {
                if idle[i].record.session.node_uri() == node_uri {
                    drained.push(idle.swap_remove(i));
                } else {
                    i += 1;
                }
            }
            drained
        };
        if drained.is_empty() {
            return;
        }
        for mut item in drained {
            // Explicit close site: disarm the abnormal-drop emission so only the
            // shutdown reason is counted for this session.
            item.record.session.disarm_close_emission();
            self.metrics
                .session_pool_session_closed(SessionPoolCloseReason::Shutdown);
        }
        self.update_session_pool_gauges();
    }

    async fn create_explicit_session(&self) -> YdbResult<IdleSession> {
        self.create_in_progress.fetch_add(1, Ordering::SeqCst);
        self.update_session_pool_gauges();
        // Drops at scope end, after the create attempt finished (also on cancellation
        // of this future), decrementing `create_in_progress`.
        let _guard = CreateInProgressGuard { inner: self };
        self.create_explicit_session_inner().await
    }

    #[instrument(name = "ydb.CreateSession", skip_all, fields(db.system.name = "ydb"), err)]
    async fn create_explicit_session_inner(&self) -> YdbResult<IdleSession> {
        #[cfg(test)]
        if self.bench_mode {
            return self.create_explicit_session_bench().await;
        }

        let start = Instant::now();
        let node_uri = self.connection_manager.endpoint(Service::Query)?;
        let mut client = self
            .connection_manager
            .get_auth_service_to_node(RawQueryClient::new, &node_uri)
            .await?;
        let create_timeout = self.settings.session_create_timeout;
        let created = tokio::time::timeout(create_timeout, client.create_session())
            .await
            .map_err(|_| {
                YdbError::Transport(format!(
                    "create query session timed out after {create_timeout:?}"
                ))
            })?;
        let created = created?;
        let created = CreatedSession::new(
            created.session_id,
            node_uri,
            self.cleanup.clone(),
            self.observer.pool_weak(),
        );
        let session = tokio::time::timeout(
            create_timeout,
            created.attach(&mut client, self.observer.clone()),
        )
        .await
        .map_err(|_| {
            YdbError::Transport(format!(
                "attach query session timed out after {create_timeout:?}"
            ))
        })?;
        let session = session?;

        self.metrics.session_pool_session_create(start.elapsed());
        self.now_session_created(session)
    }

    fn now_session_created(&self, session: AttachedSession) -> YdbResult<IdleSession> {
        self.metrics.session_pool_session_created();
        let now = Instant::now();
        self.sessions_created.fetch_add(1, Ordering::Relaxed);
        Ok(IdleSession {
            record: SessionRecord {
                session,
                created: now,
                last_used: now,
                use_count: 0,
            },
        })
    }

    #[cfg(test)]
    async fn create_explicit_session_bench(&self) -> YdbResult<IdleSession> {
        let start = Instant::now();
        let result = self.create_explicit_session_bench_inner().await;
        if result.is_ok() {
            self.metrics.session_pool_session_create(start.elapsed());
        }
        result
    }

    #[cfg(test)]
    async fn create_explicit_session_bench_inner(&self) -> YdbResult<IdleSession> {
        if self.bench_create_failures_remaining.load(Ordering::SeqCst) > 0 {
            self.bench_create_failures_remaining
                .fetch_sub(1, Ordering::SeqCst);
            return Err(YdbError::Transport(
                "bench injected create session failure".to_string(),
            ));
        }
        static BENCH_SESSION_COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = BENCH_SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
        let node_uri = Uri::from_static("http://127.0.0.1/bench");
        self.metrics.session_pool_session_created();
        let now = Instant::now();
        self.sessions_created.fetch_add(1, Ordering::Relaxed);
        Ok(IdleSession {
            record: SessionRecord {
                session: AttachedSession::new_bench_stub(
                    format!("bench-{id}"),
                    node_uri.clone(),
                    self.cleanup.clone(),
                    self.observer.pool_weak(),
                ),
                created: now,
                last_used: now,
                use_count: 0,
            },
        })
    }

    fn pop_explicit_idle(&self) -> Option<IdleSession> {
        let mut idle = self.explicit_idle.lock().expect("explicit idle lock");
        idle.pop()
    }

    /// Close reason for a session record, if the pool policy says it must close.
    fn session_close_reason(&self, item: &SessionRecord) -> Option<SessionPoolCloseReason> {
        session_close_reason(
            &self.settings,
            item.use_count,
            item.created,
            item.last_used,
            item.session.close_cause(),
        )
    }

    fn release_explicit_session(&self, mut item: SessionRecord, permit: OwnedSemaphorePermit) {
        // `last_used` still holds the lease start marker (see `SessionPoolLease::new`).
        self.metrics
            .session_pool_session_use(item.last_used.elapsed());
        item.use_count += 1;
        item.last_used = Instant::now();

        if let Some(reason) = self.session_close_reason(&item) {
            // Disarm first: only this close reason may be counted for the session.
            item.session.disarm_close_emission();
            self.metrics.session_pool_session_closed(reason);
            // Ordering-explicit: free the pool slot before the session cleanup RPC is
            // scheduled, so a new acquirer can proceed while DeleteSession runs.
            drop(permit);
            drop(item);
        } else {
            let overflow = {
                let mut idle = self.explicit_idle.lock().expect("explicit idle lock");
                if idle.len() < self.settings.limit {
                    idle.push(IdleSession { record: item });
                    None
                } else {
                    Some(item)
                }
            };
            // Ordering-explicit as above: slot first, session cleanup after.
            drop(permit);
            if let Some(mut item) = overflow {
                item.session.disarm_close_emission();
                self.metrics
                    .session_pool_session_closed(SessionPoolCloseReason::UsageLimit);
                drop(item);
            }
        }
        self.update_session_pool_gauges();
    }
}

fn session_close_reason(
    settings: &SessionPoolSettings,
    use_count: u64,
    created: Instant,
    last_used: Instant,
    close_cause: SessionCloseCause,
) -> Option<SessionPoolCloseReason> {
    match close_cause {
        SessionCloseCause::Healthy => {}
        SessionCloseCause::Broken => return Some(SessionPoolCloseReason::BadSession),
        SessionCloseCause::KeepaliveFailed => {
            return Some(SessionPoolCloseReason::KeepaliveFailed);
        }
    }
    if settings.item_usage_limit > 0 && use_count >= settings.item_usage_limit {
        return Some(SessionPoolCloseReason::UsageLimit);
    }
    if settings.item_usage_ttl > Duration::ZERO && created.elapsed() >= settings.item_usage_ttl {
        return Some(SessionPoolCloseReason::UsageLimit);
    }
    if settings.idle_ttl > Duration::ZERO && last_used.elapsed() >= settings.idle_ttl {
        return Some(SessionPoolCloseReason::IdleTtl);
    }
    None
}

#[cfg(test)]
impl SessionPool {
    /// Explicit pool backed by in-memory stub sessions (no CreateSession / Attach / Delete RPC).
    pub(crate) fn new_explicit_bench(settings: SessionPoolSettings) -> Self {
        Self::new_explicit_bench_with_metrics(
            settings,
            Arc::new(crate::client_metrics::DefaultMetricsRecorder::new()),
        )
    }

    /// Like [`Self::new_explicit_bench`], but recording into a caller-provided recorder.
    pub(crate) fn new_explicit_bench_with_metrics(
        settings: SessionPoolSettings,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        use crate::GrpcOptions;
        use crate::client_metrics::DefaultMetricsRecorder;
        use crate::discovery::StaticDiscovery;
        use crate::grpc_connection_manager::GrpcConnectionManager;
        use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
        use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};

        let settings = normalize_pool_settings(settings);
        let warm_up = settings.warm_up;
        let limit = settings.limit;
        metrics.session_pool_gauges(SessionPoolGaugeSnapshot::initial(limit));
        let connection_manager = GrpcConnectionManager::new(
            SharedLoadBalancer::new_with_balancer(Box::new(StaticLoadBalancer::new(
                Uri::from_static("http://127.0.0.1/bench"),
            ))),
            "bench".to_string(),
            MultiInterceptor::new(),
            GrpcOptions::default(),
            Arc::new(DefaultMetricsRecorder::new()),
        );

        let discovery: Arc<dyn Discovery> = Arc::new(
            StaticDiscovery::new_from_str("grpc://127.0.0.1:2136")
                .expect("static bench discovery must be valid"),
        );
        let inner = Arc::new_cyclic(|weak| SessionPoolInner {
            settings: settings.clone(),
            // Settings-based (not zero) so tests can exercise acquire-timeout paths.
            acquire_timeout_ms: AtomicU64::new(settings.acquire_timeout.as_millis() as u64),
            connection_manager: connection_manager.clone(),
            semaphore: Arc::new(Semaphore::new(limit)),
            explicit_idle: Mutex::new(Vec::new()),
            cleanup: SessionCleanup::new(connection_manager.clone(), Duration::ZERO),
            observer: SessionPoolObserver {
                discovery: discovery.clone(),
                pool: weak.clone(),
            },
            create_in_progress: AtomicUsize::new(0),
            sessions_created: AtomicU64::new(0),
            pending_acquires: AtomicUsize::new(0),
            metrics,
            bench_mode: true,
            bench_create_failures_remaining: AtomicUsize::new(0),
        });

        if warm_up > 0 {
            let mut idle = inner.explicit_idle.lock().expect("explicit idle lock");
            for i in 0..warm_up {
                let node_uri = Uri::from_static("http://127.0.0.1/bench");
                let now = Instant::now();
                idle.push(IdleSession {
                    record: SessionRecord {
                        session: AttachedSession::new_bench_stub(
                            format!("bench-prefill-{i}"),
                            node_uri.clone(),
                            inner.cleanup.clone(),
                            Arc::downgrade(&inner),
                        ),
                        created: now,
                        last_used: now,
                        use_count: 0,
                    },
                });
            }
        }

        Self { inner }
    }

    /// Bench pool that fails the first `create_failures` explicit session creations (tests only).
    pub(crate) fn new_explicit_bench_with_create_failures(
        settings: SessionPoolSettings,
        create_failures: usize,
    ) -> Self {
        let pool = Self::new_explicit_bench(settings);
        pool.inner
            .bench_create_failures_remaining
            .store(create_failures, Ordering::SeqCst);
        pool
    }

    pub(crate) async fn warm_up_for_tests(&self, count: usize) -> YdbResult<()> {
        SessionPoolInner::warm_up_parallel(self.inner.clone(), count).await
    }

    /// Observer access for metrics tests driving node shutdown drain.
    pub(super) fn observer_for_metrics_tests(&self) -> &SessionPoolObserver {
        &self.inner.observer
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn default_session_pool_timeouts() {
        let settings = SessionPoolSettings::default();
        assert_eq!(settings.limit, DEFAULT_POOL_LIMIT);
        assert_eq!(settings.session_create_timeout, Duration::from_secs(5));
        assert_eq!(settings.session_delete_timeout, Duration::from_millis(500));
    }

    #[test]
    fn normalize_pool_settings_clamps_warm_up_to_limit() {
        let settings = normalize_pool_settings(SessionPoolSettings {
            limit: 0,
            warm_up: 100,
            ..SessionPoolSettings::default()
        });
        assert_eq!(settings.limit, 1);
        assert_eq!(settings.warm_up, 1);
    }

    #[test]
    fn default_session_pool_settings_matches_driver() {
        use crate::session_pool::default_session_pool_settings;
        assert_eq!(
            default_session_pool_settings().limit,
            SessionPoolSettings::default().limit
        );
    }

    #[test]
    fn session_pool_timeout_builders_override_defaults() {
        let settings = SessionPoolSettings::new()
            .with_session_create_timeout(Duration::from_secs(2))
            .with_session_delete_timeout(Duration::from_secs(3));
        assert_eq!(settings.session_create_timeout, Duration::from_secs(2));
        assert_eq!(settings.session_delete_timeout, Duration::from_secs(3));
    }

    #[test]
    fn session_close_reason_respects_usage_limit_and_ttl() {
        let settings = SessionPoolSettings {
            item_usage_limit: 3,
            item_usage_ttl: Duration::from_secs(60),
            idle_ttl: Duration::from_secs(30),
            ..SessionPoolSettings::default()
        };
        let created = Instant::now();
        let last_used = Instant::now();
        let healthy = SessionCloseCause::Healthy;
        assert_eq!(
            session_close_reason(&settings, 2, created, last_used, healthy),
            None
        );
        assert_eq!(
            session_close_reason(&settings, 3, created, last_used, healthy),
            Some(SessionPoolCloseReason::UsageLimit)
        );
        assert_eq!(
            session_close_reason(&settings, 0, created, last_used, SessionCloseCause::Broken),
            Some(SessionPoolCloseReason::BadSession)
        );
        assert_eq!(
            session_close_reason(
                &settings,
                0,
                created,
                last_used,
                SessionCloseCause::KeepaliveFailed
            ),
            Some(SessionPoolCloseReason::KeepaliveFailed)
        );
    }
}
