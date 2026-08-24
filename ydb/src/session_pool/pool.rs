use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use http::Uri;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{instrument, trace, warn};

use crate::discovery::Discovery;
use crate::errors::{YdbError, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;
use crate::grpc_wrapper::raw_services::Service;

use super::session::{AttachedSession, CreatedSession, SessionCleanup};

/// Default pool size for [`SessionPoolSettings::default()`] and the driver built-in pool.
///
/// Matches ydb-go-sdk `pool.DefaultLimit` (50). The legacy table-only session pool
/// defaulted to 1000; callers migrating from that capacity should set
/// `SessionPoolSettings::new().with_limit(1000)` explicitly.
pub(crate) const DEFAULT_POOL_LIMIT: usize = 50;
pub(crate) const DEFAULT_SESSION_CREATE_TIMEOUT: Duration = Duration::from_millis(500);
pub(crate) const DEFAULT_SESSION_DELETE_TIMEOUT: Duration = Duration::from_millis(500);
/// Default max wait when acquiring a session from the pool.
pub(crate) const DEFAULT_POOL_ACQUIRE_TIMEOUT: Duration = Duration::ZERO;

/// Ensures `create_in_progress` is decremented when the outer future is dropped
/// (e.g. per-call `with_operation_timeout` cancelling pool acquire + create).
struct CreateInProgressGuard<'a>(&'a AtomicUsize);

impl Drop for CreateInProgressGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
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
/// returning it schedules session cleanup.
pub(crate) struct SessionPoolLease {
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
        Self {
            permit,
            record: session.record,
            pool,
        }
    }

    pub fn session_id(&self) -> &str {
        self.record.session.session_id()
    }

    pub fn node_uri(&self) -> &Uri {
        self.record.session.node_uri()
    }

    pub fn ensure_healthy(&self) -> YdbResult<()> {
        self.record.session.ensure_healthy()
    }

    pub(crate) fn cleanup_timeout(&self) -> Duration {
        self.pool.settings.session_delete_timeout
    }

    /// Consume this lease and offer its session back to the pool. Pool policy may still discard
    /// an unhealthy, expired, or excess session.
    #[instrument(name = "ydb.SessionPool.ReturnSession", skip_all, fields(db.system.name = "ydb"))]
    pub fn return_to_pool(self) {
        let Self {
            permit,
            record,
            pool,
        } = self;
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
        self.record.session.invalidate();
    }
}

#[derive(Clone)]
pub(crate) struct SessionPool {
    inner: Arc<SessionPoolInner>,
}

/// Session data shared by the pool ownership states below.
struct SessionRecord {
    session: AttachedSession,
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
    pub(super) fn node_shutdown(&self, node_uri: &Uri) {
        self.discovery.pessimization(node_uri);
        let Some(pool) = self.pool.upgrade() else {
            return;
        };
        let _ = pool.drain_idle_for_node(node_uri);
    }
}

struct SessionPoolInner {
    settings: SessionPoolSettings,
    acquire_timeout_ms: AtomicU64,
    connection_manager: GrpcConnectionManager,
    semaphore: Arc<Semaphore>,
    explicit_idle: Mutex<Vec<IdleSession>>,
    cleanup: SessionCleanup,
    observer: SessionPoolObserver,
    create_in_progress: AtomicUsize,
    sessions_created: AtomicU64,
    /// Stub create/close paths without RPC (see `session_pool_bench` and regression tests).
    #[cfg(test)]
    bench_mode: bool,
    #[cfg(test)]
    bench_create_failures_remaining: AtomicUsize,
}

impl SessionPool {
    pub fn new_explicit_sync(
        connection_manager: GrpcConnectionManager,
        discovery: Arc<dyn Discovery>,
        settings: SessionPoolSettings,
    ) -> Self {
        let settings = normalize_pool_settings(settings);
        let limit = settings.limit;
        let inner = Arc::new_cyclic(
            |weak: &std::sync::Weak<SessionPoolInner>| SessionPoolInner {
                settings: settings.clone(),
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
    ) -> YdbResult<Self> {
        let settings = normalize_pool_settings(settings);
        let warm_up = settings.warm_up;
        let pool = Self::new_explicit_sync(connection_manager, discovery, settings);

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
        let permit = self.inner.acquire_permit().await?;

        while let Some(item) = self.inner.pop_explicit_idle() {
            if self.inner.should_close_explicit(&item.record) {
                drop(item);
                continue;
            }
            trace!(
                session_id = item.record.session.session_id(),
                "got query session from pool"
            );
            return Ok(SessionPoolLease::new(item, permit, self.inner.clone()));
        }

        let item = self.inner.create_explicit_session().await?;
        trace!(
            session_id = item.record.session.session_id(),
            "created query session for pool"
        );
        Ok(SessionPoolLease::new(item, permit, self.inner.clone()))
    }
}

impl SessionPoolInner {
    fn acquire_timeout(&self) -> Duration {
        Duration::from_millis(self.acquire_timeout_ms.load(Ordering::Relaxed))
    }

    async fn acquire_permit(&self) -> YdbResult<OwnedSemaphorePermit> {
        let acquire_timeout = self.acquire_timeout();
        let acquire = self.semaphore.clone().acquire_owned();
        let permit = if acquire_timeout.is_zero() {
            acquire.await
        } else {
            tokio::time::timeout(acquire_timeout, acquire)
                .await
                .map_err(|_| {
                    YdbError::Transport(format!(
                        "acquire session from pool timed out after {acquire_timeout:?}"
                    ))
                })?
        };
        permit.map_err(|_| YdbError::Transport("session pool closed".to_string()))
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
            if let Some(item) = overflow {
                drop(item);
            }
        }
        Ok(())
    }

    fn drain_idle_for_node(&self, node_uri: &Uri) -> Vec<IdleSession> {
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
    }

    async fn create_explicit_session(&self) -> YdbResult<IdleSession> {
        self.create_in_progress.fetch_add(1, Ordering::SeqCst);
        let _guard = CreateInProgressGuard(&self.create_in_progress);
        self.create_explicit_session_inner().await
    }

    #[instrument(name = "ydb.CreateSession", skip_all, fields(db.system.name = "ydb"), err)]
    async fn create_explicit_session_inner(&self) -> YdbResult<IdleSession> {
        #[cfg(test)]
        if self.bench_mode {
            return self.create_explicit_session_bench().await;
        }

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
        let created = CreatedSession::new(created.session_id, node_uri, self.cleanup.clone());
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
        let now = Instant::now();
        self.sessions_created.fetch_add(1, Ordering::Relaxed);
        Ok(IdleSession {
            record: SessionRecord {
                session: AttachedSession::new_bench_stub(
                    format!("bench-{id}"),
                    node_uri.clone(),
                    self.cleanup.clone(),
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

    fn should_close_explicit(&self, item: &SessionRecord) -> bool {
        session_should_close(
            &self.settings,
            item.use_count,
            item.created,
            item.last_used,
            item.session.is_healthy(),
        )
    }

    fn release_explicit_session(&self, mut item: SessionRecord, permit: OwnedSemaphorePermit) {
        item.use_count += 1;
        item.last_used = Instant::now();

        if self.should_close_explicit(&item) {
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
            drop(permit);
            if let Some(item) = overflow {
                drop(item);
            }
        }
    }
}

fn session_should_close(
    settings: &SessionPoolSettings,
    use_count: u64,
    created: Instant,
    last_used: Instant,
    is_healthy: bool,
) -> bool {
    if !is_healthy {
        return true;
    }
    if settings.item_usage_limit > 0 && use_count >= settings.item_usage_limit {
        return true;
    }
    if settings.item_usage_ttl > Duration::ZERO && created.elapsed() >= settings.item_usage_ttl {
        return true;
    }
    if settings.idle_ttl > Duration::ZERO && last_used.elapsed() >= settings.idle_ttl {
        return true;
    }
    false
}

#[cfg(test)]
impl SessionPool {
    /// Explicit pool backed by in-memory stub sessions (no CreateSession / Attach / Delete RPC).
    pub(crate) fn new_explicit_bench(settings: SessionPoolSettings) -> Self {
        use crate::GrpcOptions;
        use crate::discovery::StaticDiscovery;
        use crate::grpc_connection_manager::GrpcConnectionManager;
        use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
        use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};

        let settings = normalize_pool_settings(settings);
        let warm_up = settings.warm_up;
        let limit = settings.limit;
        let connection_manager = GrpcConnectionManager::new(
            SharedLoadBalancer::new_with_balancer(Box::new(StaticLoadBalancer::new(
                Uri::from_static("http://127.0.0.1/bench"),
            ))),
            "bench".to_string(),
            MultiInterceptor::new(),
            GrpcOptions::default(),
        );

        let discovery: Arc<dyn Discovery> = Arc::new(
            StaticDiscovery::new_from_str("grpc://127.0.0.1:2136")
                .expect("static bench discovery must be valid"),
        );
        let inner = Arc::new_cyclic(|weak| SessionPoolInner {
            settings: settings.clone(),
            acquire_timeout_ms: AtomicU64::new(0),
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
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn default_session_pool_timeouts_are_500ms() {
        let settings = SessionPoolSettings::default();
        assert_eq!(settings.limit, DEFAULT_POOL_LIMIT);
        assert_eq!(settings.session_create_timeout, Duration::from_millis(500));
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
    fn session_should_close_respects_usage_limit_and_ttl() {
        let settings = SessionPoolSettings {
            item_usage_limit: 3,
            item_usage_ttl: Duration::from_secs(60),
            idle_ttl: Duration::from_secs(30),
            ..SessionPoolSettings::default()
        };
        let created = Instant::now();
        let last_used = Instant::now();
        assert!(!session_should_close(
            &settings, 2, created, last_used, true,
        ));
        assert!(session_should_close(&settings, 3, created, last_used, true));
        assert!(session_should_close(
            &settings, 0, created, last_used, false,
        ));
    }
}
