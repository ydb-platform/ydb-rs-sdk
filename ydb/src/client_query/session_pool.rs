use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use http::Uri;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{trace, warn};

use crate::client::TimeoutSettings;
use crate::discovery::Discovery;
use crate::errors::{YdbError, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;
use crate::grpc_wrapper::raw_query_service::session::{AttachedQuerySession, ImplicitQuerySession};
use crate::grpc_wrapper::raw_services::Service;

const DEFAULT_POOL_LIMIT: usize = 50;
pub(crate) const DEFAULT_SESSION_CREATE_TIMEOUT: Duration = Duration::from_millis(500);
pub(crate) const DEFAULT_SESSION_DELETE_TIMEOUT: Duration = Duration::from_millis(500);

/// CreateSession / AttachSession / DeleteSession RPC limits for non-pooled attached sessions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuerySessionRpcTimeouts {
    pub create: Duration,
    pub delete: Duration,
}

impl Default for QuerySessionRpcTimeouts {
    fn default() -> Self {
        Self {
            create: DEFAULT_SESSION_CREATE_TIMEOUT,
            delete: DEFAULT_SESSION_DELETE_TIMEOUT,
        }
    }
}

impl From<&QuerySessionPoolSettings> for QuerySessionRpcTimeouts {
    fn from(settings: &QuerySessionPoolSettings) -> Self {
        Self {
            create: settings.session_create_timeout,
            delete: settings.session_delete_timeout,
        }
    }
}

/// Ensures `create_in_progress` is decremented when the outer future is dropped
/// (e.g. per-call `with_operation_timeout` cancelling pool acquire + create).
struct CreateInProgressGuard<'a>(&'a AtomicUsize);

impl Drop for CreateInProgressGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn normalize_pool_settings(mut settings: QuerySessionPoolSettings) -> QuerySessionPoolSettings {
    settings.limit = settings.limit.max(1);
    settings.warm_up = settings.warm_up.min(settings.limit);
    settings
}

fn spawn_pool_release<F>(future: F)
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

/// Settings for the Query Service session pool (explicit or implicit items).
#[derive(Clone, Debug)]
pub struct QuerySessionPoolSettings {
    /// Maximum concurrent sessions (pool size limit).
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
    pub session_delete_timeout: Duration,
}

impl Default for QuerySessionPoolSettings {
    fn default() -> Self {
        Self {
            limit: DEFAULT_POOL_LIMIT,
            warm_up: 0,
            item_usage_limit: 0,
            item_usage_ttl: Duration::ZERO,
            idle_ttl: Duration::ZERO,
            session_create_timeout: DEFAULT_SESSION_CREATE_TIMEOUT,
            session_delete_timeout: DEFAULT_SESSION_DELETE_TIMEOUT,
        }
    }
}

/// Snapshot of query session pool counters (aligned with go-sdk `pool.Stats`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuerySessionPoolStats {
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

impl QuerySessionPoolSettings {
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

    /// Maximum time for DeleteSession when the pool closes a session.
    pub fn with_session_delete_timeout(mut self, timeout: Duration) -> Self {
        self.session_delete_timeout = timeout;
        self
    }
}

/// Pooled explicit session lease. Not concurrent-safe: one logical owner at a time.
pub(crate) struct QuerySessionLease {
    item: Option<ExplicitIdleItem>,
    pool: Arc<QuerySessionPoolInner>,
    permit: Option<OwnedSemaphorePermit>,
    returned: bool,
    use_guard: bool,
}

impl QuerySessionLease {
    pub fn session_id(&self) -> &str {
        self.item.as_ref().expect("lease item").session.session_id()
    }

    pub fn node_uri(&self) -> &Uri {
        &self.item.as_ref().expect("lease item").node_uri
    }

    pub fn ensure_alive(&self) -> YdbResult<()> {
        self.item
            .as_ref()
            .expect("lease item")
            .session
            .ensure_alive()
            .map_err(YdbError::from)
    }

    pub fn begin_use(&mut self) {
        if !self.use_guard {
            self.item.as_ref().expect("lease item").session.begin_use();
            self.use_guard = true;
        }
    }

    pub fn end_use(&mut self) {
        if self.use_guard {
            self.item.as_ref().expect("lease item").session.end_use();
            self.use_guard = false;
        }
    }

    pub async fn return_to_pool(mut self) {
        self.end_use();
        self.returned = true;
        let permit = self.permit.take();
        if let Some(item) = self.item.take() {
            self.pool.release_explicit_item(item, permit).await;
        }
    }

    #[cfg(test)]
    pub(crate) fn bench_invalidate_session(&mut self) {
        if let Some(item) = &self.item {
            item.session.bench_invalidate();
        }
    }
}

impl Drop for QuerySessionLease {
    fn drop(&mut self) {
        if self.returned {
            return;
        }
        self.end_use();
        let pool = self.pool.clone();
        let item = self.item.take();
        let permit = self.permit.take();
        if let Some(item) = item {
            spawn_pool_release(async move {
                pool.release_explicit_item(item, permit).await;
            });
        }
    }
}

/// Pooled implicit session lease (empty session id, no AttachSession).
pub(crate) struct ImplicitSessionLease {
    item: Option<ImplicitIdleItem>,
    pool: Arc<QuerySessionPoolInner>,
    permit: Option<OwnedSemaphorePermit>,
}

impl ImplicitSessionLease {
    pub fn session_id(&self) -> &str {
        self.item.as_ref().expect("lease item").session.session_id()
    }

    pub fn begin_use(&mut self) {}

    pub fn end_use(&mut self) {}
}

impl Drop for ImplicitSessionLease {
    fn drop(&mut self) {
        self.end_use();
        let pool = self.pool.clone();
        let item = self.item.take();
        let permit = self.permit.take();
        if let Some(item) = item {
            spawn_pool_release(async move {
                pool.release_implicit_item(item, permit).await;
            });
        }
    }
}

#[derive(Clone)]
pub(crate) struct QuerySessionPool {
    inner: Arc<QuerySessionPoolInner>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuerySessionPoolKind {
    Explicit,
    Implicit,
}

struct ExplicitIdleItem {
    session: AttachedQuerySession,
    node_uri: Uri,
    created: Instant,
    last_used: Instant,
    use_count: u64,
}

struct ImplicitIdleItem {
    session: ImplicitQuerySession,
    created: Instant,
    last_used: Instant,
    use_count: u64,
}

struct QuerySessionPoolInner {
    kind: QuerySessionPoolKind,
    settings: QuerySessionPoolSettings,
    acquire_timeout: Duration,
    connection_manager: GrpcConnectionManager,
    semaphore: Arc<Semaphore>,
    explicit_idle: Mutex<Vec<ExplicitIdleItem>>,
    implicit_idle: Mutex<Vec<ImplicitIdleItem>>,
    on_node_shutdown: Arc<dyn Fn(Uri) + Send + Sync>,
    create_in_progress: AtomicUsize,
    sessions_created: AtomicU64,
    /// Stub create/close paths without RPC (see `session_pool_bench`).
    #[cfg(test)]
    bench_mode: bool,
}

impl QuerySessionPool {
    pub async fn new_explicit(
        connection_manager: GrpcConnectionManager,
        timeouts: TimeoutSettings,
        discovery: Arc<Box<dyn Discovery>>,
        settings: QuerySessionPoolSettings,
    ) -> YdbResult<Self> {
        let settings = normalize_pool_settings(settings);
        let warm_up = settings.warm_up;
        let limit = settings.limit;
        let discovery_for_shutdown = discovery.clone();
        let on_node_shutdown: Arc<dyn Fn(Uri) + Send + Sync> =
            Arc::new(move |uri: Uri| discovery_for_shutdown.pessimization(&uri));

        let inner = Arc::new(QuerySessionPoolInner {
            kind: QuerySessionPoolKind::Explicit,
            settings,
            acquire_timeout: timeouts.operation_timeout,
            connection_manager,
            semaphore: Arc::new(Semaphore::new(limit)),
            explicit_idle: Mutex::new(Vec::new()),
            implicit_idle: Mutex::new(Vec::new()),
            on_node_shutdown,
            create_in_progress: AtomicUsize::new(0),
            sessions_created: AtomicU64::new(0),
            #[cfg(test)]
            bench_mode: false,
        });

        if warm_up > 0 {
            inner.warm_up_explicit(warm_up).await?;
        }

        Ok(Self { inner })
    }

    pub fn new_implicit(
        connection_manager: GrpcConnectionManager,
        timeouts: TimeoutSettings,
        discovery: Arc<Box<dyn Discovery>>,
        settings: QuerySessionPoolSettings,
    ) -> Self {
        let settings = normalize_pool_settings(settings);
        let warm_up = settings.warm_up;
        let limit = settings.limit;
        let discovery_for_shutdown = discovery.clone();
        let on_node_shutdown: Arc<dyn Fn(Uri) + Send + Sync> =
            Arc::new(move |uri: Uri| discovery_for_shutdown.pessimization(&uri));

        let inner = Arc::new(QuerySessionPoolInner {
            kind: QuerySessionPoolKind::Implicit,
            settings,
            acquire_timeout: timeouts.operation_timeout,
            connection_manager,
            semaphore: Arc::new(Semaphore::new(limit)),
            explicit_idle: Mutex::new(Vec::new()),
            implicit_idle: Mutex::new(Vec::new()),
            on_node_shutdown,
            create_in_progress: AtomicUsize::new(0),
            sessions_created: AtomicU64::new(0),
            #[cfg(test)]
            bench_mode: false,
        });

        if warm_up > 0 {
            let mut idle = inner.implicit_idle.lock().expect("implicit idle lock");
            for _ in 0..warm_up {
                let now = Instant::now();
                idle.push(ImplicitIdleItem {
                    session: ImplicitQuerySession::new(),
                    created: now,
                    last_used: now,
                    use_count: 0,
                });
            }
        }

        Self { inner }
    }

    pub fn stats(&self) -> QuerySessionPoolStats {
        self.inner.stats()
    }

    pub(crate) fn session_rpc_timeouts(&self) -> QuerySessionRpcTimeouts {
        QuerySessionRpcTimeouts::from(&self.inner.settings)
    }

    pub async fn acquire_explicit(&self) -> YdbResult<QuerySessionLease> {
        if self.inner.kind != QuerySessionPoolKind::Explicit {
            return Err(YdbError::Custom(
                "explicit session pool is not configured".to_string(),
            ));
        }

        let permit = self.inner.acquire_permit().await?;

        let mut stale_items = Vec::new();
        while let Some(item) = self.inner.pop_explicit_idle() {
            if self.inner.should_close_explicit(&item) {
                stale_items.push(item);
                continue;
            }
            for stale in stale_items {
                let inner = self.inner.clone();
                spawn_pool_release(async move {
                    inner.close_explicit_item(stale).await;
                });
            }
            trace!(
                session_id = item.session.session_id(),
                "got query session from pool"
            );
            return Ok(QuerySessionLease {
                item: Some(item),
                pool: self.inner.clone(),
                permit: Some(permit),
                returned: false,
                use_guard: false,
            });
        }
        for stale in stale_items {
            let inner = self.inner.clone();
            spawn_pool_release(async move {
                inner.close_explicit_item(stale).await;
            });
        }

        let item = self.inner.create_explicit_session().await?;
        trace!(
            session_id = item.session.session_id(),
            "created query session for pool"
        );
        Ok(QuerySessionLease {
            item: Some(item),
            pool: self.inner.clone(),
            permit: Some(permit),
            returned: false,
            use_guard: false,
        })
    }

    pub async fn acquire_implicit(&self) -> YdbResult<ImplicitSessionLease> {
        if self.inner.kind != QuerySessionPoolKind::Implicit {
            return Err(YdbError::Custom(
                "implicit session pool is not configured".to_string(),
            ));
        }

        let permit = self.inner.acquire_permit().await?;

        while let Some(item) = self.inner.pop_implicit_idle() {
            if self.inner.should_close_implicit(&item) {
                item.session.close();
                continue;
            }
            return Ok(ImplicitSessionLease {
                item: Some(item),
                pool: self.inner.clone(),
                permit: Some(permit),
            });
        }

        Ok(ImplicitSessionLease {
            item: Some(ImplicitIdleItem {
                session: ImplicitQuerySession::new(),
                created: Instant::now(),
                last_used: Instant::now(),
                use_count: 0,
            }),
            pool: self.inner.clone(),
            permit: Some(permit),
        })
    }
}

impl QuerySessionPoolInner {
    async fn acquire_permit(&self) -> YdbResult<OwnedSemaphorePermit> {
        let acquire = self.semaphore.clone().acquire_owned();
        let permit = if self.acquire_timeout.is_zero() {
            acquire.await
        } else {
            tokio::time::timeout(self.acquire_timeout, acquire)
                .await
                .map_err(|_| {
                    YdbError::Transport(format!(
                        "acquire session from pool timed out after {:?}",
                        self.acquire_timeout
                    ))
                })?
        };
        permit.map_err(|_| YdbError::Transport("query session pool closed".to_string()))
    }

    fn stats(&self) -> QuerySessionPoolStats {
        let idle = match self.kind {
            QuerySessionPoolKind::Explicit => {
                self.explicit_idle.lock().expect("explicit idle lock").len()
            }
            QuerySessionPoolKind::Implicit => {
                self.implicit_idle.lock().expect("implicit idle lock").len()
            }
        };
        let permits_held = self
            .settings
            .limit
            .saturating_sub(self.semaphore.available_permits());
        let create_in_progress = self.create_in_progress.load(Ordering::Acquire);
        // Permits held during post-acquire CreateSession are not live sessions yet (go-sdk
        // tracks Size separately from CreateInProgress). Warm-up creates do not hold permits.
        let creates_with_permit = create_in_progress.min(permits_held);
        let in_use = permits_held.saturating_sub(creates_with_permit);
        QuerySessionPoolStats {
            limit: self.settings.limit,
            warm_up: self.settings.warm_up,
            size: idle + in_use,
            idle,
            in_use,
            create_in_progress,
            sessions_created: self.sessions_created.load(Ordering::Relaxed),
        }
    }

    async fn warm_up_explicit(&self, count: usize) -> YdbResult<()> {
        for _ in 0..count {
            let item = match self.create_explicit_session().await {
                Ok(item) => item,
                Err(err) => {
                    self.drain_and_close_explicit_idle().await;
                    return Err(err);
                }
            };
            let overflow = {
                let mut idle = self.explicit_idle.lock().expect("explicit idle lock");
                if idle.len() < self.settings.limit {
                    idle.push(item);
                    None
                } else {
                    Some(item)
                }
            };
            if let Some(item) = overflow {
                self.close_explicit_item(item).await;
            }
        }
        Ok(())
    }

    async fn drain_and_close_explicit_idle(&self) {
        let items: Vec<ExplicitIdleItem> = {
            let mut idle = self.explicit_idle.lock().expect("explicit idle lock");
            std::mem::take(&mut *idle)
        };
        for item in items {
            self.close_explicit_item(item).await;
        }
    }

    async fn create_explicit_session(&self) -> YdbResult<ExplicitIdleItem> {
        self.create_in_progress.fetch_add(1, Ordering::SeqCst);
        let _guard = CreateInProgressGuard(&self.create_in_progress);
        self.create_explicit_session_inner().await
    }

    async fn create_explicit_session_inner(&self) -> YdbResult<ExplicitIdleItem> {
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
        let delete_timeout = self.settings.session_delete_timeout;
        let on_node_shutdown = self.on_node_shutdown.clone();

        let created = tokio::time::timeout(create_timeout, client.create_session())
            .await
            .map_err(|_| {
                YdbError::Transport(format!(
                    "create query session timed out after {create_timeout:?}"
                ))
            })?
            .map_err(YdbError::from)?;
        let session_id = created.session_id;

        let session = match tokio::time::timeout(
            create_timeout,
            AttachedQuerySession::open(
                &mut client,
                node_uri.clone(),
                session_id.clone(),
                on_node_shutdown,
                delete_timeout,
            ),
        )
        .await
        {
            Ok(Ok(session)) => session,
            Ok(Err(err)) => {
                let _ =
                    tokio::time::timeout(delete_timeout, client.delete_session(&session_id)).await;
                return Err(YdbError::from(err));
            }
            Err(_) => {
                let _ =
                    tokio::time::timeout(delete_timeout, client.delete_session(&session_id)).await;
                return Err(YdbError::Transport(format!(
                    "attach query session timed out after {create_timeout:?}"
                )));
            }
        };

        let now = Instant::now();
        self.sessions_created.fetch_add(1, Ordering::Relaxed);
        Ok(ExplicitIdleItem {
            session,
            node_uri,
            created: now,
            last_used: now,
            use_count: 0,
        })
    }

    #[cfg(test)]
    async fn create_explicit_session_bench(&self) -> YdbResult<ExplicitIdleItem> {
        static BENCH_SESSION_COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = BENCH_SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
        let node_uri = Uri::from_static("http://127.0.0.1/bench");
        let now = Instant::now();
        self.sessions_created.fetch_add(1, Ordering::Relaxed);
        Ok(ExplicitIdleItem {
            session: AttachedQuerySession::new_bench_stub(format!("bench-{id}"), node_uri.clone()),
            node_uri,
            created: now,
            last_used: now,
            use_count: 0,
        })
    }

    fn pop_explicit_idle(&self) -> Option<ExplicitIdleItem> {
        let mut idle = self.explicit_idle.lock().expect("explicit idle lock");
        idle.pop()
    }

    fn pop_implicit_idle(&self) -> Option<ImplicitIdleItem> {
        let mut idle = self.implicit_idle.lock().expect("implicit idle lock");
        idle.pop()
    }

    fn should_close_explicit(&self, item: &ExplicitIdleItem) -> bool {
        session_should_close(
            &self.settings,
            item.use_count,
            item.created,
            item.last_used,
            item.session.is_alive(),
        )
    }

    fn should_close_implicit(&self, item: &ImplicitIdleItem) -> bool {
        session_should_close(
            &self.settings,
            item.use_count,
            item.created,
            item.last_used,
            item.session.is_alive(),
        )
    }

    async fn close_explicit_item(&self, item: ExplicitIdleItem) {
        #[cfg(test)]
        if self.bench_mode {
            item.session.bench_close();
            return;
        }

        match self
            .connection_manager
            .get_auth_service_to_node(RawQueryClient::new, &item.node_uri)
            .await
        {
            Ok(mut client) => {
                item.session.close(&mut client).await;
            }
            Err(err) => {
                warn!(
                    session_id = item.session.session_id(),
                    error = %err,
                    "failed to connect for DeleteSession; aborting attach listener"
                );
                item.session.abort_without_delete().await;
            }
        }
    }

    async fn release_explicit_item(
        &self,
        mut item: ExplicitIdleItem,
        permit: Option<OwnedSemaphorePermit>,
    ) {
        item.use_count += 1;
        item.last_used = Instant::now();

        if self.should_close_explicit(&item) {
            // Drop permit first so other acquirers are not blocked during DeleteSession.
            drop(permit);
            self.close_explicit_item(item).await;
        } else {
            let overflow = {
                let mut idle = self.explicit_idle.lock().expect("explicit idle lock");
                if idle.len() < self.settings.limit {
                    idle.push(item);
                    None
                } else {
                    Some(item)
                }
            };
            // Push to idle before dropping permit: a waiter may acquire the same session
            // as soon as the semaphore slot is freed.
            drop(permit);
            if let Some(item) = overflow {
                self.close_explicit_item(item).await;
            }
        }
    }

    async fn release_implicit_item(
        &self,
        mut item: ImplicitIdleItem,
        permit: Option<OwnedSemaphorePermit>,
    ) {
        item.use_count += 1;
        item.last_used = Instant::now();

        if self.should_close_implicit(&item) {
            item.session.close();
            drop(permit);
        } else {
            let overflow = {
                let mut idle = self.implicit_idle.lock().expect("implicit idle lock");
                if idle.len() < self.settings.limit {
                    idle.push(item);
                    None
                } else {
                    Some(item)
                }
            };
            drop(permit);
            if let Some(item) = overflow {
                item.session.close();
            }
        }
    }
}

impl Drop for QuerySessionPoolInner {
    fn drop(&mut self) {
        let explicit: Vec<ExplicitIdleItem> = self
            .explicit_idle
            .lock()
            .expect("explicit idle lock")
            .drain(..)
            .collect();
        let implicit: Vec<ImplicitIdleItem> = self
            .implicit_idle
            .lock()
            .expect("implicit idle lock")
            .drain(..)
            .collect();
        if explicit.is_empty() && implicit.is_empty() {
            return;
        }
        #[cfg(test)]
        if self.bench_mode {
            for item in explicit {
                item.session.bench_close();
            }
            for item in implicit {
                item.session.close();
            }
            return;
        }
        let connection_manager = self.connection_manager.clone();
        spawn_pool_release(async move {
            for item in explicit {
                match connection_manager
                    .get_auth_service_to_node(RawQueryClient::new, &item.node_uri)
                    .await
                {
                    Ok(mut client) => {
                        item.session.close(&mut client).await;
                    }
                    Err(err) => {
                        warn!(
                            session_id = item.session.session_id(),
                            error = %err,
                            "failed to connect for DeleteSession during pool shutdown; aborting attach listener"
                        );
                        item.session.abort_without_delete().await;
                    }
                }
            }
            for item in implicit {
                item.session.close();
            }
        });
    }
}

fn session_should_close(
    settings: &QuerySessionPoolSettings,
    use_count: u64,
    created: Instant,
    last_used: Instant,
    is_alive: bool,
) -> bool {
    if !is_alive {
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
impl QuerySessionPool {
    /// Explicit pool backed by in-memory stub sessions (no CreateSession / Attach / Delete RPC).
    pub(crate) fn new_explicit_bench(settings: QuerySessionPoolSettings) -> Self {
        use crate::grpc_connection_manager::GrpcConnectionManager;
        use crate::grpc_wrapper::grpc_limits::DEFAULT_GRPC_MESSAGE_SIZE_LIMIT_BYTES;
        use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
        use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};

        let settings = normalize_pool_settings(settings);
        let warm_up = settings.warm_up;
        let limit = settings.limit;
        let on_node_shutdown: Arc<dyn Fn(Uri) + Send + Sync> = Arc::new(|_: Uri| {});

        let connection_manager = GrpcConnectionManager::new(
            SharedLoadBalancer::new_with_balancer(Box::new(StaticLoadBalancer::new(
                Uri::from_static("http://127.0.0.1/bench"),
            ))),
            "bench".to_string(),
            MultiInterceptor::new(),
            None,
            DEFAULT_GRPC_MESSAGE_SIZE_LIMIT_BYTES,
        );

        let inner = Arc::new(QuerySessionPoolInner {
            kind: QuerySessionPoolKind::Explicit,
            settings,
            acquire_timeout: Duration::ZERO,
            connection_manager,
            semaphore: Arc::new(Semaphore::new(limit)),
            explicit_idle: Mutex::new(Vec::new()),
            implicit_idle: Mutex::new(Vec::new()),
            on_node_shutdown,
            create_in_progress: AtomicUsize::new(0),
            sessions_created: AtomicU64::new(0),
            bench_mode: true,
        });

        if warm_up > 0 {
            let mut idle = inner.explicit_idle.lock().expect("explicit idle lock");
            for i in 0..warm_up {
                let node_uri = Uri::from_static("http://127.0.0.1/bench");
                let now = Instant::now();
                idle.push(ExplicitIdleItem {
                    session: AttachedQuerySession::new_bench_stub(
                        format!("bench-prefill-{i}"),
                        node_uri.clone(),
                    ),
                    node_uri,
                    created: now,
                    last_used: now,
                    use_count: 0,
                });
            }
        }

        Self { inner }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn default_session_pool_timeouts_are_500ms() {
        let settings = QuerySessionPoolSettings::default();
        assert_eq!(settings.session_create_timeout, Duration::from_millis(500));
        assert_eq!(settings.session_delete_timeout, Duration::from_millis(500));
    }

    #[test]
    fn session_pool_timeout_builders_override_defaults() {
        let settings = QuerySessionPoolSettings::new()
            .with_session_create_timeout(Duration::from_secs(2))
            .with_session_delete_timeout(Duration::from_secs(3));
        assert_eq!(settings.session_create_timeout, Duration::from_secs(2));
        assert_eq!(settings.session_delete_timeout, Duration::from_secs(3));
        let rpc = QuerySessionRpcTimeouts::from(&settings);
        assert_eq!(rpc.create, Duration::from_secs(2));
        assert_eq!(rpc.delete, Duration::from_secs(3));
    }

    #[test]
    fn session_should_close_respects_usage_limit_and_ttl() {
        let settings = QuerySessionPoolSettings {
            item_usage_limit: 3,
            item_usage_ttl: Duration::from_secs(60),
            idle_ttl: Duration::from_secs(30),
            ..QuerySessionPoolSettings::default()
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
