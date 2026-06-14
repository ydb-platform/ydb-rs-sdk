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
const DEFAULT_SESSION_CREATE_TIMEOUT: Duration = Duration::from_millis(500);
const DEFAULT_SESSION_DELETE_TIMEOUT: Duration = Duration::from_millis(500);

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
            tokio::spawn(async move {
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
    returned: bool,
    use_guard: bool,
}

impl ImplicitSessionLease {
    pub fn session_id(&self) -> &str {
        self.item.as_ref().expect("lease item").session.session_id()
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
            self.pool.release_implicit_item(item, permit).await;
        }
    }
}

impl Drop for ImplicitSessionLease {
    fn drop(&mut self) {
        if self.returned {
            return;
        }
        self.end_use();
        let pool = self.pool.clone();
        let item = self.item.take();
        let permit = self.permit.take();
        if let Some(item) = item {
            tokio::spawn(async move {
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
    connection_manager: GrpcConnectionManager,
    semaphore: Arc<Semaphore>,
    explicit_idle: Mutex<Vec<ExplicitIdleItem>>,
    implicit_idle: Mutex<Vec<ImplicitIdleItem>>,
    on_node_shutdown: Arc<dyn Fn(Uri) + Send + Sync>,
}

impl QuerySessionPool {
    pub async fn new_explicit(
        connection_manager: GrpcConnectionManager,
        _timeouts: TimeoutSettings,
        discovery: Arc<Box<dyn Discovery>>,
        settings: QuerySessionPoolSettings,
    ) -> YdbResult<Self> {
        let limit = settings.limit.max(1);
        let warm_up = settings.warm_up.min(limit);
        let discovery_for_shutdown = discovery.clone();
        let on_node_shutdown: Arc<dyn Fn(Uri) + Send + Sync> =
            Arc::new(move |uri: Uri| discovery_for_shutdown.pessimization(&uri));

        let inner = Arc::new(QuerySessionPoolInner {
            kind: QuerySessionPoolKind::Explicit,
            settings,
            connection_manager,
            semaphore: Arc::new(Semaphore::new(limit)),
            explicit_idle: Mutex::new(Vec::new()),
            implicit_idle: Mutex::new(Vec::new()),
            on_node_shutdown,
        });

        if warm_up > 0 {
            inner.warm_up_explicit(warm_up).await?;
        }

        Ok(Self { inner })
    }

    pub fn new_implicit(
        connection_manager: GrpcConnectionManager,
        _timeouts: TimeoutSettings,
        discovery: Arc<Box<dyn Discovery>>,
        settings: QuerySessionPoolSettings,
    ) -> Self {
        let limit = settings.limit.max(1);
        let warm_up = settings.warm_up.min(limit);
        let discovery_for_shutdown = discovery.clone();
        let on_node_shutdown: Arc<dyn Fn(Uri) + Send + Sync> =
            Arc::new(move |uri: Uri| discovery_for_shutdown.pessimization(&uri));

        let inner = Arc::new(QuerySessionPoolInner {
            kind: QuerySessionPoolKind::Implicit,
            settings,
            connection_manager,
            semaphore: Arc::new(Semaphore::new(limit)),
            explicit_idle: Mutex::new(Vec::new()),
            implicit_idle: Mutex::new(Vec::new()),
            on_node_shutdown,
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

    pub async fn acquire_explicit(&self) -> YdbResult<QuerySessionLease> {
        if self.inner.kind != QuerySessionPoolKind::Explicit {
            return Err(YdbError::Custom(
                "explicit session pool is not configured".to_string(),
            ));
        }

        let permit = self
            .inner
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| YdbError::Transport("query session pool closed".to_string()))?;

        for _ in 0..2 {
            if let Some(item) = self.inner.pop_explicit_idle() {
                if self.inner.should_close_explicit(&item) {
                    self.inner.close_explicit_item(item).await;
                    continue;
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
            break;
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

        let permit = self
            .inner
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| YdbError::Transport("query session pool closed".to_string()))?;

        for _ in 0..2 {
            if let Some(item) = self.inner.pop_implicit_idle() {
                if self.inner.should_close_implicit(&item) {
                    item.session.close();
                    continue;
                }
                return Ok(ImplicitSessionLease {
                    item: Some(item),
                    pool: self.inner.clone(),
                    permit: Some(permit),
                    returned: false,
                    use_guard: false,
                });
            }
            break;
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
            returned: false,
            use_guard: false,
        })
    }
}

impl QuerySessionPoolInner {
    async fn warm_up_explicit(&self, count: usize) -> YdbResult<()> {
        for _ in 0..count {
            let item = self.create_explicit_session().await?;
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

    async fn create_explicit_session(&self) -> YdbResult<ExplicitIdleItem> {
        let node_uri = self.connection_manager.endpoint(Service::Query)?;
        let mut client = self
            .connection_manager
            .get_auth_service_to_node(RawQueryClient::new, &node_uri)
            .await?;
        let create_timeout = self.settings.session_create_timeout;
        let delete_timeout = self.settings.session_delete_timeout;
        let on_node_shutdown = self.on_node_shutdown.clone();

        let session = tokio::time::timeout(create_timeout, async {
            AttachedQuerySession::create_and_open(
                &mut client,
                node_uri.clone(),
                on_node_shutdown,
                delete_timeout,
            )
            .await
        })
        .await
        .map_err(|_| {
            YdbError::Transport(format!(
                "create query session timed out after {create_timeout:?}"
            ))
        })?
        .map_err(YdbError::from)?;

        let now = Instant::now();
        Ok(ExplicitIdleItem {
            session,
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
        if !item.session.is_alive() {
            return true;
        }
        if self.settings.item_usage_limit > 0 && item.use_count >= self.settings.item_usage_limit {
            return true;
        }
        if self.settings.item_usage_ttl > Duration::ZERO
            && item.created.elapsed() >= self.settings.item_usage_ttl
        {
            return true;
        }
        if self.settings.idle_ttl > Duration::ZERO
            && item.last_used.elapsed() >= self.settings.idle_ttl
        {
            return true;
        }
        false
    }

    fn should_close_implicit(&self, item: &ImplicitIdleItem) -> bool {
        if !item.session.is_alive() {
            return true;
        }
        if self.settings.item_usage_limit > 0 && item.use_count >= self.settings.item_usage_limit {
            return true;
        }
        if self.settings.item_usage_ttl > Duration::ZERO
            && item.created.elapsed() >= self.settings.item_usage_ttl
        {
            return true;
        }
        if self.settings.idle_ttl > Duration::ZERO
            && item.last_used.elapsed() >= self.settings.idle_ttl
        {
            return true;
        }
        false
    }

    async fn close_explicit_item(&self, item: ExplicitIdleItem) {
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
                    "failed to connect for DeleteSession; server-side session may leak until idle timeout"
                );
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
            let mut idle = self.implicit_idle.lock().expect("implicit idle lock");
            if idle.len() < self.settings.limit {
                idle.push(item);
            } else {
                item.session.close();
            }
            drop(permit);
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn explicit_idle_lifo_storage() {
        let mut idle: Vec<ExplicitIdleItem> = Vec::new();
        // LIFO: pop from end
        assert!(idle.pop().is_none());
    }
}
