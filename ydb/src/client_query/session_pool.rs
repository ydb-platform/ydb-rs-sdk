use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::client::TimeoutSettings;
use crate::discovery::Discovery;
use crate::errors::{YdbError, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_query_service::session::ImplicitQuerySession;

const DEFAULT_POOL_LIMIT: usize = 50;

/// Settings for the implicit Query Service session pool.
#[derive(Clone, Debug)]
pub struct QuerySessionPoolSettings {
    /// Maximum concurrent implicit sessions (pool size limit).
    pub limit: usize,
    /// Minimum sessions to pre-create at pool initialization (warm-up).
    pub warm_up: usize,
    /// Close a session after this many uses (0 = unlimited).
    pub item_usage_limit: u64,
    /// Close a session after this wall-clock lifetime (0 = unlimited).
    pub item_usage_ttl: Duration,
    /// Close idle sessions after this duration (0 = unlimited).
    pub idle_ttl: Duration,
}

impl Default for QuerySessionPoolSettings {
    fn default() -> Self {
        Self {
            limit: DEFAULT_POOL_LIMIT,
            warm_up: 0,
            item_usage_limit: 0,
            item_usage_ttl: Duration::ZERO,
            idle_ttl: Duration::ZERO,
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

struct ImplicitIdleItem {
    session: ImplicitQuerySession,
    created: Instant,
    last_used: Instant,
    use_count: u64,
}

struct QuerySessionPoolInner {
    settings: QuerySessionPoolSettings,
    semaphore: Arc<Semaphore>,
    implicit_idle: Mutex<Vec<ImplicitIdleItem>>,
}

impl QuerySessionPool {
    pub fn new_implicit(
        _connection_manager: GrpcConnectionManager,
        _timeouts: TimeoutSettings,
        _discovery: Arc<Box<dyn Discovery>>,
        settings: QuerySessionPoolSettings,
    ) -> Self {
        let limit = settings.limit.max(1);
        let warm_up = settings.warm_up.min(limit);

        let inner = Arc::new(QuerySessionPoolInner {
            settings,
            semaphore: Arc::new(Semaphore::new(limit)),
            implicit_idle: Mutex::new(Vec::new()),
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

    pub async fn acquire_implicit(&self) -> YdbResult<ImplicitSessionLease> {
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
    fn pop_implicit_idle(&self) -> Option<ImplicitIdleItem> {
        let mut idle = self.implicit_idle.lock().expect("implicit idle lock");
        idle.pop()
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
