use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rand::Rng;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{sleep, timeout};

use crate::client::TimeoutSettings;
use crate::errors::{NeedRetry, YdbError, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;
use crate::grpc_wrapper::raw_query_service::execute_query::RawExecuteQueryRequest;
use crate::grpc_wrapper::raw_query_service::stream::ExecuteQueryStream;
use crate::types::Value;

const DEFAULT_RETRY_BUDGET: Duration = Duration::from_secs(5);
const INITIAL_RETRY_BACKOFF_MILLISECONDS: u64 = 1;
const MAX_RETRY_BACKOFF_MILLISECONDS: u64 = 1_000;
const DEFAULT_POOL_LIMIT: usize = 50;

#[derive(Clone, Debug, Default)]
pub(crate) struct CallOptions {
    pub timeout: Option<Duration>,
    pub idempotent: Option<bool>,
    pub collect_stats: bool,
}

#[derive(Clone)]
pub(crate) struct ClientExecContext {
    pub connection_manager: GrpcConnectionManager,
    pub timeouts: TimeoutSettings,
    pub idempotent_operation: bool,
    pub retry_budget: Duration,
    pub(crate) session_pool: QuerySessionPool,
}

// --- implicit session pool (internal, always enabled) ---

struct ImplicitQuerySession {
    in_use: AtomicUsize,
    alive: AtomicBool,
}

impl ImplicitQuerySession {
    fn new() -> Self {
        Self {
            in_use: AtomicUsize::new(0),
            alive: AtomicBool::new(true),
        }
    }

    fn session_id(&self) -> &str {
        ""
    }

    fn begin_use(&self) {
        self.in_use.fetch_add(1, Ordering::SeqCst);
    }

    fn end_use(&self) {
        self.in_use.fetch_sub(1, Ordering::SeqCst);
    }

    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    fn close(&self) {
        self.alive.store(false, Ordering::Release);
    }
}

struct ImplicitIdleItem {
    session: ImplicitQuerySession,
    created: Instant,
    last_used: Instant,
    use_count: u64,
}

#[derive(Clone)]
struct QuerySessionPoolSettings {
    limit: usize,
    warm_up: usize,
    item_usage_limit: u64,
    item_usage_ttl: Duration,
    idle_ttl: Duration,
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

struct QuerySessionPoolInner {
    settings: QuerySessionPoolSettings,
    semaphore: Arc<Semaphore>,
    implicit_idle: Mutex<Vec<ImplicitIdleItem>>,
}

#[derive(Clone)]
pub(crate) struct QuerySessionPool {
    inner: Arc<QuerySessionPoolInner>,
}

impl QuerySessionPool {
    fn new(settings: QuerySessionPoolSettings) -> Self {
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

    async fn acquire(&self) -> YdbResult<ImplicitSessionLease> {
        let permit = self
            .inner
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| YdbError::Transport("query session pool closed".to_string()))?;

        if let Some(item) = self.inner.pop_idle() {
            if !self.inner.should_close(&item) {
                return Ok(ImplicitSessionLease::new(item, self.inner.clone(), permit));
            }
            item.session.close();
        }

        Ok(ImplicitSessionLease::new(
            ImplicitIdleItem {
                session: ImplicitQuerySession::new(),
                created: Instant::now(),
                last_used: Instant::now(),
                use_count: 0,
            },
            self.inner.clone(),
            permit,
        ))
    }
}

impl QuerySessionPoolInner {
    fn pop_idle(&self) -> Option<ImplicitIdleItem> {
        self.implicit_idle.lock().expect("implicit idle lock").pop()
    }

    fn should_close(&self, item: &ImplicitIdleItem) -> bool {
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

    async fn release(&self, mut item: ImplicitIdleItem, permit: Option<OwnedSemaphorePermit>) {
        item.use_count += 1;
        item.last_used = Instant::now();
        if self.should_close(&item) {
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

struct ImplicitSessionLease {
    item: Option<ImplicitIdleItem>,
    pool: Arc<QuerySessionPoolInner>,
    permit: Option<OwnedSemaphorePermit>,
    returned: bool,
    use_guard: bool,
}

impl ImplicitSessionLease {
    fn new(
        item: ImplicitIdleItem,
        pool: Arc<QuerySessionPoolInner>,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            item: Some(item),
            pool,
            permit: Some(permit),
            returned: false,
            use_guard: false,
        }
    }

    fn session_id(&self) -> &str {
        self.item.as_ref().expect("lease item").session.session_id()
    }

    fn begin_use(&mut self) {
        if !self.use_guard {
            self.item.as_ref().expect("lease item").session.begin_use();
            self.use_guard = true;
        }
    }

    fn end_use(&mut self) {
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
                pool.release(item, permit).await;
            });
        }
    }
}

// --- query execution ---

fn operation_timeout(opts: &CallOptions, defaults: &TimeoutSettings) -> Duration {
    opts.timeout.unwrap_or(defaults.operation_timeout)
}

async fn with_operation_timeout<T, F>(timeout_duration: Duration, operation: F) -> YdbResult<T>
where
    F: Future<Output = YdbResult<T>>,
{
    match timeout(timeout_duration, operation).await {
        Ok(result) => result,
        Err(_) => Err(YdbError::Transport(format!(
            "operation timed out after {timeout_duration:?}"
        ))),
    }
}

async fn query_client(ctx: &ClientExecContext) -> YdbResult<RawQueryClient> {
    ctx.connection_manager
        .get_auth_service(RawQueryClient::new)
        .await
}

async fn retry_with_budget<T, F, Fut>(
    idempotent: bool,
    retry_budget: Duration,
    mut attempt_fn: F,
) -> YdbResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = YdbResult<T>>,
{
    let start = Instant::now();
    let mut attempt = 0usize;
    loop {
        attempt += 1;
        match attempt_fn().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if !should_retry_ydb_error(idempotent, &err) {
                    return Err(err);
                }
                match retry_wait(attempt, start.elapsed(), retry_budget) {
                    Some(wait) if wait > Duration::ZERO => sleep(wait).await,
                    Some(_) => {}
                    None => return Err(err),
                }
            }
        }
    }
}

async fn begin_stream_once(
    ctx: &ClientExecContext,
    text: &str,
    params: &HashMap<String, Value>,
    opts: &CallOptions,
) -> YdbResult<ExecuteQueryStream> {
    let timeout_duration = operation_timeout(opts, &ctx.timeouts);
    let mut lease = ctx.session_pool.acquire().await?;
    lease.begin_use();
    let mut client = query_client(ctx).await?;
    let req = RawExecuteQueryRequest::new(
        lease.session_id(),
        text,
        params.clone(),
        None,
        opts.collect_stats,
    );
    let stream = with_operation_timeout(timeout_duration, async {
        client.execute_query(req).await.map_err(Into::into)
    })
    .await?;
    Ok(ExecuteQueryStream::new(stream).with_session_guard(lease))
}

pub(crate) async fn client_begin_stream(
    ctx: &mut ClientExecContext,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
) -> YdbResult<ExecuteQueryStream> {
    let idempotent = opts.idempotent.unwrap_or(ctx.idempotent_operation);
    retry_with_budget(idempotent, ctx.retry_budget, || {
        begin_stream_once(ctx, &text, &params, &opts)
    })
    .await
}

pub(crate) fn new_exec_context(
    connection_manager: GrpcConnectionManager,
    timeouts: TimeoutSettings,
) -> ClientExecContext {
    ClientExecContext {
        connection_manager,
        timeouts,
        idempotent_operation: false,
        retry_budget: DEFAULT_RETRY_BUDGET,
        session_pool: QuerySessionPool::new(QuerySessionPoolSettings::default()),
    }
}

pub(crate) fn should_retry_ydb_error(idempotent: bool, err: &YdbError) -> bool {
    match err.need_retry() {
        NeedRetry::True => true,
        NeedRetry::IdempotentOnly => idempotent,
        NeedRetry::False => false,
    }
}

pub(crate) fn retry_wait(
    attempt: usize,
    time_from_start: Duration,
    retry_budget: Duration,
) -> Option<Duration> {
    if time_from_start >= retry_budget {
        return None;
    }
    let wait = if attempt > 0 {
        let exp_shift = (attempt - 1).min(63) as u32;
        let base_ms = INITIAL_RETRY_BACKOFF_MILLISECONDS
            .saturating_mul(1u64 << exp_shift)
            .min(MAX_RETRY_BACKOFF_MILLISECONDS);
        let base = Duration::from_millis(base_ms);
        let half = base / 2;
        if half.is_zero() {
            base
        } else {
            half + Duration::from_millis(rand::thread_rng().gen_range(0..=half.as_millis() as u64))
        }
    } else {
        Duration::ZERO
    };
    if time_from_start + wait < retry_budget {
        Some(wait)
    } else {
        None
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn retry_helpers_and_wait() {
        assert!(should_retry_ydb_error(
            true,
            &YdbError::Transport("timeout".into())
        ));
        assert!(!should_retry_ydb_error(
            false,
            &YdbError::Transport("timeout".into())
        ));

        let budget = Duration::from_millis(100);
        let wait1 = retry_wait(1, Duration::ZERO, budget).expect("wait");
        assert!(wait1 > Duration::ZERO);
        assert!(retry_wait(10, budget, budget).is_none());
    }
}
