//! Retry backoffs, timeouts, strategies and client-side rate limiting.
//!
//! A [`RetrySettings`] instance is shared by all service clients created from
//! the same [`Client`](crate::Client).

use futures_util::{
    FutureExt,
    future::{self, BoxFuture},
};
use rand::Rng;
use std::{
    fmt::Debug,
    ops::ControlFlow,
    pin::pin,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::Semaphore, time::MissedTickBehavior};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{instrument, trace, warn};

use crate::{AsyncFnMut, RefWithLifetime, YdbError, YdbResult, closure, errors::Idempotency};

/// Retry settings.
///
/// Defines retry strategy and deadlines for retried operations.
#[derive(Debug, Clone)]
pub struct RetrySettings {
    strategy: Arc<dyn BoxRetryStrategy>,
    deadline: Arc<dyn BoxRetryDeadline>,
}

impl RetrySettings {
    /// Default retry timeout.
    ///
    /// Can be set using [`Self::with_default_timeout`] method.
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

    /// Constructs a retry settings with default
    /// exponential backoff without any deadlines.
    pub fn with_default_backoff() -> Self {
        Self {
            strategy: Arc::new(ExponentialBackoff::default()),
            deadline: Arc::new(NoDeadline),
        }
    }

    /// Constructs a retry settings that allows no retries.
    pub fn dont_retry() -> Self {
        Self::new(DontRetry)
    }

    /// Constructs a retry settings from a retry strategy.
    ///
    /// Note that this function doesn't include
    /// exponential backoff automatically. Use it only
    /// when you want to construct retry settings
    /// from scratch. Otherwise you probably want
    /// [`RetrySettings::with_default_backoff`].
    pub fn new<S: RetryStrategy>(strategy: S) -> Self {
        Self {
            strategy: Arc::new(strategy),
            deadline: Arc::new(NoDeadline),
        }
    }

    /// Adds another deadline to the retry budget on top of existing deadlines.
    ///
    /// Deadline is exceeded when either of deadlines is exceeded.
    pub fn with_deadline<D: RetryDeadline>(self, deadline: D) -> RetrySettings {
        RetrySettings {
            strategy: self.strategy,
            deadline: Arc::new(Combine(self.deadline, deadline)),
        }
    }

    /// Adds the default timeout to the retry budget.
    ///
    /// This doesn't remove any existing retry deadlines from
    /// the budget.
    ///
    /// The default timeout is [`Self::DEFAULT_TIMEOUT`].
    pub fn with_default_timeout(self) -> RetrySettings {
        self.with_deadline(Self::DEFAULT_TIMEOUT)
    }

    /// Adds another retry strategy on top of existing strategies.
    ///
    /// Their delays are applied in parallel.
    pub fn with<T: RetryStrategy>(self, strategy: T) -> RetrySettings {
        RetrySettings {
            strategy: Arc::new(Combine(self.strategy, strategy)),
            deadline: self.deadline,
        }
    }

    /// Waits for the deadline.
    ///
    /// Can be used to manually implement retry loop in difficult
    /// cases. Not recommended to use. If you use it, make sure
    /// that all your operations are aborted when deadline is exceeded.
    /// Also make sure that the deadline is polled at the start of the loop.
    pub(crate) async fn wait_deadline(&self) {
        self.deadline.wait_deadline().await
    }

    /// Applies deadline for given retry loop future.
    pub(crate) async fn run_with_deadline<D: Future<Output = ()>, F: Future>(
        deadline: D,
        f: F,
    ) -> Option<F::Output> {
        tokio::select! {
            biased;
            res = f => Some(res),
            () = deadline => None
        }
    }

    /// Waits until retry or deadline.
    ///
    /// Returns whether to continue retries.
    pub(crate) async fn wait_retry(&self, retry: &RetryState) -> ControlFlow<()> {
        self.strategy.wait_retry(retry).await
    }

    /// Makes an attempt with proper tracing.
    #[instrument(name = "ydb.Try", skip_all, fields(
        ydb.retry.attempt = retry.attempt,
        ydb.retry.backoff_ms = tracing::field::Empty,
        db.system.name = "ydb",
    ))]
    pub(crate) async fn attempt<F: AsyncFnMut<RefWithLifetime<RetryState>>>(
        closure: &mut F,
        retry: &RetryState,
    ) -> F::Output {
        closure.call(retry).await
    }

    /// Runs retry-wait loop.
    ///
    /// Calls `attempt_fn` until it returns [`ControlFlow::Break`]
    /// or the retrier asks to stop. Waits between retries.
    pub(crate) async fn retry<B, C, F>(&self, mut attempt_fn: F) -> ControlFlow<B, Option<C>>
    where
        F: AsyncFnMut<RefWithLifetime<RetryState>, Output = ControlFlow<B, C>>,
    {
        let mut attempt_result = None;
        let loop_result = Self::run_with_deadline(self.deadline.wait_deadline(), async {
            let mut retry = RetryState::init();

            loop {
                attempt_result = Some(Self::attempt(&mut attempt_fn, &retry).await?);

                let should_continue = self.strategy.wait_retry(&retry).await.is_continue();

                if !should_continue {
                    return ControlFlow::Continue(());
                }

                retry.attempt += 1;
            }
        })
        .await;

        match loop_result {
            Some(ControlFlow::Break(value)) => ControlFlow::Break(value),
            Some(ControlFlow::Continue(())) | None => ControlFlow::Continue(attempt_result),
        }
    }

    /// Runs retry-wait loop retrying on retriable errors.
    pub(crate) async fn retry_on_retriable_errors<T, F>(
        &self,
        idempotency: Idempotency,
        attempt_fn: F,
    ) -> YdbResult<T>
    where
        F: AsyncFnMut<RefWithLifetime<RetryState>, Output = YdbResult<T>>,
    {
        let result = self
            .retry(closure!([attempt_fn, idempotency], async |retry| {
                match attempt_fn.call(retry).await {
                    Ok(value) => ControlFlow::Break(Ok(value)),
                    Err(err) => {
                        trace!("attempt failed: {err}");
                        err.retry_flow(*idempotency)
                    }
                }
            }))
            .await;

        match result {
            ControlFlow::Continue(err) => Err(err.unwrap_or(YdbError::DeadlineExceeded)),
            ControlFlow::Break(Err(err)) => Err(err),
            ControlFlow::Break(Ok(value)) => Ok(value),
        }
    }
}

/// State of a retried operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RetryState {
    /// Number of the failed attempt.
    ///
    /// Starts from zero.
    pub attempt: usize,

    /// Start time of retrying loop.
    pub start_time: Instant,
}

impl RetryState {
    /// Constructs a state for retry loop
    /// that starts now.
    pub fn init() -> Self {
        Self {
            attempt: 0,
            start_time: Instant::now(),
        }
    }
}

/// Retry strategy.
///
/// Should be used with [`RetrySettings`].
pub trait RetryStrategy: Debug + Send + Sync + 'static {
    /// Returns a future that waits before the next retry.
    ///
    /// Note that the future can be created before the time it's polled.
    ///
    /// Its output tells whether to continue retries.
    ///
    /// Can be a mere `async` method, as long as its future
    /// meets trait and lifetime bounds.
    fn wait_retry<'a>(
        &'a self,
        retry: &'a RetryState,
    ) -> impl Future<Output = ControlFlow<()>> + Send + 'a;
}

trait BoxRetryStrategy: Debug + Send + Sync + 'static {
    fn wait_retry_boxed<'a>(&'a self, retry: &'a RetryState) -> BoxFuture<'a, ControlFlow<()>>;
}

impl RetryStrategy for Arc<dyn BoxRetryStrategy> {
    fn wait_retry<'a>(
        &'a self,
        retry: &'a RetryState,
    ) -> impl Future<Output = ControlFlow<()>> + Send + 'a {
        self.wait_retry_boxed(retry)
    }
}

impl<S: RetryStrategy> BoxRetryStrategy for S {
    fn wait_retry_boxed<'a>(&'a self, retry: &'a RetryState) -> BoxFuture<'a, ControlFlow<()>> {
        self.wait_retry(retry).boxed()
    }
}

/// Retry strategy that doesn't allow retries.
#[derive(Debug, Clone, Copy)]
pub struct DontRetry;

impl RetryStrategy for DontRetry {
    async fn wait_retry(&self, _retry: &RetryState) -> ControlFlow<()> {
        ControlFlow::Break(())
    }
}

/// Exponential backoff retry strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExponentialBackoff {
    /// The initial wait duration.
    ///
    /// Defaults to [`Self::DEFAULT_INITIAL_WAIT_DURATION`].
    pub initial: Duration,

    /// The maximum wait duration.
    ///
    /// Defaults to [`Self::DEFAULT_MAX_WAIT_DURATION`].
    pub max: Duration,

    /// Wait duration multiplier per attempt.
    ///
    /// Defaults to [`Self::DEFAULT_BACKOFF_MULTIPLIER`].
    pub multiplier: u32,
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self {
            initial: Self::DEFAULT_INITIAL_WAIT_DURATION,
            max: Self::DEFAULT_MAX_WAIT_DURATION,
            multiplier: Self::DEFAULT_BACKOFF_MULTIPLIER,
        }
    }
}

impl ExponentialBackoff {
    /// Default initial backoff wait duration.
    pub const DEFAULT_INITIAL_WAIT_DURATION: Duration = Duration::from_millis(2);

    /// Default maximum backoff wait duration.
    pub const DEFAULT_MAX_WAIT_DURATION: Duration = Duration::from_secs(10);

    /// Default backoff multiplier.
    pub const DEFAULT_BACKOFF_MULTIPLIER: u32 = 2;

    /// Sets the initial wait duration.
    pub fn initial(mut self, initial: Duration) -> Self {
        self.initial = initial;
        self
    }

    /// Sets the maximum wait duration.
    pub fn max(mut self, max: Duration) -> Self {
        self.max = max;
        self
    }

    /// Sets the backoff multiplier.
    pub fn multiplier(mut self, multiplier: u32) -> Self {
        self.multiplier = multiplier;
        self
    }

    fn wait_duration(&self, retry: usize) -> Duration {
        let total_multiplier = self
            .multiplier
            .saturating_pow(retry.try_into().unwrap_or(u32::MAX));

        self.initial.saturating_mul(total_multiplier).min(self.max)
    }

    /// Runs retry-wait loop until an attempt results in `Some(_)`.
    ///
    /// This method ignores deadlines and [`ControlFlow`] returned by retry strategy,
    /// so it should be used only with retry settings that never stops retries.
    /// Unfortunately, this limitation is not expressible on type level,
    /// because we decided to type-erase deadlines and strategies,
    /// so this method is `pub(crate)` and should be used with care.
    pub(crate) async fn retry_indefinitely<T, F>(&self, mut attempt_fn: F) -> T
    where
        F: AsyncFnMut<RefWithLifetime<RetryState>, Output = Option<T>>,
    {
        let mut retry = RetryState::init();

        loop {
            let attempt_result = RetrySettings::attempt(&mut attempt_fn, &retry).await;

            if let Some(value) = attempt_result {
                return value;
            } else {
                trace!("attempt failed");
                _ = self.wait_retry(&retry).await;
            }

            retry.attempt += 1;
        }
    }
}

impl RetryStrategy for ExponentialBackoff {
    async fn wait_retry(&self, retry: &RetryState) -> ControlFlow<()> {
        tokio::time::sleep(self.wait_duration(retry.attempt)).await;
        ControlFlow::Continue(())
    }
}

impl<S: RetryStrategy + ?Sized> RetryStrategy for Box<S> {
    async fn wait_retry(&self, retry: &RetryState) -> ControlFlow<()> {
        S::wait_retry(self, retry).await
    }
}

impl<S: RetryStrategy + ?Sized> RetryStrategy for Arc<S> {
    async fn wait_retry(&self, retry: &RetryState) -> ControlFlow<()> {
        S::wait_retry(self, retry).await
    }
}

/// Retry strategy with limited number of retry attempts per second,
/// using token bucket technique.
///
/// If the budget is exhausted, waits until a slot appears.
///
/// Initializing it with zero attempts per second
/// makes it identical to [`DontRetry`].
///
/// Aligned `budget.Limited` retry budget from YDB Go SDK.
#[derive(Debug)]
pub struct RetriesPerSecond {
    semaphore: Option<Arc<Semaphore>>,
    _drop_guard: Option<DropGuard>,
}

impl RetriesPerSecond {
    pub fn new(attempts_per_second: u32) -> Self {
        if attempts_per_second == 0 {
            // Zero is a special case
            return Self {
                semaphore: None,
                _drop_guard: None,
            };
        }

        let capacity = attempts_per_second as usize;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(capacity));

        let cancellation = CancellationToken::new();
        let drop_guard = cancellation.clone().drop_guard();

        let interval = Duration::from_secs(1) / attempts_per_second;
        let semaphore_refill = semaphore.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // Skip the first tick as it's immediate
            ticker.tick().await;
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select!(
                    _ = cancellation.cancelled() => break,
                    _ = ticker.tick() => {
                        if semaphore_refill.available_permits() < capacity {
                            semaphore_refill.add_permits(1);
                        }
                    }
                );
            }
        });

        Self {
            semaphore: Some(semaphore),
            _drop_guard: Some(drop_guard),
        }
    }
}

impl RetryStrategy for RetriesPerSecond {
    async fn wait_retry(&self, _retry: &RetryState) -> ControlFlow<()> {
        if let Some(semaphore) = self.semaphore.as_ref() {
            let Ok(permit) = semaphore.acquire().await else {
                warn!("semaphore that must never be closed is closed");
                return ControlFlow::Break(());
            };
            permit.forget();
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
    }
}

/// Probabilistic rate limiter.
///
/// Each subsequent retry attempt is allowed with probability `percent / 100`.
///
/// Aligned with `budget.Percent` from YDB Go SDK.
#[derive(Debug, Clone)]
pub struct RetryProbability {
    percent: u32,
}

impl RetryProbability {
    pub fn new(percent: u32) -> Self {
        assert!(
            percent <= 100,
            "percent must be between 0 and 100, got {percent}"
        );
        Self { percent }
    }
}

impl RetryStrategy for RetryProbability {
    async fn wait_retry(&self, _retry: &RetryState) -> ControlFlow<()> {
        if rand::thread_rng().gen_range(0..100) < self.percent {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
    }
}

/// Retry deadline.
///
/// Should be used with [`RetrySettings`].
pub trait RetryDeadline: Debug + Send + Sync + 'static {
    /// Returns a future that waits for the retry deadline.
    ///
    /// It is called once per retry loop.
    /// Its future is guaranteed to be polled at the start of the loop.
    ///
    /// When it completes, attempts should be stopped.
    ///
    /// Can be a mere `async` method, as long as its future
    /// meets trait and lifetime bounds.
    fn wait_deadline(&self) -> impl Future<Output = ()> + Send + '_;
}

trait BoxRetryDeadline: Debug + Send + Sync + 'static {
    fn wait_deadline_boxed(&self) -> BoxFuture<'_, ()>;
}

impl<D: RetryDeadline> BoxRetryDeadline for D {
    fn wait_deadline_boxed(&self) -> BoxFuture<'_, ()> {
        self.wait_deadline().boxed()
    }
}

impl RetryDeadline for Arc<dyn BoxRetryDeadline> {
    fn wait_deadline(&self) -> impl Future<Output = ()> + Send + '_ {
        self.wait_deadline_boxed()
    }
}

/// Retry deadline that is never exceeded.
#[derive(Debug, Clone, Copy)]
struct NoDeadline;

impl RetryDeadline for NoDeadline {
    async fn wait_deadline(&self) {
        future::pending().await
    }
}

impl RetryDeadline for Duration {
    async fn wait_deadline(&self) {
        tokio::time::sleep_until((Instant::now() + *self).into()).await
    }
}

impl RetryDeadline for Instant {
    async fn wait_deadline(&self) {
        tokio::time::sleep_until((*self).into()).await
    }
}

impl RetryDeadline for CancellationToken {
    async fn wait_deadline(&self) {
        self.cancelled().await
    }
}

impl<D: RetryDeadline> RetryDeadline for Option<D> {
    async fn wait_deadline(&self) {
        match self {
            Some(deadline) => deadline.wait_deadline().await,
            None => future::pending().await,
        }
    }
}

impl<D: RetryDeadline + ?Sized> RetryDeadline for Box<D> {
    fn wait_deadline(&self) -> impl Future<Output = ()> + Send + '_ {
        D::wait_deadline(self)
    }
}

impl<D: RetryDeadline + ?Sized> RetryDeadline for Arc<D> {
    fn wait_deadline(&self) -> impl Future<Output = ()> + Send + '_ {
        D::wait_deadline(self)
    }
}

/// Helper type for combining deadlines and retry strategies.
#[derive(Debug)]
struct Combine<A, B>(A, B);

impl<A: RetryStrategy, B: RetryStrategy> RetryStrategy for Combine<A, B> {
    async fn wait_retry(&self, retry: &RetryState) -> ControlFlow<()> {
        let first_future = pin!(self.0.wait_retry(retry));
        let second_future = pin!(self.1.wait_retry(retry));
        let select_result = future::select(first_future, second_future).await;

        match select_result {
            future::Either::Left((result, other_future)) => {
                result?;
                other_future.await
            }
            future::Either::Right((result, other_future)) => {
                result?;
                other_future.await
            }
        }
    }
}

impl<A: RetryDeadline, B: RetryDeadline> RetryDeadline for Combine<A, B> {
    async fn wait_deadline(&self) {
        tokio::select! {
            _ = self.0.wait_deadline() => (),
            _ = self.1.wait_deadline() => (),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct ConstantBackoff(Duration);

    impl RetryStrategy for ConstantBackoff {
        async fn wait_retry(&self, _retry: &RetryState) -> ControlFlow<()> {
            tokio::time::sleep(self.0).await;
            ControlFlow::Continue(())
        }
    }

    #[derive(Debug)]
    struct WaitTrap {
        waited: std::sync::Mutex<bool>,
    }

    impl WaitTrap {
        fn new() -> Self {
            Self {
                waited: Default::default(),
            }
        }

        fn waited(&self) -> bool {
            *self.waited.lock().unwrap()
        }
    }

    impl RetryStrategy for WaitTrap {
        async fn wait_retry(&self, _retry: &RetryState) -> ControlFlow<()> {
            *self.waited.lock().unwrap() = true;
            ControlFlow::Continue(())
        }
    }

    #[tokio::test]
    async fn dont_retry_dont_retries() {
        let retry_budget = RetrySettings::dont_retry();

        assert!(
            tokio::time::timeout(
                Duration::from_millis(15),
                retry_budget.wait_retry(&RetryState::init()),
            )
            .await
            .unwrap()
            .is_break()
        );
    }

    #[tokio::test]
    async fn combine_deadlines() {
        let start = Instant::now();
        Combine(Duration::from_secs(1), Duration::from_secs(1))
            .wait_deadline()
            .await;
        // Deadline composition is their minimum
        assert!(start.elapsed() >= Duration::from_secs(1));
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn combine_backoffs() {
        let start = Instant::now();

        let result = Combine(
            ConstantBackoff(Duration::from_secs(1)),
            ConstantBackoff(Duration::from_secs(1)),
        )
        .wait_retry(&RetryState::init())
        .await;

        assert!(result.is_continue());
        assert!(start.elapsed() >= Duration::from_secs(1));
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn combine_first_fail() {
        let first_trap = Arc::new(WaitTrap::new());
        let last_trap = Arc::new(WaitTrap::new());
        let retry_settings = RetrySettings::new(first_trap.clone())
            .with(DontRetry)
            .with(last_trap.clone());

        assert!(
            retry_settings
                .wait_retry(&RetryState::init())
                .await
                .is_break()
        );

        assert!(first_trap.waited());
        assert!(!last_trap.waited());
    }

    #[tokio::test]
    async fn limited_budget_respects_rate() {
        async fn try_wait_retry(retry_strategy: &impl RetryStrategy) -> Option<ControlFlow<()>> {
            tokio::time::timeout(
                Duration::from_millis(50),
                retry_strategy.wait_retry(&RetryState::init()),
            )
            .await
            .ok()
        }

        tokio::time::pause();

        let strategy = RetriesPerSecond::new(1);
        assert!(strategy.wait_retry(&RetryState::init()).await.is_continue());
        let second = try_wait_retry(&strategy).await;
        assert!(second.is_none());
        tokio::time::advance(Duration::from_secs(2)).await;

        assert!(try_wait_retry(&strategy).await.unwrap().is_continue());
        assert!(try_wait_retry(&strategy).await.is_none());
    }

    #[tokio::test]
    async fn limited_zero_denies_retries() {
        let budget = RetriesPerSecond::new(0);
        let result = tokio::time::timeout(
            Duration::from_millis(20),
            budget.wait_retry(&RetryState::init()),
        )
        .await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_break());
    }
}
