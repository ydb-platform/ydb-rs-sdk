//! Retry backoffs, timeouts, strategies and client-side rate limiting.
//!
//! A [`RetryBudget`] instance is shared by all service clients created from
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
use tokio::sync::Semaphore;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{instrument, trace, warn};

use crate::{
    YdbResult,
    async_closure::{AsyncFnMut, with_lifetime::Ref},
    closure,
    errors::Idempotency,
};

/// Retry budget.
#[derive(Debug, Clone, Copy)]
pub struct RetryBudget<S, D = NoDeadline> {
    strategy: S,
    deadline: D,
}

impl RetryBudget<ExponentialBackoff, NoDeadline> {
    /// Constructs a retry budget with default
    /// exponential backoff without any deadlines.
    pub fn with_default_backoff() -> Self {
        Self {
            strategy: ExponentialBackoff::default(),
            deadline: NoDeadline,
        }
    }
}

impl Default for ArcRetryBudget {
    fn default() -> Self {
        RetryBudget::with_default_backoff().arc()
    }
}

/// Alias for type-erased retry budget.
pub type BoxRetryBudget = RetryBudget<Box<dyn BoxRetryStrategy>, Box<dyn BoxDeadline>>;

/// Alias for reference-counted type-erased retry budget.
pub type ArcRetryBudget = RetryBudget<Arc<dyn BoxRetryStrategy>, Arc<dyn BoxDeadline>>;

impl<S: RetryStrategy> RetryBudget<S> {
    /// Constructs a retry budget from a retry strategy.
    pub fn new(strategy: S) -> Self {
        Self {
            strategy,
            deadline: NoDeadline,
        }
    }

    /// Runs retry-wait loop until an attempt results in `Some(_)`.
    pub async fn retry_indefinitely<T, F>(&self, mut attempt_fn: F) -> T
    where
        S: RetryAlways,
        F: AsyncFnMut<Ref<RetryState>, Output = Option<T>>,
    {
        let mut retry = RetryState::init();

        loop {
            let attempt_result = Self::attempt(&mut attempt_fn, &retry).await;

            if let Some(value) = attempt_result {
                return value;
            } else {
                trace!("attempt failed");
                _ = self.strategy.wait_retry(&retry).await;
            }

            retry.attempt += 1;
        }
    }
}

impl RetryBudget<DontRetry, NoDeadline> {
    /// Constructs a retry budget that allows no retries.
    pub fn dont_retry() -> Self {
        Self::new(DontRetry)
    }
}

impl<S: RetryStrategy, D: RetryDeadline> RetryBudget<S, D> {
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

    /// Adds another deadline to the retry budget on top of existing deadlines.
    ///
    /// Deadline is exceeded when either of deadlines is exceeded.
    pub fn deadline<T: RetryDeadline>(self, deadline: T) -> RetryBudget<S, Combine<D, T>> {
        RetryBudget {
            strategy: self.strategy,
            deadline: Combine(self.deadline, deadline),
        }
    }

    /// Adds the default timeout to the retry budget.
    ///
    /// This doesn't remove any existing retry deadlines from
    /// the budget.
    ///
    /// The default timeout is [`Self::DEFAULT_TIMEOUT`].
    pub fn default_timeout(self) -> RetryBudget<S, Combine<D, Duration>> {
        self.deadline(Self::DEFAULT_TIMEOUT)
    }

    /// Adds another retry wait strategy on top of existing strategies.
    ///
    /// Waits are applied sequentially.
    pub fn and_then<T: RetryStrategy>(self, wait: T) -> RetryBudget<Combine<S, T>, D> {
        RetryBudget {
            strategy: Combine(self.strategy, wait),
            deadline: self.deadline,
        }
    }

    /// Type-erases the retry budget using [`Box`].
    pub fn boxed(self) -> BoxRetryBudget
    where
        S: 'static,
        D: 'static,
    {
        RetryBudget {
            strategy: Box::new(self.strategy),
            deadline: Box::new(self.deadline),
        }
    }

    /// Type-erases retry budget using [`Arc`].
    pub fn arc(self) -> ArcRetryBudget
    where
        S: 'static,
        D: 'static,
    {
        RetryBudget {
            strategy: Arc::new(self.strategy),
            deadline: Arc::new(self.deadline),
        }
    }

    /// Returns a retry budget that borrows
    /// the current one.
    pub fn as_ref(&self) -> RetryBudget<RefStrategy<'_, S>, RefDeadline<'_, D>> {
        RetryBudget {
            strategy: self.strategy.as_ref_strategy(),
            deadline: self.deadline.as_ref_deadline(),
        }
    }

    /// Waits until retry or deadline.
    ///
    /// Returns whether to continue retries.
    pub async fn wait_retry(&self, retry: &RetryState) -> ControlFlow<()> {
        tokio::select! {
            biased;
            () = self.deadline.wait_deadline() => ControlFlow::Break(()),
            control_flow = self.strategy.wait_retry(retry) => control_flow
        }
    }

    /// Makes an attempt with proper tracing.
    #[instrument(name = "ydb.Try", skip_all, fields(
        ydb.retry.attempt = retry.attempt,
        ydb.retry.backoff_ms = tracing::field::Empty,
        db.system.name = "ydb",
    ))]
    async fn attempt<F: AsyncFnMut<Ref<RetryState>>>(
        closure: &mut F,
        retry: &RetryState,
    ) -> F::Output {
        closure.call(retry).await
    }

    /// Runs retry-wait loop.
    ///
    /// Calls `attempt_fn` until it returns [`ControlFlow::Break`]
    /// or the retrier asks to stop. Waits between retries.
    pub async fn retry<B, C, F>(&self, mut attempt_fn: F) -> ControlFlow<B, C>
    where
        F: AsyncFnMut<Ref<RetryState>, Output = ControlFlow<B, C>>,
    {
        let mut deadline_exceeded = pin!(self.deadline.wait_deadline());
        let mut retry = RetryState::init();

        loop {
            let attempt_result = Self::attempt(&mut attempt_fn, &retry).await?;

            let should_continue = tokio::select! {
                biased;
                () = &mut deadline_exceeded => false,
                control_flow = self.strategy.wait_retry(&retry) => control_flow.is_continue()
            };

            if !should_continue {
                return ControlFlow::Continue(attempt_result);
            }

            retry.attempt += 1;
        }
    }

    /// Runs retry-wait loop retrying on errors.
    pub async fn retry_on_errors<T, E, F>(&self, attempt_fn: F) -> Result<T, E>
    where
        F: AsyncFnMut<Ref<RetryState>, Output = Result<T, E>>,
        E: std::error::Error,
    {
        let result = self
            .retry(closure!([attempt_fn], async |retry| {
                match attempt_fn.call(retry).await {
                    Ok(value) => ControlFlow::Break(value),
                    Err(err) => {
                        trace!("attempt failed: {err}");
                        ControlFlow::Continue(err)
                    }
                }
            }))
            .await;

        match result {
            ControlFlow::Break(value) => Ok(value),
            ControlFlow::Continue(err) => Err(err),
        }
    }

    /// Runs retry-wait loop retrying on retriable errors.
    pub(crate) async fn retry_on_retriable_errors<T, F>(
        &self,
        idempotency: Idempotency,
        attempt_fn: F,
    ) -> YdbResult<T>
    where
        F: AsyncFnMut<Ref<RetryState>, Output = YdbResult<T>>,
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
            ControlFlow::Continue(err) | ControlFlow::Break(Err(err)) => Err(err),
            ControlFlow::Break(Ok(value)) => Ok(value),
        }
    }
}

/// State of a retried operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RetryState {
    /// Number of the current attempt.
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

    /// Resets retry state.
    pub fn reset(&mut self) {
        *self = Self::init();
    }
}

/// Retry wait strategy.
///
/// Should be used with [`RetryBudget`].
pub trait RetryStrategy: Send + Sync {
    /// Returns a future that waits before the next retry.
    ///
    /// Note that the future can be created before the time it's polled.
    ///
    /// Its output tells whether to continue retries.
    fn wait_retry<'a>(
        &'a self,
        _retry: &'a RetryState,
    ) -> impl Future<Output = ControlFlow<()>> + Send + 'a {
        future::ready(ControlFlow::Continue(()))
    }
}

/// Extension trait that provides useful methods for retry strategies.
pub trait RetryStrategyExt: RetryStrategy {
    /// Returns a borrowed retry strategy.
    fn as_ref_strategy(&self) -> RefStrategy<'_, Self> {
        RefStrategy(self)
    }
}

impl<S: RetryStrategy> RetryStrategyExt for S {}

/// Retry wait strategy that never asks to stop.
///
/// This trait should be implemented for retry strategies
/// that always returns [`ControlFlow::Continue`]. It also
/// implies that output of its [`RetryWait::wait_retry`]
/// can be ignored.
pub trait RetryAlways: Send + Sync + RetryStrategy {}

/// Retry wait strategy that doesn't allow retries.
#[derive(Debug, Clone, Copy)]
pub struct DontRetry;

impl RetryStrategy for DontRetry {
    fn wait_retry<'a>(
        &'a self,
        _retry: &'a RetryState,
    ) -> impl Future<Output = ControlFlow<()>> + Send + 'a {
        future::ready(ControlFlow::Break(()))
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
}

impl RetryStrategy for ExponentialBackoff {
    fn wait_retry<'a>(
        &'a self,
        retry: &'a RetryState,
    ) -> impl Future<Output = ControlFlow<()>> + Send + 'a {
        tokio::time::sleep(self.wait_duration(retry.attempt)).map(ControlFlow::Continue)
    }
}

impl RetryAlways for ExponentialBackoff {}

/// Borrowed retry strategy that is a retry strategy itself.
pub struct RefStrategy<'a, S: RetryStrategy + ?Sized>(pub &'a S);

impl<'s, S: RetryStrategy> RetryStrategy for RefStrategy<'s, S> {
    fn wait_retry<'a>(
        &'a self,
        retry: &'a RetryState,
    ) -> impl Future<Output = ControlFlow<()>> + Send + 'a {
        self.0.wait_retry(retry)
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
    async fn wait_retry<'a>(&'a self, _retry: &'a RetryState) -> ControlFlow<()> {
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
    async fn wait_retry<'a>(&'a self, _retry: &'a RetryState) -> ControlFlow<()> {
        if rand::thread_rng().gen_range(0..100) < self.percent {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
    }
}

/// Retry deadline.
///
/// Should be used with [`RetryBudget`].
pub trait RetryDeadline: Send + Sync {
    /// Returns a future that waits for the retry deadline.
    ///
    /// It can be called once per retry loop or each time
    /// and should behave correctly in both cases.
    ///
    /// When it completes, retries should be stopped.
    fn wait_deadline(&self) -> impl Future<Output = ()> + Send + '_;
}

/// Extension trait that provides useful methods for retry deadlines.
pub trait RetryDeadlineExt: RetryDeadline {
    /// Returns a borrowed retry deadline.
    fn as_ref_deadline(&self) -> RefDeadline<'_, Self> {
        RefDeadline(self)
    }
}

impl<D: RetryDeadline> RetryDeadlineExt for D {}

/// Retry deadline that is never exceeded.
#[derive(Debug, Clone, Copy)]
pub struct NoDeadline;

impl RetryDeadline for NoDeadline {
    fn wait_deadline(&self) -> impl Future<Output = ()> + Send {
        future::pending()
    }
}

impl RetryDeadline for Duration {
    fn wait_deadline(&self) -> impl Future<Output = ()> + Send {
        let timeout = *self;
        async move { tokio::time::sleep_until((Instant::now() + timeout).into()).await }
    }
}

impl RetryDeadline for Instant {
    fn wait_deadline(&self) -> impl Future<Output = ()> + Send {
        tokio::time::sleep_until((*self).into())
    }
}

impl RetryDeadline for CancellationToken {
    fn wait_deadline(&self) -> impl Future<Output = ()> + Send + '_ {
        self.cancelled()
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

/// Borrowed retry deadline that is retry deadline itself.
pub struct RefDeadline<'a, D: RetryDeadline + ?Sized>(pub &'a D);

impl<'a, D: RetryDeadline> RetryDeadline for RefDeadline<'a, D> {
    fn wait_deadline(&self) -> impl Future<Output = ()> + Send + '_ {
        self.0.wait_deadline()
    }
}

/// Dyn-safe counterpart of [`RetryStrategy`] trait.
pub trait BoxRetryStrategy: Send + Sync {
    fn wait_retry_boxed<'a>(&'a self, retry: &'a RetryState) -> BoxFuture<'a, ControlFlow<()>>;
}

impl<S: RetryStrategy> BoxRetryStrategy for S {
    fn wait_retry_boxed<'a>(&'a self, retry: &'a RetryState) -> BoxFuture<'a, ControlFlow<()>> {
        self.wait_retry(retry).boxed()
    }
}

impl<'s> RetryStrategy for Box<dyn BoxRetryStrategy + 's> {
    fn wait_retry<'a>(
        &'a self,
        retry: &'a RetryState,
    ) -> impl Future<Output = ControlFlow<()>> + Send + 'a {
        self.as_ref().wait_retry_boxed(retry)
    }
}

impl<'s> RetryStrategy for Arc<dyn BoxRetryStrategy + 's> {
    fn wait_retry<'a>(
        &'a self,
        retry: &'a RetryState,
    ) -> impl Future<Output = ControlFlow<()>> + Send + 'a {
        self.as_ref().wait_retry_boxed(retry)
    }
}

/// Dyn-safe counterpart of [`RetryDeadline`] trait.
pub trait BoxDeadline: Send + Sync {
    fn wait_deadline_boxed(&self) -> BoxFuture<'_, ()>;
}

impl<D: RetryDeadline> BoxDeadline for D {
    fn wait_deadline_boxed(&self) -> BoxFuture<'_, ()> {
        self.wait_deadline().boxed()
    }
}

impl<'d> RetryDeadline for Box<dyn BoxDeadline + 'd> {
    fn wait_deadline(&self) -> impl Future<Output = ()> + Send + '_ {
        self.as_ref().wait_deadline_boxed()
    }
}

impl<'d> RetryDeadline for Arc<dyn BoxDeadline + 'd> {
    fn wait_deadline(&self) -> impl Future<Output = ()> + Send + '_ {
        self.as_ref().wait_deadline_boxed()
    }
}

/// Helper type for combining deadlines and retry wait strategies.
pub struct Combine<A, B>(A, B);

impl<A: RetryStrategy, B: RetryStrategy> RetryStrategy for Combine<A, B> {
    fn wait_retry<'a>(
        &'a self,
        retry: &'a RetryState,
    ) -> impl Future<Output = ControlFlow<()>> + Send + 'a {
        let left_future = self.0.wait_retry(retry);
        let right_future = self.1.wait_retry(retry);
        async move {
            left_future.await?;
            right_future.await
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

impl<A: RetryAlways, B: RetryAlways> RetryAlways for Combine<A, B> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn limited_budget_respects_rate() {
        let budget = RetriesPerSecond::new(1);
        assert!(budget.wait_retry(&RetryState::init()).await.is_continue());
        let second = tokio::time::timeout(
            Duration::from_millis(50),
            budget.wait_retry(&RetryState::init()),
        )
        .await;
        assert!(second.is_err());
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
