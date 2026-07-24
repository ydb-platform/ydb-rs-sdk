//! Retry backoffs, timeouts, strategies and client-side rate limiting.
//!
//! A [`RetrySettings`] instance is shared by all service clients created from
//! the same [`Client`](crate::Client).

use async_trait::async_trait;
use futures_util::future;
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

use crate::{AsyncFnMut, RefWithLifetime, YdbResult, closure, errors::Idempotency};

/// Retry settings.
///
/// Defines retry strategy and deadlines for retried operations.
#[derive(Debug, Clone, Copy)]
pub struct RetrySettings<S: RetryStrategy, D: RetryDeadline = NoDeadline> {
    strategy: S,
    deadline: D,
}

impl RetrySettings<ExponentialBackoff> {
    /// Constructs a retry settings with default
    /// exponential backoff without any deadlines.
    pub fn with_default_backoff() -> Self {
        Self {
            strategy: ExponentialBackoff::default(),
            deadline: NoDeadline,
        }
    }
}

impl RetrySettings<DontRetry> {
    /// Constructs a retry settings udget that allows no retries.
    pub fn dont_retry() -> Self {
        Self::new(DontRetry)
    }
}

impl Default for ArcRetrySettings {
    fn default() -> Self {
        RetrySettings::with_default_backoff().arc()
    }
}

/// Alias for type-erased retry settings.
///
/// Can be constructed from [`RetrySettings`]
/// using [`RetrySettings::arc`] method.
pub type BoxRetrySettings = RetrySettings<Box<dyn RetryStrategy>, Box<dyn RetryDeadline>>;

/// Alias for reference-counted type-erased retry settings.
///
/// Can be constructed from [`RetrySettings`]
/// using [`RetrySettings::boxed`] method.
pub type ArcRetrySettings = RetrySettings<Arc<dyn RetryStrategy>, Arc<dyn RetryDeadline>>;

impl<S: RetryStrategy> RetrySettings<S> {
    /// Constructs a retry settings from a retry strategy.
    ///
    /// Note that this function doesn't include
    /// exponential backoff automatically. Use it only
    /// when you want to construct retry settings
    /// from scratch. Otherwise you probably want
    /// [`RetrySettings::with_default_backoff`]
    /// or [`ArcRetrySettings::default`].
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
        F: AsyncFnMut<RefWithLifetime<RetryState>, Output = Option<T>>,
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

impl<S: RetryStrategy, D: RetryDeadline> RetrySettings<S, D> {
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

    /// Adds another deadline to the retry budget on top of existing deadlines.
    ///
    /// Deadline is exceeded when either of deadlines is exceeded.
    pub fn with_deadline<T: RetryDeadline>(self, deadline: T) -> RetrySettings<S, Combine<D, T>> {
        RetrySettings {
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
    pub fn with_default_timeout(self) -> RetrySettings<S, Combine<D, Duration>> {
        self.with_deadline(Self::DEFAULT_TIMEOUT)
    }

    /// Adds another retry strategy on top of existing strategies.
    ///
    /// Their delays are applied in parallel.
    pub fn with<T: RetryStrategy>(self, wait: T) -> RetrySettings<Combine<S, T>, D> {
        RetrySettings {
            strategy: Combine(self.strategy, wait),
            deadline: self.deadline,
        }
    }

    /// Maps `RetryStrategy<S, D>` to `RetryStrategy<NewS, D>`
    /// by applying a function to contained retry strategy.
    pub fn map_strategy<F, NewS>(self, f: F) -> RetrySettings<NewS, D>
    where
        F: FnOnce(S) -> NewS,
        NewS: RetryStrategy,
    {
        RetrySettings {
            strategy: f(self.strategy),
            deadline: self.deadline,
        }
    }

    /// Maps `RetryStrategy<S, D>` to `RetryStrategy<S, NewD>`
    /// by applying a function to contained deadline.
    pub fn map_deadline<F, NewD>(self, f: F) -> RetrySettings<S, NewD>
    where
        F: FnOnce(D) -> NewD,
        NewD: RetryDeadline,
    {
        RetrySettings {
            strategy: self.strategy,
            deadline: f(self.deadline),
        }
    }

    /// Type-erases the retry budget using [`Box`].
    pub fn boxed(self) -> BoxRetrySettings
    where
        S: 'static,
        D: 'static,
    {
        RetrySettings {
            strategy: Box::new(self.strategy),
            deadline: Box::new(self.deadline),
        }
    }

    /// Type-erases retry budget using [`Arc`].
    pub fn arc(self) -> ArcRetrySettings
    where
        S: 'static,
        D: 'static,
    {
        RetrySettings {
            strategy: Arc::new(self.strategy),
            deadline: Arc::new(self.deadline),
        }
    }

    /// Returns a retry strategy that borrows
    /// the current one.
    pub fn as_ref(&self) -> RetrySettings<&'_ S, &'_ D> {
        RetrySettings {
            strategy: &self.strategy,
            deadline: &self.deadline,
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
    async fn attempt<F: AsyncFnMut<RefWithLifetime<RetryState>>>(
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
        F: AsyncFnMut<RefWithLifetime<RetryState>, Output = ControlFlow<B, C>>,
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
        F: AsyncFnMut<RefWithLifetime<RetryState>, Output = Result<T, E>>,
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
            ControlFlow::Continue(err) | ControlFlow::Break(Err(err)) => Err(err),
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
#[async_trait]
pub trait RetryStrategy: Send + Sync {
    /// Returns a future that waits before the next retry.
    ///
    /// Note that the future can be created before the time it's polled.
    ///
    /// Its output tells whether to continue retries.
    async fn wait_retry(&self, retry: &RetryState) -> ControlFlow<()>;
}

/// Retry strategy that never asks to stop.
///
/// This trait should be implemented for retry strategies
/// that always returns [`ControlFlow::Continue`]. It also
/// implies that output of its [`RetryWait::wait_retry`]
/// can be ignored.
pub trait RetryAlways: RetryStrategy {}

/// Retry strategy that doesn't allow retries.
#[derive(Debug, Clone, Copy)]
pub struct DontRetry;

#[async_trait]
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
}

#[async_trait]
impl RetryStrategy for ExponentialBackoff {
    async fn wait_retry(&self, retry: &RetryState) -> ControlFlow<()> {
        tokio::time::sleep(self.wait_duration(retry.attempt)).await;
        ControlFlow::Continue(())
    }
}

impl RetryAlways for ExponentialBackoff {}

#[async_trait]
impl<S: RetryStrategy + ?Sized> RetryStrategy for &S {
    async fn wait_retry(&self, retry: &RetryState) -> ControlFlow<()> {
        S::wait_retry(*self, retry).await
    }
}

#[async_trait]
impl<S: RetryStrategy + ?Sized> RetryStrategy for Box<S> {
    async fn wait_retry(&self, retry: &RetryState) -> ControlFlow<()> {
        S::wait_retry(&self, retry).await
    }
}

#[async_trait]
impl<S: RetryStrategy + ?Sized> RetryStrategy for Arc<S> {
    async fn wait_retry(&self, retry: &RetryState) -> ControlFlow<()> {
        S::wait_retry(&self, retry).await
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

#[async_trait]
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

#[async_trait]
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
#[async_trait]
pub trait RetryDeadline: Send + Sync {
    /// Returns a future that waits for the retry deadline.
    ///
    /// It can be called once per retry loop or each time
    /// and should behave correctly in both cases.
    ///
    /// When it completes, retries should be stopped.
    async fn wait_deadline(&self);
}

/// Retry deadline that is never exceeded.
#[derive(Debug, Clone, Copy)]
pub struct NoDeadline;

#[async_trait]
impl RetryDeadline for NoDeadline {
    async fn wait_deadline(&self) {
        future::pending().await
    }
}

#[async_trait]
impl RetryDeadline for Duration {
    async fn wait_deadline(&self) {
        tokio::time::sleep_until((Instant::now() + *self).into()).await
    }
}

#[async_trait]
impl RetryDeadline for Instant {
    async fn wait_deadline(&self) {
        tokio::time::sleep_until((*self).into()).await
    }
}

#[async_trait]
impl RetryDeadline for CancellationToken {
    async fn wait_deadline(&self) {
        self.cancelled().await
    }
}

#[async_trait]
impl<D: RetryDeadline> RetryDeadline for Option<D> {
    async fn wait_deadline(&self) {
        match self {
            Some(deadline) => deadline.wait_deadline().await,
            None => future::pending().await,
        }
    }
}

#[async_trait]
impl<D: RetryDeadline + ?Sized> RetryDeadline for &D {
    async fn wait_deadline(&self) {
        D::wait_deadline(*self).await
    }
}

#[async_trait]
impl<D: RetryDeadline + ?Sized> RetryDeadline for Box<D> {
    async fn wait_deadline(&self) {
        D::wait_deadline(&self).await
    }
}

#[async_trait]
impl<D: RetryDeadline + ?Sized> RetryDeadline for Arc<D> {
    async fn wait_deadline(&self) {
        D::wait_deadline(&self).await
    }
}

/// Helper type for combining deadlines and retry strategies.
pub struct Combine<A, B>(A, B);

#[async_trait]
impl<A: RetryStrategy, B: RetryStrategy> RetryStrategy for Combine<A, B> {
    async fn wait_retry(&self, retry: &RetryState) -> ControlFlow<()> {
        let (result, other_future) =
            future::select(self.0.wait_retry(retry), self.1.wait_retry(retry))
                .await
                .into_inner();

        result?;
        other_future.await
    }
}

#[async_trait]
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

    struct ConstantBackoff(Duration);

    #[async_trait]
    impl RetryStrategy for ConstantBackoff {
        async fn wait_retry(&self, _retry: &RetryState) -> ControlFlow<()> {
            tokio::time::sleep(self.0).await;
            ControlFlow::Continue(())
        }
    }

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

    #[async_trait]
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
        let first_trap = WaitTrap::new();
        let last_trap = WaitTrap::new();
        let retry_settings = RetrySettings::new(&first_trap)
            .with(DontRetry)
            .with(&last_trap);

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
