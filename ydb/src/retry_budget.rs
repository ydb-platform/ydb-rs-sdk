//! Driver-wide retry budget (rate limiter for client-side retries).
//!
//! A [`RetryBudget`] is shared by all service clients created from the same [`crate::Client`]
//! (or a child from [`crate::Client::clone_with_retry_budget`]). It is consulted on the second and
//! each subsequent retry attempt: when the budget is exhausted the retrier waits until a slot
//! appears or the call deadline expires.

use std::ops::ControlFlow;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use rand::Rng;
use tokio::sync::{Mutex as AsyncMutex, mpsc, watch};
use tokio::time::sleep;

use crate::retry_strategy::{
    ArcRetryBudget, RetryAlways, RetryBudget, RetryDeadline, RetryState, RetryStrategy,
    RetryStrategyExt,
};

/// Fixed maximum number of retry attempts per second (token bucket).
///
/// Pass `0` to deny all retries (every [`acquire`](RetryBudget::acquire) waits until deadline,
/// then returns [`RetryBudgetError::Closed`]).
#[derive(Debug)]
pub struct LimitedRetryBudget {
    quota: AsyncMutex<mpsc::Receiver<()>>,
    _shutdown: Arc<LimitedRetryBudgetShutdown>,
}

#[derive(Debug)]
struct LimitedRetryBudgetShutdown {
    done: watch::Sender<()>,
}

impl Drop for LimitedRetryBudgetShutdown {
    fn drop(&mut self) {
        let _ = self.done.send(());
    }
}

impl LimitedRetryBudget {
    pub fn new(attempts_per_second: u32) -> Self {
        let (done_tx, done_rx) = watch::channel(());
        let shutdown = Arc::new(LimitedRetryBudgetShutdown { done: done_tx });

        if attempts_per_second == 0 {
            let (tx, rx) = mpsc::channel(1);
            drop(tx);
            return Self {
                quota: AsyncMutex::new(rx),
                _shutdown: shutdown,
            };
        }

        let capacity = attempts_per_second as usize;
        let (tx, rx) = mpsc::channel(capacity);
        for _ in 0..attempts_per_second {
            let _ = tx.try_send(());
        }

        let tx_refill = tx.clone();
        let mut done_rx = done_rx;
        let interval = Duration::from_secs(1) / attempts_per_second;
        tokio::spawn(async move {
            let start = tokio::time::Instant::now() + interval;
            let mut ticker = tokio::time::interval_at(start, interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let _ = tx_refill.try_send(());
                    }
                    changed = done_rx.changed() => {
                        if changed.is_ok() {
                            break;
                        }
                    }
                }
            }
        });

        Self {
            quota: AsyncMutex::new(rx),
            _shutdown: shutdown,
        }
    }
}

impl RetryStrategy for LimitedRetryBudget {
    async fn wait_retry<'a>(&'a self, _retry: &'a RetryState) -> ControlFlow<()> {
        let mut quota = self.quota.lock().await;
        match quota.recv().await {
            Some(()) => ControlFlow::Continue(()),
            None => ControlFlow::Break(()),
        }
    }
}

/// Sliding-window counters used by [`PercentOfRpsRetryBudget`].
#[derive(Debug, Default)]
pub struct RetryMetrics {
    inner: Mutex<RetryMetricsInner>,
}

#[derive(Debug)]
struct RetryMetricsInner {
    window_start: Instant,
    total_ops: u64,
    retry_ops: u64,
}

impl Default for RetryMetricsInner {
    fn default() -> Self {
        Self {
            window_start: Instant::now(),
            total_ops: 0,
            retry_ops: 0,
        }
    }
}

impl RetryMetrics {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RetryStrategy for RetryMetrics {
    async fn before_attempt<'a>(&'a self, retry: &'a RetryState) {
        let mut metrics = self.lock_inner();

        metrics.record_attempt();

        if retry.attempt != 0 {
            metrics.record_retry();
        }
    }
}

impl RetryMetricsInner {
    pub(crate) fn record_attempt(&mut self) {
        self.maybe_roll_window();
        self.total_ops += 1;
    }

    pub(crate) fn record_retry(&mut self) {
        self.maybe_roll_window();
        self.retry_ops += 1;
    }

    fn maybe_roll_window(&mut self) {
        if self.window_start.elapsed() >= Duration::from_secs(1) {
            self.window_start = Instant::now();
            self.total_ops = 0;
            self.retry_ops = 0;
        }
    }
}

impl RetryMetrics {
    fn lock_inner(&self) -> MutexGuard<'_, RetryMetricsInner> {
        self.inner.lock().unwrap_or_else(|mut poison_err| {
            **poison_err.get_mut() = RetryMetricsInner::default();
            self.inner.clear_poison();
            poison_err.into_inner()
        })
    }

    fn try_acquire_retry_slot(&self, percent: u32) -> bool {
        let mut metrics = self.lock_inner();

        metrics.maybe_roll_window();

        let max_retries = (metrics.total_ops as u128 * percent as u128 / 100) as u64;

        metrics.retry_ops < max_retries.max(1)
    }
}

/// Retry budget as a percentage of the driver's request rate (operations per second).
///
/// [`RetryMetrics`] must be shared with the driver — use [`crate::Client::retry_metrics`] when
/// constructing this budget, or obtain metrics from an existing child client.
#[derive(Debug, Clone)]
pub struct PercentOfRpsRetryBudget {
    percent: u32,
    metrics: Arc<RetryMetrics>,
}

impl PercentOfRpsRetryBudget {
    pub fn new(percent: u32, metrics: Arc<RetryMetrics>) -> Self {
        assert!(
            percent <= 100,
            "percent must be between 0 and 100, got {percent}"
        );
        Self { percent, metrics }
    }
}

impl RetryStrategy for PercentOfRpsRetryBudget {
    async fn wait_retry<'a>(&'a self, _retry: &'a RetryState) -> ControlFlow<()> {
        loop {
            if self.metrics.try_acquire_retry_slot(self.percent) {
                return ControlFlow::Continue(());
            }
            sleep(Duration::from_millis(1)).await
        }
    }
}

impl RetryAlways for PercentOfRpsRetryBudget {}

/// Probabilistic retry budget (aligned with ydb-go-sdk `budget.Percent`).
///
/// Each retry attempt is allowed with probability `percent / 100`. When denied, the retrier
/// waits and tries again until the call deadline.
#[derive(Debug, Clone)]
pub struct PercentRetryBudget {
    percent: u32,
}

impl PercentRetryBudget {
    pub fn new(percent: u32) -> Self {
        assert!(
            percent <= 100,
            "percent must be between 0 and 100, got {percent}"
        );
        Self { percent }
    }
}

impl RetryStrategy for PercentRetryBudget {
    async fn wait_retry<'a>(&'a self, _retry: &'a RetryState) -> ControlFlow<()> {
        loop {
            if rand::thread_rng().gen_range(0..100) < self.percent {
                return ControlFlow::Continue(());
            }
            sleep(Duration::from_millis(1)).await
        }
    }
}

impl RetryAlways for PercentRetryBudget {}

/// Driver-local retry budget and RPS counters shared by all retriers on one [`crate::Client`].
#[derive(Clone)]
pub(crate) struct RetryControl {
    budget: ArcRetryBudget,
    metrics: Arc<RetryMetrics>,
}

impl Default for RetryControl {
    fn default() -> Self {
        Self::new(RetryBudget::default().arc())
    }
}

impl RetryControl {
    pub(crate) fn new(budget: ArcRetryBudget) -> Self {
        Self {
            budget,
            metrics: Arc::new(RetryMetrics::new()),
        }
    }

    pub(crate) fn with_shared_metrics(budget: ArcRetryBudget, metrics: Arc<RetryMetrics>) -> Self {
        Self { budget, metrics }
    }

    pub(crate) fn budget(&self) -> RetryBudget<impl RetryStrategy + '_, impl RetryDeadline + '_> {
        self.budget
            .as_ref()
            .and_then(self.metrics.as_ref().as_ref_strategy())
    }

    pub(crate) fn metrics(&self) -> &Arc<RetryMetrics> {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn limited_budget_respects_rate() {
        let budget = LimitedRetryBudget::new(1);
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
        let budget = LimitedRetryBudget::new(0);
        let result = tokio::time::timeout(
            Duration::from_millis(20),
            budget.wait_retry(&RetryState::init()),
        )
        .await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_break());
    }

    #[test]
    fn percent_of_rps_respects_share() {
        let metrics = Arc::new(RetryMetrics::new());
        for _ in 0..10 {
            metrics.lock_inner().record_attempt();
        }
        let budget = PercentOfRpsRetryBudget::new(50, metrics.clone());
        assert!(metrics.try_acquire_retry_slot(50));
        assert!(metrics.try_acquire_retry_slot(50));
        assert!(metrics.try_acquire_retry_slot(50));
        assert!(metrics.try_acquire_retry_slot(50));
        assert!(metrics.try_acquire_retry_slot(50));
        assert!(!metrics.try_acquire_retry_slot(50));
        let _ = budget;
    }
}
