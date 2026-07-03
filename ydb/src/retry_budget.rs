//! Driver-wide retry budget (rate limiter for client-side retries).
//!
//! A [`RetryBudget`] is shared by all service clients created from the same [`crate::Client`]
//! (or a child from [`crate::Client::clone_with_retry_budget`]). It is consulted on the second and
//! each subsequent retry attempt: when the budget is exhausted the retrier waits until a slot
//! appears or the call deadline expires.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rand::Rng;
use tokio::sync::{mpsc, watch, Mutex as AsyncMutex};
use tokio::time::sleep;

const INITIAL_RETRY_BACKOFF_MILLISECONDS: u64 = 1;
const MAX_RETRY_BACKOFF_MILLISECONDS: u64 = 1_000;

/// Error returned when [`RetryBudget::acquire`] cannot grant a retry slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryBudgetError {
    /// The call deadline expired while waiting for budget quota.
    Exhausted,
    /// The budget was stopped or misconfigured (for example [`LimitedRetryBudget::new`](LimitedRetryBudget::new)(0)).
    Closed,
}

impl std::fmt::Display for RetryBudgetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RetryBudgetError::Exhausted => write!(f, "retry budget exhausted"),
            RetryBudgetError::Closed => write!(f, "retry budget closed"),
        }
    }
}

impl std::error::Error for RetryBudgetError {}

/// Limits how many client-side retries may proceed across all SDK retriers on one driver.
#[async_trait]
pub trait RetryBudget: Send + Sync {
    /// Reserve quota for the next retry attempt.
    ///
    /// Called before the second and each subsequent retry. When `deadline` is `Some`, the call
    /// must return [`RetryBudgetError::Exhausted`] if quota is not available before that instant.
    async fn acquire(&self, deadline: Option<Instant>) -> Result<(), RetryBudgetError>;
}

/// No-op budget used when the driver has no explicit retry budget.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct UnlimitedRetryBudget;

#[async_trait]
impl RetryBudget for UnlimitedRetryBudget {
    async fn acquire(&self, _deadline: Option<Instant>) -> Result<(), RetryBudgetError> {
        Ok(())
    }
}

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

#[async_trait]
impl RetryBudget for LimitedRetryBudget {
    async fn acquire(&self, deadline: Option<Instant>) -> Result<(), RetryBudgetError> {
        match deadline {
            None => {
                let mut quota = self.quota.lock().await;
                match quota.recv().await {
                    Some(()) => Ok(()),
                    None => Err(RetryBudgetError::Closed),
                }
            }
            Some(deadline) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(RetryBudgetError::Exhausted);
                }
                let remaining = deadline.saturating_duration_since(now);
                let recv = async {
                    let mut quota = self.quota.lock().await;
                    quota.recv().await
                };
                tokio::select! {
                    token = recv => match token {
                        Some(()) => Ok(()),
                        None => Err(RetryBudgetError::Closed),
                    },
                    _ = sleep(remaining) => Err(RetryBudgetError::Exhausted),
                }
            }
        }
    }
}

/// Sliding-window counters used by [`PercentOfRpsRetryBudget`].
#[derive(Debug)]
pub struct RetryMetrics {
    window_start: Mutex<Instant>,
    total_ops: AtomicU64,
    retry_ops: AtomicU64,
}

impl RetryMetrics {
    pub fn new() -> Self {
        Self {
            window_start: Mutex::new(Instant::now()),
            total_ops: AtomicU64::new(0),
            retry_ops: AtomicU64::new(0),
        }
    }
}

impl Default for RetryMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl RetryMetrics {
    pub(crate) fn record_attempt(&self) {
        self.maybe_roll_window();
        self.total_ops.fetch_add(1, Ordering::Relaxed);
    }

    fn maybe_roll_window(&self) {
        let mut start = self.window_start.lock().expect("retry metrics lock");
        if start.elapsed() >= Duration::from_secs(1) {
            *start = Instant::now();
            self.total_ops.store(0, Ordering::Relaxed);
            self.retry_ops.store(0, Ordering::Relaxed);
        }
    }

    fn try_acquire_retry_slot(&self, percent: u32) -> bool {
        self.maybe_roll_window();
        let total = self.total_ops.load(Ordering::Relaxed);
        if total == 0 {
            self.retry_ops.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        let max_retries = (total as u128 * percent as u128 / 100) as u64;
        let retries = self.retry_ops.load(Ordering::Relaxed);
        if retries < max_retries.max(1) {
            self.retry_ops.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
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

#[async_trait]
impl RetryBudget for PercentOfRpsRetryBudget {
    async fn acquire(&self, deadline: Option<Instant>) -> Result<(), RetryBudgetError> {
        loop {
            if self.metrics.try_acquire_retry_slot(self.percent) {
                return Ok(());
            }
            match deadline {
                None => sleep(Duration::from_millis(1)).await,
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(RetryBudgetError::Exhausted);
                    }
                    let remaining = deadline.saturating_duration_since(now);
                    sleep(remaining.min(Duration::from_millis(10))).await;
                }
            }
        }
    }
}

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

#[async_trait]
impl RetryBudget for PercentRetryBudget {
    async fn acquire(&self, deadline: Option<Instant>) -> Result<(), RetryBudgetError> {
        loop {
            if rand::thread_rng().gen_range(0..100) < self.percent {
                return Ok(());
            }
            match deadline {
                None => sleep(Duration::from_millis(1)).await,
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(RetryBudgetError::Exhausted);
                    }
                    let remaining = deadline.saturating_duration_since(now);
                    sleep(remaining.min(Duration::from_millis(10))).await;
                }
            }
        }
    }
}

/// Driver-local retry budget and RPS counters shared by all retriers on one [`crate::Client`].
#[derive(Clone)]
pub(crate) struct RetryControl {
    budget: Arc<dyn RetryBudget>,
    metrics: Arc<RetryMetrics>,
}

impl Default for RetryControl {
    fn default() -> Self {
        Self::new(Arc::new(UnlimitedRetryBudget))
    }
}

impl RetryControl {
    pub(crate) fn new(budget: Arc<dyn RetryBudget>) -> Self {
        Self {
            budget,
            metrics: Arc::new(RetryMetrics::new()),
        }
    }

    pub(crate) fn with_shared_metrics(
        budget: Arc<dyn RetryBudget>,
        metrics: Arc<RetryMetrics>,
    ) -> Self {
        Self { budget, metrics }
    }

    pub(crate) fn budget(&self) -> Arc<dyn RetryBudget> {
        self.budget.clone()
    }

    pub(crate) fn metrics(&self) -> Arc<RetryMetrics> {
        self.metrics.clone()
    }
}

#[derive(Debug)]
pub(crate) enum RetryPauseError {
    Timeout,
    #[allow(dead_code)]
    Budget(RetryBudgetError),
}

/// Sleep duration before the next retry attempt, or `None` when a timeout limit is exhausted.
///
/// `limit: None` — retry indefinitely (only non-retryable errors stop the loop).
pub(crate) fn retry_wait(
    attempt: usize,
    time_from_start: Duration,
    limit: Option<Duration>,
) -> Option<Duration> {
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
    match limit {
        None => Some(wait),
        Some(budget) if time_from_start >= budget => None,
        Some(budget) if time_from_start + wait < budget => Some(wait),
        Some(_) => None,
    }
}

pub(crate) async fn acquire_retry_budget(
    control: &RetryControl,
    start: Instant,
    wall_limit: Option<Duration>,
) -> Result<(), RetryPauseError> {
    let deadline = wall_limit.map(|limit| start + limit);
    control
        .budget()
        .acquire(deadline)
        .await
        .map_err(RetryPauseError::Budget)?;
    if wall_limit.is_some_and(|limit| start.elapsed() >= limit) {
        return Err(RetryPauseError::Timeout);
    }
    Ok(())
}

pub(crate) async fn pause_before_retry(
    control: &RetryControl,
    attempt: usize,
    start: Instant,
    wall_limit: Option<Duration>,
) -> Result<(), RetryPauseError> {
    let wait = match retry_wait(attempt, start.elapsed(), wall_limit) {
        Some(wait) => wait,
        None => return Err(RetryPauseError::Timeout),
    };
    if wait > Duration::ZERO {
        sleep(wait).await;
    }

    let deadline = wall_limit.map(|limit| start + limit);
    control
        .budget()
        .acquire(deadline)
        .await
        .map_err(RetryPauseError::Budget)?;

    if wall_limit.is_some_and(|limit| start.elapsed() >= limit) {
        return Err(RetryPauseError::Timeout);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unlimited_budget_always_acquires() {
        let budget = UnlimitedRetryBudget;
        budget.acquire(None).await.unwrap();
    }

    #[tokio::test]
    async fn limited_budget_respects_rate() {
        let budget = LimitedRetryBudget::new(1);
        budget.acquire(None).await.unwrap();
        let second = tokio::time::timeout(Duration::from_millis(50), budget.acquire(None)).await;
        assert!(second.is_err());
    }

    #[tokio::test]
    async fn limited_zero_denies_retries() {
        let budget = LimitedRetryBudget::new(0);
        let result = tokio::time::timeout(
            Duration::from_millis(20),
            budget.acquire(Some(Instant::now() + Duration::from_millis(10))),
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Err(RetryBudgetError::Closed));
    }

    #[test]
    fn retry_wait_helpers() {
        assert!(retry_wait(1, Duration::ZERO, None).is_some());
        let budget = Duration::from_millis(100);
        let wait1 = retry_wait(1, Duration::ZERO, Some(budget)).expect("wait");
        assert!(!wait1.is_zero());
        assert!(retry_wait(10, budget, Some(budget)).is_none());
    }

    #[test]
    fn percent_of_rps_respects_share() {
        let metrics = Arc::new(RetryMetrics::new());
        for _ in 0..10 {
            metrics.record_attempt();
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
