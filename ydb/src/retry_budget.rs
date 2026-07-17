//! Driver-wide retry budget (rate limiter for client-side retries).
//!
//! A [`RetryBudget`] is shared by all service clients created from the same [`crate::Client`]
//! (or a child from [`crate::Client::clone_with_retry_budget`]). It is consulted on the second and
//! each subsequent retry attempt: when the budget is exhausted the retrier waits until a slot
//! appears or the call deadline expires.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use tokio::sync::Semaphore;
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::retry_strategy::{RetryState, RetryStrategy};

/// Fixed maximum number of retry attempts per second (token bucket).
///
/// Pass `0` to deny all retries (every [`acquire`](RetryBudget::acquire) waits until deadline,
/// then returns [`RetryBudgetError::Closed`]).
#[derive(Debug)]
pub struct LimitedRetryBudget {
    semaphore: Arc<Semaphore>,
    _drop_guard: DropGuard,
}

impl LimitedRetryBudget {
    pub fn new(attempts_per_second: u32) -> Self {
        let capacity = attempts_per_second as usize;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(capacity));

        let cancellation = CancellationToken::new();
        let _drop_guard = cancellation.clone().drop_guard();

        if attempts_per_second > 0 {
            let interval = Duration::from_secs(1) / attempts_per_second;
            let semaphore_refill = semaphore.clone();

            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
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
        }

        Self {
            semaphore,
            _drop_guard,
        }
    }
}

impl RetryStrategy for LimitedRetryBudget {
    async fn wait_retry<'a>(&'a self, _retry: &'a RetryState) -> ControlFlow<()> {
        self.semaphore
            .acquire()
            .await
            .expect("semaphore cannot be canceled because is not accessible anywhere else")
            .forget();
        ControlFlow::Continue(())
    }
}

/// Probabilistic retry budget (aligned with ydb-go-sdk `budget.Percent`).
///
/// Each retry attempt is allowed with probability `percent / 100`.
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
        if rand::thread_rng().gen_range(0..100) < self.percent {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
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
}
