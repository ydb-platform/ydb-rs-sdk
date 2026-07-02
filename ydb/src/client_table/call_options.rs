use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::client::TimeoutSettings;
use crate::errors::{NeedRetry, YdbResult};
use crate::retry::{Retry, RetryParams, TimeoutRetrier};

pub(crate) const DEFAULT_TABLE_RETRY_BUDGET: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Default)]
pub(crate) struct TableCallOptions {
    pub timeout: Option<Duration>,
    pub retry_budget: Option<Duration>,
    pub no_retry: bool,
}

pub(crate) fn resolve_timeouts(opts: &TableCallOptions) -> TimeoutSettings {
    opts.timeout
        .map(|operation_timeout| TimeoutSettings {
            operation_timeout,
        })
        .unwrap_or_default()
}

pub(crate) fn resolve_retry_budget(opts: &TableCallOptions) -> Duration {
    if opts.no_retry {
        Duration::ZERO
    } else {
        opts.retry_budget.unwrap_or(DEFAULT_TABLE_RETRY_BUDGET)
    }
}

pub(crate) async fn retry_table_operation<CallbackFuture, CallbackResult>(
    opts: &TableCallOptions,
    idempotent: bool,
    callback: impl Fn() -> CallbackFuture,
) -> YdbResult<CallbackResult>
where
    CallbackFuture: Future<Output = YdbResult<CallbackResult>>,
{
    let retry_budget = resolve_retry_budget(opts);
    let retrier: Arc<Box<dyn Retry>> = Arc::new(Box::new(TimeoutRetrier {
        timeout: retry_budget,
    }));
    let mut attempt: usize = 0;
    let start = Instant::now();
    loop {
        attempt += 1;
        let last_err = match callback().await {
            Ok(res) => return Ok(res),
            Err(err) => match (err.need_retry(), idempotent) {
                (NeedRetry::True, _) => err,
                (NeedRetry::IdempotentOnly, true) => err,
                _ => return Err(err),
            },
        };

        let now = Instant::now();
        let retry_decision = retrier.wait_duration(RetryParams {
            attempt,
            time_from_start: now.duration_since(start),
        });
        if !retry_decision.allow_retry {
            return Err(last_err);
        }
        tokio::time::sleep(retry_decision.wait_timeout).await;
    }
}
