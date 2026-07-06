use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::client::TimeoutSettings;
use crate::errors::{NeedRetry, YdbResult};
use crate::retry::{IndefiniteRetrier, Retry, RetryParams, TimeoutRetrier};
use crate::retry_budget::{acquire_retry_budget, RetryControl, RetryPauseError};

#[derive(Clone, Debug, Default)]
pub(crate) struct TableCallOptions {
    pub timeout: Option<Duration>,
    pub idempotent: Option<bool>,
}

pub(crate) fn resolve_idempotent(opts: &TableCallOptions, default: bool) -> bool {
    opts.idempotent.unwrap_or(default)
}

pub(crate) fn resolve_timeouts(opts: &TableCallOptions) -> TimeoutSettings {
    TimeoutSettings {
        operation_timeout: opts.timeout,
    }
}

pub(crate) async fn retry_table_operation<CallbackFuture, CallbackResult>(
    retry_control: &RetryControl,
    opts: &TableCallOptions,
    idempotent: bool,
    callback: impl Fn() -> CallbackFuture,
) -> YdbResult<CallbackResult>
where
    CallbackFuture: Future<Output = YdbResult<CallbackResult>>,
{
    let retrier: Arc<dyn Retry> = match opts.timeout {
        None => Arc::new(IndefiniteRetrier {}),
        Some(timeout) => Arc::new(TimeoutRetrier { timeout }),
    };
    let mut attempt: usize = 0;
    let start = Instant::now();
    loop {
        retry_control.metrics().record_attempt();
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
        match acquire_retry_budget(retry_control, start, opts.timeout).await {
            Ok(()) => {}
            Err(RetryPauseError::Timeout) | Err(RetryPauseError::Budget(_)) => {
                return Err(last_err);
            }
        }
    }
}
