use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::client::TimeoutSettings;
use crate::errors::{NeedRetry, YdbResult};
use crate::retry::{IndefiniteRetrier, Retry, RetryParams, TimeoutRetrier};

#[derive(Clone, Debug, Default)]
pub(crate) struct TableCallOptions {
    pub timeout: Option<Duration>,
}

pub(crate) fn resolve_timeouts(opts: &TableCallOptions) -> TimeoutSettings {
    TimeoutSettings {
        operation_timeout: opts.timeout,
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
    let retrier: Arc<Box<dyn Retry>> = match opts.timeout {
        None => Arc::new(Box::new(IndefiniteRetrier {})),
        Some(timeout) => Arc::new(Box::new(TimeoutRetrier { timeout })),
    };
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
