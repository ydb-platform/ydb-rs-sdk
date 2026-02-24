use num::pow;
use std::time::Duration;
use tracing::instrument;

const DEFAULT_RETRY_TIMEOUT: Duration = Duration::from_secs(5);
const INITIAL_RETRY_BACKOFF_MILLISECONDS: u64 = 1;

#[derive(Debug)]
pub(crate) struct RetryParams {
    pub(crate) attempt: usize,
    pub(crate) time_from_start: Duration,
}

// May be extended in the future
#[derive(Default, Debug)]
pub(crate) struct RetryDecision {
    pub(crate) allow_retry: bool,
    pub(crate) wait_timeout: Duration,
}

pub(crate) trait Retry: Send + Sync {
    fn wait_duration(&self, params: RetryParams) -> RetryDecision;
}

#[derive(Debug)]
pub(crate) struct TimeoutRetrier {
    pub(crate) timeout: Duration,
}

impl Default for TimeoutRetrier {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_RETRY_TIMEOUT,
        }
    }
}

impl Retry for TimeoutRetrier {
    #[instrument(ret)]
    fn wait_duration(&self, params: RetryParams) -> RetryDecision {
        let mut res = RetryDecision::default();
        if params.time_from_start < self.timeout {
            if params.attempt > 0 {
                res.wait_timeout =
                    Duration::from_millis(pow(INITIAL_RETRY_BACKOFF_MILLISECONDS, params.attempt));
            }
            res.allow_retry = (params.time_from_start + res.wait_timeout) < self.timeout;
        };

        res
    }
}

pub(crate) struct NoRetrier {}

impl Retry for NoRetrier {
    #[instrument(skip_all)]
    fn wait_duration(&self, _: RetryParams) -> RetryDecision {
        RetryDecision {
            allow_retry: false,
            wait_timeout: Duration::default(),
        }
    }
}
