use std::time::Duration;
use tracing::instrument;

const DEFAULT_RETRY_TIMEOUT: Duration = Duration::from_secs(5);
const BACKOFF_RETRY_MAX_WAIT_DURATION: Duration = Duration::from_secs(10);
const BACKOFF_RETRY_MAX_WAIT_DURATION_MILLISECONDS: u64 =
    BACKOFF_RETRY_MAX_WAIT_DURATION.as_millis() as u64;

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

fn exponential_backoff_retry_wait_duration(attempt: usize) -> Duration {
    if attempt == 0 {
        return Duration::default();
    }

    let duration_milliseconds = 2u64
        .pow(attempt as u32)
        .min(BACKOFF_RETRY_MAX_WAIT_DURATION_MILLISECONDS);
    Duration::from_millis(duration_milliseconds)
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
            res.wait_timeout = exponential_backoff_retry_wait_duration(params.attempt);
            res.allow_retry = (params.time_from_start + res.wait_timeout) < self.timeout;
        };

        res
    }
}

pub(crate) struct IndefiniteRetrier {}

impl Retry for IndefiniteRetrier {
    #[instrument(skip_all)]
    fn wait_duration(&self, params: RetryParams) -> RetryDecision {
        RetryDecision {
            allow_retry: true,
            wait_timeout: exponential_backoff_retry_wait_duration(params.attempt),
        }
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

#[cfg(test)]
mod tests {
    use crate::{types_converters::try_vec_to_list_of_structs, ydb_struct};

    #[test]
    fn try_vec_empty() {
        assert!(matches!(try_vec_to_list_of_structs(vec![]), Ok(None)));
    }

    #[test]
    fn try_vec_same_structure() {
        let values = vec![ydb_struct!("id" => 1), ydb_struct!("id" => 2)];
        assert!(matches!(try_vec_to_list_of_structs(values), Ok(Some(_))));
    }

    #[test]
    fn try_vec_different_structure() {
        let values = vec![ydb_struct!("id" => 1), ydb_struct!("key" => 2)];
        assert!(try_vec_to_list_of_structs(values).is_err());
    }

    #[test]
    fn try_vec_non_struct() {
        let values = vec![ydb_struct!("id" => 1), 42i64.into()];
        assert!(try_vec_to_list_of_structs(values).is_err());

        let values = vec![1i64.into()];
        assert!(try_vec_to_list_of_structs(values).is_err());
    }
}
