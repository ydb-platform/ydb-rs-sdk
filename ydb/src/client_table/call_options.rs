use std::time::Duration;

use crate::client::TimeoutSettings;
use crate::errors::Idempotency;

#[derive(Clone, Debug, Default)]
pub(crate) struct TableCallOptions {
    pub timeout: Option<Duration>,
    pub idempotency: Option<Idempotency>,
}

pub(crate) fn resolve_timeouts(opts: &TableCallOptions) -> TimeoutSettings {
    TimeoutSettings {
        operation_timeout: opts.timeout,
    }
}
