use std::time::Duration;

use crate::client::TimeoutSettings;

#[derive(Clone, Debug, Default)]
pub(crate) struct TableCallOptions {
    pub timeout: Option<Duration>,
    pub idempotent: Option<bool>,
}

pub(crate) fn resolve_timeouts(opts: &TableCallOptions) -> TimeoutSettings {
    TimeoutSettings {
        operation_timeout: opts.timeout,
    }
}
