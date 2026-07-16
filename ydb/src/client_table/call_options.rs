use std::time::Duration;

use crate::async_closure::AsyncFnMut;
use crate::async_closure::with_lifetime::Ref;
use crate::client::TimeoutSettings;
use crate::errors::{Idempotency, YdbResult};
use crate::retry_budget::RetryControl;
use crate::retry_strategy::RetryState;

use tracing::instrument;

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

#[instrument(name = "ydb.TableClient.RetryOperation", skip_all, fields(db.system.name = "ydb"), err)]
pub(crate) async fn retry_table_operation<F, T>(
    retry_control: &RetryControl,
    opts: &TableCallOptions,
    idempotent: bool,
    callback: F,
) -> YdbResult<T>
where
    F: AsyncFnMut<Ref<RetryState>, Output = YdbResult<T>>,
{
    retry_control
        .budget()
        .deadline(opts.timeout)
        .retry_on_retriable_errors(Idempotency::from(idempotent), callback)
        .await
}
