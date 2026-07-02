use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::time::Duration;

use tokio::time::timeout;

use crate::errors::{NeedRetry, YdbError, YdbResult};
use crate::grpc_wrapper::raw_operation_service::types::{
    RawListOperationsResult, RawOperation,
};

use super::client::{retry_wait, OperationClient, DEFAULT_RETRY_BUDGET};
use super::types::{ListOperationsRequest, ListOperationsResult, OperationInfo};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Debug, Default)]
pub(crate) struct OperationCallOptions {
    pub timeout: Option<Duration>,
    pub retry_budget: Option<Duration>,
    pub no_retry: bool,
}

pub(crate) fn resolve_operation_timeout(opts: &OperationCallOptions) -> Duration {
    opts.timeout
        .unwrap_or_else(|| crate::client::TimeoutSettings::default().operation_timeout)
}

pub(crate) fn resolve_operation_retry_budget(opts: &OperationCallOptions) -> Duration {
    if opts.no_retry {
        Duration::ZERO
    } else {
        opts.retry_budget.unwrap_or(DEFAULT_RETRY_BUDGET)
    }
}

macro_rules! impl_operation_call_builder {
    ($name:ident) => {
        impl<'a> $name<'a> {
            pub fn timeout(mut self, timeout: Duration) -> Self {
                self.opts.timeout = Some(timeout);
                self
            }

            pub fn retry_budget(mut self, budget: Duration) -> Self {
                self.opts.retry_budget = Some(budget);
                self
            }

            pub fn no_retry(mut self) -> Self {
                self.opts.no_retry = true;
                self
            }
        }
    };
}

pub struct GetOperationBuilder<'a> {
    pub(crate) client: &'a OperationClient,
    pub(crate) id: String,
    pub(crate) opts: OperationCallOptions,
}

impl<'a> IntoFuture for GetOperationBuilder<'a> {
    type Output = YdbResult<OperationInfo>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.client.get_operation_call(self.id, self.opts))
    }
}

pub struct ListOperationsBuilder<'a> {
    pub(crate) client: &'a OperationClient,
    pub(crate) request: ListOperationsRequest,
    pub(crate) opts: OperationCallOptions,
}

impl<'a> IntoFuture for ListOperationsBuilder<'a> {
    type Output = YdbResult<ListOperationsResult>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.client.list_operations_call(self.request, self.opts))
    }
}

pub struct ForgetOperationBuilder<'a> {
    pub(crate) client: &'a OperationClient,
    pub(crate) id: String,
    pub(crate) opts: OperationCallOptions,
}

impl<'a> IntoFuture for ForgetOperationBuilder<'a> {
    type Output = YdbResult<()>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.client.forget_operation_call(self.id, self.opts))
    }
}

pub struct CancelOperationBuilder<'a> {
    pub(crate) client: &'a OperationClient,
    pub(crate) id: String,
    pub(crate) opts: OperationCallOptions,
}

impl<'a> IntoFuture for CancelOperationBuilder<'a> {
    type Output = YdbResult<()>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.client.cancel_operation_call(self.id, self.opts))
    }
}

impl_operation_call_builder!(GetOperationBuilder);
impl_operation_call_builder!(ListOperationsBuilder);
impl_operation_call_builder!(ForgetOperationBuilder);
impl_operation_call_builder!(CancelOperationBuilder);

pub(crate) async fn retry_operation_call<T, F, Fut>(
    opts: &OperationCallOptions,
    mut attempt_fn: F,
) -> YdbResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = YdbResult<T>>,
{
    let retry_budget = resolve_operation_retry_budget(opts);
    let start = std::time::Instant::now();
    let mut attempt = 0usize;
    loop {
        attempt += 1;
        match attempt_fn().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if !matches!(
                    err.need_retry(),
                    NeedRetry::True | NeedRetry::IdempotentOnly
                ) {
                    return Err(err);
                }
                match retry_wait(attempt, start.elapsed(), retry_budget) {
                    Some(wait) if wait > Duration::ZERO => tokio::time::sleep(wait).await,
                    Some(_) => {}
                    None => return Err(err),
                }
            }
        }
    }
}

pub(crate) async fn with_rpc_timeout<T, F, Fut>(
    opts: &OperationCallOptions,
    operation: F,
) -> YdbResult<T>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = YdbResult<T>>,
{
    let timeout_duration = resolve_operation_timeout(opts);
    match timeout(timeout_duration, operation()).await {
        Ok(result) => result,
        Err(_) => Err(YdbError::Transport(format!(
            "operation service rpc timed out after {timeout_duration:?}"
        ))),
    }
}

pub(crate) fn raw_to_operation_info(raw: RawOperation) -> OperationInfo {
    OperationInfo {
        id: raw.id,
        ready: raw.ready,
        status: raw.status,
        issues: raw.issues,
        consumed_units: raw.consumed_units,
    }
}

pub(crate) fn raw_to_list_result(raw: RawListOperationsResult) -> ListOperationsResult {
    ListOperationsResult {
        operations: raw
            .operations
            .into_iter()
            .map(raw_to_operation_info)
            .collect(),
        next_page_token: raw.next_page_token,
    }
}
