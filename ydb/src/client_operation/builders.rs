use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::time::Duration;

use tokio::time::timeout;

use crate::errors::{NeedRetry, YdbError, YdbResult};
use crate::grpc_wrapper::raw_operation_service::types::{
    RawListOperationsResult, RawOperation,
};

use super::client::{retry_wait, OperationClient};
use super::types::{ListOperationsRequest, ListOperationsResult, OperationInfo};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Debug, Default)]
pub(crate) struct OperationCallOptions {
    pub timeout: Option<Duration>,
}

macro_rules! impl_operation_call_builder {
    ($name:ident) => {
        impl<'a> $name<'a> {
            pub fn timeout(mut self, timeout: Duration) -> Self {
                self.opts.timeout = Some(timeout);
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
    let limit = opts.timeout.unwrap_or(Duration::ZERO);
    let start = std::time::Instant::now();
    let mut attempt = 0usize;
    let run = async {
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
                    match retry_wait(attempt, start.elapsed(), limit) {
                        Some(wait) if wait > Duration::ZERO => tokio::time::sleep(wait).await,
                        Some(_) => {}
                        None => return Err(err),
                    }
                }
            }
        }
    };
    match opts.timeout {
        Some(duration) => match timeout(duration, run).await {
            Ok(result) => result,
            Err(_) => Err(YdbError::Transport(format!(
                "operation service rpc timed out after {duration:?}"
            ))),
        },
        None => run.await,
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
