use std::future::IntoFuture;
use std::time::Duration;

use futures_util::future::BoxFuture;

use crate::async_closure::AsyncFnMut;
use crate::async_closure::with_lifetime::Ref;
use crate::errors::{Idempotency, YdbResult};
use crate::grpc_wrapper::raw_operation_service::types::{RawListOperationsResult, RawOperation};
use crate::retry_budget::RetryControl;
use crate::retry_strategy::RetryState;

use super::client::OperationClient;
use super::types::{ListOperationsRequest, ListOperationsResult, OperationInfo};

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

pub(crate) async fn retry_operation_call<T, F>(
    retry_control: &RetryControl,
    opts: &OperationCallOptions,
    attempt_fn: F,
) -> YdbResult<T>
where
    F: AsyncFnMut<Ref<RetryState>, Output = YdbResult<T>>,
{
    retry_control
        .budget()
        .deadline(opts.timeout)
        .retry_on_retriable_errors(Idempotency::Idempotent, attempt_fn)
        .await
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
