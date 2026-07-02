use std::time::Duration;

use rand::Rng;
use ydb_grpc::ydb_proto::status_ids::StatusCode;

use crate::errors::{YdbError, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_operation_service::client::RawOperationClient;
use crate::grpc_wrapper::raw_operation_service::types::RawListOperationsRequest;

use super::builders::{
    retry_operation_call, with_rpc_timeout, CancelOperationBuilder, ForgetOperationBuilder,
    GetOperationBuilder, ListOperationsBuilder, OperationCallOptions, raw_to_list_result,
    raw_to_operation_info,
};
use super::types::{ListOperationsRequest, ListOperationsResult, OperationInfo};

const INITIAL_RETRY_BACKOFF_MILLISECONDS: u64 = 1;
const MAX_RETRY_BACKOFF_MILLISECONDS: u64 = 1_000;

#[derive(Clone)]
pub struct OperationClient {
    connection_manager: GrpcConnectionManager,
}

impl OperationClient {
    pub(crate) fn new(connection_manager: GrpcConnectionManager) -> Self {
        Self { connection_manager }
    }

    pub fn get_operation(&self, id: impl Into<String>) -> GetOperationBuilder<'_> {
        GetOperationBuilder {
            client: self,
            id: id.into(),
            opts: OperationCallOptions::default(),
        }
    }

    pub(crate) async fn get_operation_call(
        &self,
        id: String,
        opts: OperationCallOptions,
    ) -> YdbResult<OperationInfo> {
        retry_operation_call(&opts, || async {
            let mut client = self.raw_client().await?;
            let op = with_rpc_timeout(&opts, || async {
                client.get_operation(&id).await.map_err(YdbError::from)
            })
            .await?;
            Ok(raw_to_operation_info(op))
        })
        .await
    }

    pub fn list_operations(
        &self,
        request: ListOperationsRequest,
    ) -> ListOperationsBuilder<'_> {
        ListOperationsBuilder {
            client: self,
            request,
            opts: OperationCallOptions::default(),
        }
    }

    pub(crate) async fn list_operations_call(
        &self,
        request: ListOperationsRequest,
        opts: OperationCallOptions,
    ) -> YdbResult<ListOperationsResult> {
        let raw_req = RawListOperationsRequest {
            kind: request.kind,
            page_size: request.page_size,
            page_token: request.page_token,
        };
        retry_operation_call(&opts, || async {
            let mut client = self.raw_client().await?;
            let result = with_rpc_timeout(&opts, || async {
                client
                    .list_operations(raw_req.clone())
                    .await
                    .map_err(YdbError::from)
            })
            .await?;
            Ok(raw_to_list_result(result))
        })
        .await
    }

    /// Forgets a completed operation on the server.
    ///
    /// If the operation was already forgotten (e.g. a retry after a successful first attempt
    /// that lost the response), `NOT_FOUND` is treated as success.
    pub fn forget_operation(&self, id: impl Into<String>) -> ForgetOperationBuilder<'_> {
        ForgetOperationBuilder {
            client: self,
            id: id.into(),
            opts: OperationCallOptions::default(),
        }
    }

    pub(crate) async fn forget_operation_call(
        &self,
        id: String,
        opts: OperationCallOptions,
    ) -> YdbResult<()> {
        retry_operation_call(&opts, || async {
            let mut client = self.raw_client().await?;
            match with_rpc_timeout(&opts, || async {
                client.forget_operation(&id).await.map_err(YdbError::from)
            })
            .await
            {
                Ok(()) => Ok(()),
                Err(YdbError::YdbStatusError(status))
                    if status.operation_status == StatusCode::NotFound as i32 =>
                {
                    Ok(())
                }
                Err(err) => Err(err),
            }
        })
        .await
    }

    pub fn cancel_operation(&self, id: impl Into<String>) -> CancelOperationBuilder<'_> {
        CancelOperationBuilder {
            client: self,
            id: id.into(),
            opts: OperationCallOptions::default(),
        }
    }

    pub(crate) async fn cancel_operation_call(
        &self,
        id: String,
        opts: OperationCallOptions,
    ) -> YdbResult<()> {
        retry_operation_call(&opts, || async {
            let mut client = self.raw_client().await?;
            with_rpc_timeout(&opts, || async {
                client.cancel_operation(&id).await.map_err(YdbError::from)
            })
            .await?;
            Ok(())
        })
        .await
    }

    async fn raw_client(&self) -> YdbResult<RawOperationClient> {
        self.connection_manager
            .get_auth_service(RawOperationClient::new)
            .await
    }
}

pub(crate) fn retry_wait(
    attempt: usize,
    time_from_start: Duration,
    retry_budget: Duration,
) -> Option<Duration> {
    if time_from_start >= retry_budget {
        return None;
    }
    let wait = if attempt > 0 {
        let exp_shift = (attempt - 1).min(63) as u32;
        let base_ms = INITIAL_RETRY_BACKOFF_MILLISECONDS
            .saturating_mul(1u64 << exp_shift)
            .min(MAX_RETRY_BACKOFF_MILLISECONDS);
        let base = Duration::from_millis(base_ms);
        let half = base / 2;
        if half.is_zero() {
            base
        } else {
            half + Duration::from_millis(rand::thread_rng().gen_range(0..=half.as_millis() as u64))
        }
    } else {
        Duration::ZERO
    };
    if time_from_start + wait < retry_budget {
        Some(wait)
    } else {
        None
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn retry_wait_bounded_by_budget() {
        let budget = Duration::from_millis(100);
        assert!(retry_wait(1, Duration::ZERO, budget).is_some());
        assert!(retry_wait(10, budget, budget).is_none());
    }
}
