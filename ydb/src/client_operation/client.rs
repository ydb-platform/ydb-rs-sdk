use ydb_grpc::ydb_proto::status_ids::StatusCode;

use crate::closure;
use crate::errors::{YdbError, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_operation_service::client::RawOperationClient;
use crate::grpc_wrapper::raw_operation_service::types::RawListOperationsRequest;

use super::builders::{
    CancelOperationBuilder, ForgetOperationBuilder, GetOperationBuilder, ListOperationsBuilder,
    OperationCallOptions, raw_to_list_result, raw_to_operation_info, retry_operation_call,
};
use super::types::{ListOperationsRequest, ListOperationsResult, OperationInfo};
use crate::retry_budget::RetryControl;
use tracing::instrument;

#[derive(Clone)]
pub struct OperationClient {
    connection_manager: GrpcConnectionManager,
    retry_control: std::sync::Arc<RetryControl>,
}

impl OperationClient {
    pub(crate) fn new(
        connection_manager: GrpcConnectionManager,
        retry_control: std::sync::Arc<RetryControl>,
    ) -> Self {
        Self {
            connection_manager,
            retry_control,
        }
    }

    pub fn get_operation(&self, id: impl Into<String>) -> GetOperationBuilder<'_> {
        GetOperationBuilder {
            client: self,
            id: id.into(),
            opts: OperationCallOptions::default(),
        }
    }

    #[instrument(name = "ydb.OperationClient.GetOperation", skip_all, fields(db.system.name = "ydb", ydb.operation.id = %id), err)]
    pub(crate) async fn get_operation_call(
        &self,
        id: String,
        opts: OperationCallOptions,
    ) -> YdbResult<OperationInfo> {
        retry_operation_call(
            &self.retry_control,
            &opts,
            closure!([&client = self, &id], async |_| {
                let mut client = client.raw_client().await?;
                let op = client.get_operation(id).await.map_err(YdbError::from)?;
                Ok(raw_to_operation_info(op))
            }),
        )
        .await
    }

    pub fn list_operations(&self, request: ListOperationsRequest) -> ListOperationsBuilder<'_> {
        ListOperationsBuilder {
            client: self,
            request,
            opts: OperationCallOptions::default(),
        }
    }

    #[instrument(name = "ydb.OperationClient.ListOperations", skip_all, fields(db.system.name = "ydb"), err)]
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
        retry_operation_call(
            &self.retry_control,
            &opts,
            closure!([&client = self, raw_req], async |_| {
                let mut client = client.raw_client().await?;
                let result = client
                    .list_operations(raw_req.clone())
                    .await
                    .map_err(YdbError::from)?;
                Ok(raw_to_list_result(result))
            }),
        )
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

    #[instrument(name = "ydb.OperationClient.ForgetOperation", skip_all, fields(db.system.name = "ydb", ydb.operation.id = %id), err)]
    pub(crate) async fn forget_operation_call(
        &self,
        id: String,
        opts: OperationCallOptions,
    ) -> YdbResult<()> {
        retry_operation_call(
            &self.retry_control,
            &opts,
            closure!([&client = self, &id], async |_| {
                let mut client = client.raw_client().await?;
                match client.forget_operation(id).await.map_err(YdbError::from) {
                    Ok(()) => Ok(()),
                    Err(YdbError::YdbStatusError(status))
                        if status.operation_status == StatusCode::NotFound as i32 =>
                    {
                        Ok(())
                    }
                    Err(err) => Err(err),
                }
            }),
        )
        .await
    }

    pub fn cancel_operation(&self, id: impl Into<String>) -> CancelOperationBuilder<'_> {
        CancelOperationBuilder {
            client: self,
            id: id.into(),
            opts: OperationCallOptions::default(),
        }
    }

    #[instrument(name = "ydb.OperationClient.CancelOperation", skip_all, fields(db.system.name = "ydb", ydb.operation.id = %id), err)]
    pub(crate) async fn cancel_operation_call(
        &self,
        id: String,
        opts: OperationCallOptions,
    ) -> YdbResult<()> {
        retry_operation_call(
            &self.retry_control,
            &opts,
            closure!([&client = self, &id], async |_| {
                let mut client = client.raw_client().await?;
                client.cancel_operation(id).await.map_err(YdbError::from)?;
                Ok(())
            }),
        )
        .await
    }

    async fn raw_client(&self) -> YdbResult<RawOperationClient> {
        self.connection_manager
            .get_auth_service(RawOperationClient::new)
            .await
    }
}
