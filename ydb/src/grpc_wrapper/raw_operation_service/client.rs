use tracing::instrument;

use crate::grpc_wrapper::grpc_limits::WithGrpcMaxMessageSize;
use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::grpc_wrapper::raw_operation_service::status::check_status;
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;

use super::types::{RawListOperationsRequest, RawListOperationsResult, RawOperation};

use ydb_grpc::ydb_proto::operation::v1::operation_service_client::OperationServiceClient;
use ydb_grpc::ydb_proto::operations::{
    CancelOperationRequest, ForgetOperationRequest, GetOperationRequest, ListOperationsRequest,
};

pub(crate) struct RawOperationClient {
    service: OperationServiceClient<InterceptedChannel>,
}

impl WithGrpcMaxMessageSize for RawOperationClient {
    fn with_grpc_max_message_size(mut self, bytes: usize) -> Self {
        self.service = self
            .service
            .max_decoding_message_size(bytes)
            .max_encoding_message_size(bytes);
        self
    }
}

impl GrpcServiceForDiscovery for RawOperationClient {
    fn get_grpc_discovery_service() -> Service {
        Service::Operation
    }
}

impl RawOperationClient {
    pub fn new(service: InterceptedChannel) -> Self {
        Self {
            service: OperationServiceClient::new(service),
        }
    }

    #[instrument(name = "ydb.grpc.GetOperation", skip_all, fields(ydb.operation.id = %id), err)]
    pub async fn get_operation(&mut self, id: &str) -> RawResult<RawOperation> {
        let response = self
            .service
            .get_operation(GetOperationRequest { id: id.to_string() })
            .await?;
        let inner = response.into_inner();
        let operation = inner
            .operation
            .ok_or_else(|| RawError::custom("get operation response has no operation field"))?;
        Ok(RawOperation::from(operation))
    }

    #[instrument(name = "ydb.grpc.ListOperations", skip_all, err)]
    pub async fn list_operations(
        &mut self,
        req: RawListOperationsRequest,
    ) -> RawResult<RawListOperationsResult> {
        let response = self
            .service
            .list_operations(ListOperationsRequest {
                kind: req.kind,
                page_size: req.page_size,
                page_token: req.page_token,
            })
            .await?;
        let inner = response.into_inner();
        check_status(inner.status, &inner.issues)?;
        let operations = inner
            .operations
            .into_iter()
            .map(RawOperation::from)
            .collect();
        Ok(RawListOperationsResult {
            operations,
            next_page_token: inner.next_page_token,
        })
    }

    #[instrument(name = "ydb.grpc.ForgetOperation", skip_all, fields(ydb.operation.id = %id), err)]
    pub async fn forget_operation(&mut self, id: &str) -> RawResult<()> {
        let response = self
            .service
            .forget_operation(ForgetOperationRequest { id: id.to_string() })
            .await?;
        let inner = response.into_inner();
        check_status(inner.status, &inner.issues)
    }

    #[instrument(name = "ydb.grpc.CancelOperation", skip_all, fields(ydb.operation.id = %id), err)]
    pub async fn cancel_operation(&mut self, id: &str) -> RawResult<()> {
        let response = self
            .service
            .cancel_operation(CancelOperationRequest { id: id.to_string() })
            .await?;
        let inner = response.into_inner();
        check_status(inner.status, &inner.issues)
    }
}
