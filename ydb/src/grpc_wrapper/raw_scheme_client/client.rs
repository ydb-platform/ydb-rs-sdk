use crate::grpc_wrapper::channel::ChannelWithAuth;
use crate::grpc_wrapper::grpc::{grpc_read_operation_result, grpc_read_void_operation_result};
use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_scheme_client::list_directory_types::{
    RawListDirectoryRequest, RawListDirectoryResult,
};
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;

use tracing::{instrument, trace};
use ydb_grpc::ydb_proto::operations::OperationParams;
use ydb_grpc::ydb_proto::scheme::v1::scheme_service_client::SchemeServiceClient;
use ydb_grpc::ydb_proto::scheme::{MakeDirectoryRequest, RemoveDirectoryRequest};

pub(crate) struct SchemeClient {
    service: SchemeServiceClient<ChannelWithAuth>,
}

impl SchemeClient {
    pub fn new(service: ChannelWithAuth) -> Self {
        Self {
            service: SchemeServiceClient::new(service),
        }
    }

    #[instrument(skip(self), err, ret)]
    pub async fn list_directory(
        &mut self,
        req: RawListDirectoryRequest,
    ) -> RawResult<RawListDirectoryResult> {
        let req = ydb_grpc::ydb_proto::scheme::ListDirectoryRequest::from(req);
        trace!(
            "list directory request: {}",
            serde_json::to_string(&req).unwrap_or("bad json".into())
        );

        let response = self.service.list_directory(req).await?;
        let result: ydb_grpc::ydb_proto::scheme::ListDirectoryResult =
            grpc_read_operation_result(response)?;

        trace!(
            "list directory result: {}",
            serde_json::to_string(&result).unwrap_or("bad json".into())
        );

        RawListDirectoryResult::try_from(result)
    }

    #[instrument(skip(self), err, ret)]
    pub async fn make_directory(&mut self, req: RawMakeDirectoryRequest) -> RawResult<()> {
        let req = MakeDirectoryRequest::from(req);
        trace!(
            "make directory request: {}",
            serde_json::to_string(&req).unwrap_or("bad json".into())
        );

        let response = self.service.make_directory(req).await?;
        grpc_read_void_operation_result(response)
    }

    #[instrument(skip(self), err, ret)]
    pub async fn remove_directory(&mut self, req: RawRemoveDirectoryRequest) -> RawResult<()> {
        let req = RemoveDirectoryRequest::from(req);
        trace!(
            "remove directory request: {}",
            serde_json::to_string(&req).unwrap_or("bad json".into())
        );

        let response = self.service.remove_directory(req).await?;
        grpc_read_void_operation_result(response)
    }
}

impl GrpcServiceForDiscovery for SchemeClient {
    fn get_grpc_discovery_service() -> Service {
        Service::Scheme
    }
}

#[derive(Debug)]
pub(crate) struct RawMakeDirectoryRequest {
    pub operation_params: RawOperationParams,
    pub path: String,
}

impl From<RawMakeDirectoryRequest> for MakeDirectoryRequest {
    fn from(value: RawMakeDirectoryRequest) -> Self {
        Self {
            operation_params: Some(OperationParams::from(value.operation_params)),
            path: value.path,
        }
    }
}

#[derive(Debug)]
pub(crate) struct RawRemoveDirectoryRequest {
    pub operation_params: RawOperationParams,
    pub path: String,
}

impl From<RawRemoveDirectoryRequest> for RemoveDirectoryRequest {
    fn from(value: RawRemoveDirectoryRequest) -> Self {
        Self {
            operation_params: Some(OperationParams::from(value.operation_params)),
            path: value.path,
        }
    }
}
