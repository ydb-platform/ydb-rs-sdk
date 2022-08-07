use crate::grpc_wrapper::channel::ChannelWithAuth;
use crate::grpc_wrapper::grpc::{grpc_read_operation_result, grpc_read_void_operation_result};
use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_scheme_client::list_directory_types::{
    RawListDirectoryRequest, RawListDirectoryResult,
};
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use crate::YdbResult;
use tracing::{instrument, trace};
use ydb_grpc::ydb_proto::operations::OperationParams;
use ydb_grpc::ydb_proto::scheme::v1::scheme_service_client::SchemeServiceClient;
use ydb_grpc::ydb_proto::scheme::{MakeDirectoryRequest, RemoveDirectoryRequest};

pub(crate) struct RawSchemeClient {
    service: SchemeServiceClient<ChannelWithAuth>,
}

impl RawSchemeClient {
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
        request_with_result!(
            self.service.list_directory,
            req => ydb_grpc::ydb_proto::scheme::ListDirectoryRequest,
            ydb_grpc::ydb_proto::scheme::ListDirectoryResult => RawListDirectoryResult
        );
    }

    #[instrument(skip(self), err, ret)]
    pub async fn make_directory(&mut self, req: RawMakeDirectoryRequest) -> RawResult<()> {
        request_without_result!(
            self.service.make_directory,
            req => ydb_grpc::ydb_proto::scheme::MakeDirectoryRequest
        );
    }

    #[instrument(skip(self), err, ret)]
    pub async fn remove_directory(&mut self, req: RawRemoveDirectoryRequest) -> RawResult<()> {
        request_without_result!(
            self.service.remove_directory,
            req => ydb_grpc::ydb_proto::scheme::RemoveDirectoryRequest
        );
    }
}

impl GrpcServiceForDiscovery for RawSchemeClient {
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
