use crate::grpc_wrapper::channel::ChannelWithAuth;
use crate::grpc_wrapper::grpc::grpc_read_operation_result;
use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_scheme_client::list_directory_types::{
    RawListDirectoryRequest, RawListDirectoryResult,
};
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::raw_ydb_operation::OperationParams;
use crate::YdbResult;
use tracing::{instrument, trace};
use ydb_grpc::ydb_proto::scheme::v1::scheme_service_client::SchemeServiceClient;

pub(crate) struct SchemeClient {
    service: SchemeServiceClient<ChannelWithAuth>,
}

impl SchemeClient {
    pub fn new(service: ChannelWithAuth) -> Self {
        return Self {
            service: SchemeServiceClient::new(service),
        };
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

        return RawListDirectoryResult::try_from(result);
    }
}

impl GrpcServiceForDiscovery for SchemeClient {
    fn get_grpc_discovery_service() -> Service {
        return Service::Scheme;
    }
}
