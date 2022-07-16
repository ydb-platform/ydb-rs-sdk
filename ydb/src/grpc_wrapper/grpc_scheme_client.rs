use crate::grpc_wrapper::channel::ChannelWithAuth;
use crate::grpc_wrapper::grpc_services::{GrpcServiceForDiscovery, Service};
use ydb_grpc::ydb_proto::scheme::v1::scheme_service_client::SchemeServiceClient;

pub(crate) struct SchemeClient {
    service: SchemeServiceClient<ChannelWithAuth>,
}

impl SchemeClient {}

impl GrpcServiceForDiscovery for SchemeClient {
    fn get_grpc_discovery_service() -> Service {
        return Service::Scheme;
    }
}

pub(crate) struct ListDirectoryRequest {}
