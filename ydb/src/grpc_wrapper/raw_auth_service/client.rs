use ydb_grpc::ydb_proto::auth::v1::auth_service_client::AuthServiceClient;

use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;
use crate::grpc_wrapper::raw_errors::RawResult;

pub(crate) struct RawAuthClient {
    service: AuthServiceClient<InterceptedChannel>,
}

impl RawAuthClient {
    pub fn new(service: InterceptedChannel) -> Self {
        Self {
            service: AuthServiceClient::new(service),
        }
    }

    pub async fn login(&mut self, req: RawLoginRequest) -> RawResult<()> {
        request_without_result!(
            self.service.login,
            req => ydb_grpc::ydb_proto::auth::LoginRequest
        );
    }
}

impl GrpcServiceForDiscovery for RawAuthClient {
    fn get_grpc_discovery_service() -> Service {
        Service::Topic
    }
}
