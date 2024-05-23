use ydb_grpc::ydb_proto::auth::v1::auth_service_client::AuthServiceClient;

use crate::grpc_wrapper::raw_auth_service::login::RawLoginResult;
use crate::grpc_wrapper::raw_auth_service::login::RawLoginRequest;
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;
use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};

use tracing::trace;

#[derive(Clone)]
pub(crate) struct RawAuthClient {
    service: AuthServiceClient<InterceptedChannel>,
}

impl RawAuthClient {
    pub fn new(service: InterceptedChannel) -> Self {
        Self {
            service: AuthServiceClient::new(service),
        }
    }

    pub async fn login(&mut self, req: RawLoginRequest) -> RawResult<RawLoginResult> {
        request_with_hidden_result!(
            self.service.login,
            req => ydb_grpc::ydb_proto::auth::LoginRequest,
            ydb_grpc::ydb_proto::auth::LoginResult => RawLoginResult
        );
    }
}

impl GrpcServiceForDiscovery for RawAuthClient {
    fn get_grpc_discovery_service() -> Service {
        Service::Auth
    }
}
