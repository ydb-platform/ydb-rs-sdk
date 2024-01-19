use ydb_grpc::ydb_proto::auth::v1::auth_service_client::AuthServiceClient;

use crate::grpc_wrapper::raw_auth_service::login::RawLoginResult;
use crate::grpc_wrapper::raw_auth_service::login::RawLoginRequest;
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;
use crate::grpc_wrapper::raw_errors::RawResult;

use tracing::trace;

#[allow(dead_code)]
pub(crate) struct RawAuthClient {
    service: AuthServiceClient<InterceptedChannel>,
}

impl RawAuthClient {
    #[allow(dead_code)]
    pub fn new(service: InterceptedChannel) -> Self {
        Self {
            service: AuthServiceClient::new(service),
        }
    }

    #[allow(dead_code)]
    pub async fn login(&mut self, req: RawLoginRequest) -> RawResult<RawLoginResult> {
        request_with_result!(
            self.service.login,
            req => ydb_grpc::ydb_proto::auth::LoginRequest,
            ydb_grpc::ydb_proto::auth::LoginResult => RawLoginResult
        );
    }
}
