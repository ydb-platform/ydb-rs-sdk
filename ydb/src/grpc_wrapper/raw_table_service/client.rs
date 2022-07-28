use crate::grpc_wrapper::channel::ChannelWithAuth;
use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_table_service::create_session::{
    RawCreateSessionRequest, RawCreateSessionResult,
};
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use tracing::{instrument, trace};
use ydb_grpc::ydb_proto::table::v1::table_service_client::TableServiceClient;

pub(crate) struct RawTableClient {
    operation_timeout: std::time::Duration,
    service: TableServiceClient<ChannelWithAuth>,
}

impl RawTableClient {
    pub fn new(service: ChannelWithAuth, operation_timeout: std::time::Duration) -> Self {
        return Self {
            service: TableServiceClient::new(service),
            operation_timeout,
        };
    }

    pub async fn create_session(&mut self) -> RawResult<RawCreateSessionResult> {
        let req = RawCreateSessionRequest {
            operation_params: RawOperationParams::new_with_timeout(self.operation_timeout),
        };

        request_with_result!(
            self.service.create_session,
            req => ydb_grpc::ydb_proto::table::CreateSessionRequest,
            ydb_grpc::ydb_proto::table::CreateSessionResult => RawCreateSessionResult
        );
    }
}
