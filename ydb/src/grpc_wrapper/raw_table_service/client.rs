use crate::client::TimeoutSettings;
use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::raw_table_service::create_session::{
    RawCreateSessionRequest, RawCreateSessionResult,
};
use crate::grpc_wrapper::raw_table_service::keepalive::{RawKeepAliveRequest, RawKeepAliveResult};
use crate::grpc_wrapper::raw_table_service::rollback_transaction::RawRollbackTransactionRequest;
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;
use tracing::trace;
use ydb_grpc::ydb_proto::table::v1::table_service_client::TableServiceClient;

pub(crate) struct RawTableClient {
    timeouts: TimeoutSettings,
    service: TableServiceClient<InterceptedChannel>,
}

impl RawTableClient {
    pub fn new(service: InterceptedChannel) -> Self {
        Self {
            service: TableServiceClient::new(service),
            timeouts: TimeoutSettings::default(),
        }
    }

    pub fn with_timeout(mut self, timeouts: TimeoutSettings) -> Self {
        self.timeouts = timeouts;
        self
    }

    pub async fn create_session(&mut self) -> RawResult<RawCreateSessionResult> {
        let req = RawCreateSessionRequest {
            operation_params: self.timeouts.operation_params(),
        };

        request_with_result!(
            self.service.create_session,
            req => ydb_grpc::ydb_proto::table::CreateSessionRequest,
            ydb_grpc::ydb_proto::table::CreateSessionResult => RawCreateSessionResult
        );
    }

    pub async fn keep_alive(&mut self, req: RawKeepAliveRequest) -> RawResult<RawKeepAliveResult> {
        request_with_result!(
            self.service.keep_alive,
            req => ydb_grpc::ydb_proto::table::KeepAliveRequest,
            ydb_grpc::ydb_proto::table::KeepAliveResult => RawKeepAliveResult
        );
    }

    pub async fn rollback_transaction(
        &mut self,
        req: RawRollbackTransactionRequest,
    ) -> RawResult<()> {
        request_without_result!(
            self.service.rollback_transaction,
            req => ydb_grpc::ydb_proto::table::RollbackTransactionRequest
        );
    }
}

impl GrpcServiceForDiscovery for RawTableClient {
    fn get_grpc_discovery_service() -> Service {
        Service::Table
    }
}

#[derive(Debug)]
pub(crate) enum SessionStatus {
    Ready,
    Busy,
    Unknown(i32),
}

impl From<i32> for SessionStatus {
    fn from(value: i32) -> Self {
        use ydb_grpc::ydb_proto::table::keep_alive_result;

        match keep_alive_result::SessionStatus::from_i32(value) {
            Some(keep_alive_result::SessionStatus::Ready) => SessionStatus::Ready,
            Some(keep_alive_result::SessionStatus::Busy) => SessionStatus::Busy,
            Some(keep_alive_result::SessionStatus::Unspecified) | None => {
                SessionStatus::Unknown(value)
            }
        }
    }
}
