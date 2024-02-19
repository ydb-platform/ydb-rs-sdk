use crate::client::TimeoutSettings;
use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::raw_table_service::commit_transaction::{
    RawCommitTransactionRequest, RawCommitTransactionResult,
};
use crate::grpc_wrapper::raw_table_service::create_session::{
    RawCreateSessionRequest, RawCreateSessionResult,
};
use crate::grpc_wrapper::raw_table_service::execute_scheme_query::RawExecuteSchemeQueryRequest;
use crate::grpc_wrapper::raw_table_service::keepalive::{RawKeepAliveRequest, RawKeepAliveResult};
use crate::grpc_wrapper::raw_table_service::rollback_transaction::RawRollbackTransactionRequest;
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;
use tracing::trace;
use ydb_grpc::ydb_proto::table::v1::table_service_client::TableServiceClient;
use crate::grpc_wrapper::raw_table_service::copy_table::{RawCopyTableRequest, RawCopyTablesRequest};
use crate::grpc_wrapper::raw_table_service::execute_data_query::{RawExecuteDataQueryRequest, RawExecuteDataQueryResult};

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

    pub async fn commit_transaction(
        &mut self,
        req: RawCommitTransactionRequest,
    ) -> RawResult<RawCommitTransactionResult> {
        request_with_result!(
            self.service.commit_transaction,
            req => ydb_grpc::ydb_proto::table::CommitTransactionRequest,
            ydb_grpc::ydb_proto::table::CommitTransactionResult => crate::grpc_wrapper::raw_table_service::commit_transaction::RawCommitTransactionResult
        );
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

    pub async fn execute_data_query(&mut self, req: RawExecuteDataQueryRequest)->RawResult<RawExecuteDataQueryResult>{
        request_with_result!(
            self.service.execute_data_query,
            req => ydb_grpc::ydb_proto::table::ExecuteDataQueryRequest,
            ydb_grpc::ydb_proto::table::ExecuteQueryResult => RawExecuteDataQueryResult
        );
    }

    pub async fn execute_scheme_query(
        &mut self,
        req: RawExecuteSchemeQueryRequest,
    ) -> RawResult<()> {
        request_without_result!(
            self.service.execute_scheme_query,
            req => ydb_grpc::ydb_proto::table::ExecuteSchemeQueryRequest
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

    pub async fn copy_table(
        &mut self,
        req: RawCopyTableRequest,
    ) -> RawResult<()> {
        request_without_result!(
            self.service.copy_table,
            req => ydb_grpc::ydb_proto::table::CopyTableRequest
        );
    }

    pub async fn copy_tables(
        &mut self,
        req: RawCopyTablesRequest,
    ) -> RawResult<()> {
        request_without_result!(
            self.service.copy_tables,
            req => ydb_grpc::ydb_proto::table::CopyTablesRequest
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
    Unspecified,
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
            Some(keep_alive_result::SessionStatus::Unspecified) => SessionStatus::Unspecified,
            None => SessionStatus::Unknown(value),
        }
    }
}

#[derive(Copy, Clone)]
pub(crate) enum CollectStatsMode {
    Unspecified,
    None,
    Basic,
    Full,
}

impl From<CollectStatsMode> for ydb_grpc::ydb_proto::table::query_stats_collection::Mode {
    fn from(value: CollectStatsMode) -> Self {
        use ydb_grpc::ydb_proto::table::query_stats_collection::Mode;
        match value {
            CollectStatsMode::Unspecified => Mode::StatsCollectionUnspecified,
            CollectStatsMode::None => Mode::StatsCollectionNone,
            CollectStatsMode::Basic => Mode::StatsCollectionBasic,
            CollectStatsMode::Full => Mode::StatsCollectionFull,
        }
    }
}

impl From<CollectStatsMode> for i32 {
    fn from(value: CollectStatsMode) -> Self {
        let grpc_val = ydb_grpc::ydb_proto::table::query_stats_collection::Mode::from(value);
        grpc_val as i32
    }
}

#[derive(Debug)]
pub(crate) struct RawQueryStats {
    process_cpu_time: std::time::Duration,
    query_plan: String,
    query_ast: String,
    total_duration: std::time::Duration,
    total_cpu_time: std::time::Duration,
}

impl From<ydb_grpc::ydb_proto::table_stats::QueryStats> for RawQueryStats {
    fn from(value: ydb_grpc::ydb_proto::table_stats::QueryStats) -> Self {
        Self {
            process_cpu_time: std::time::Duration::from_micros(value.process_cpu_time_us),
            query_plan: value.query_plan,
            query_ast: value.query_ast,
            total_duration: std::time::Duration::from_micros(value.total_duration_us),
            total_cpu_time: std::time::Duration::from_micros(value.total_cpu_time_us),
        }
    }
}
