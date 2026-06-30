use crate::client::TimeoutSettings;
use crate::grpc_wrapper::grpc_limits::WithGrpcMaxMessageSize;
use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::raw_table_service::alter_table::RawAlterTableRequest;
use crate::grpc_wrapper::raw_table_service::bulk_upsert::RawBulkUpsertRequest;
use crate::grpc_wrapper::raw_table_service::commit_transaction::{
    RawCommitTransactionRequest, RawCommitTransactionResult,
};
use crate::grpc_wrapper::raw_table_service::copy_table::{
    RawCopyTableRequest, RawCopyTablesRequest,
};
use crate::grpc_wrapper::raw_table_service::create_session::{
    RawCreateSessionRequest, RawCreateSessionResult,
};
use crate::grpc_wrapper::raw_table_service::create_table::RawCreateTableRequest;
use crate::grpc_wrapper::raw_table_service::drop_table::RawDropTableRequest;
use crate::grpc_wrapper::raw_table_service::describe_table::{
    RawDescribeTableRequest, RawDescribeTableResult,
};
use crate::grpc_wrapper::raw_table_service::describe_table_options::{
    RawDescribeTableOptionsRequest, RawDescribeTableOptionsResult,
};
use crate::grpc_wrapper::raw_table_service::execute_data_query::{
    RawExecuteDataQueryRequest, RawExecuteDataQueryResult,
};
use crate::grpc_wrapper::raw_table_service::execute_scheme_query::RawExecuteSchemeQueryRequest;
use crate::grpc_wrapper::raw_table_service::explain_data_query::{
    RawExplainDataQueryRequest, RawExplainDataQueryResult,
};
use crate::grpc_wrapper::raw_table_service::prepare_data_query::{
    RawPrepareDataQueryRequest, RawPrepareDataQueryResult,
};
use crate::grpc_wrapper::raw_table_service::read_rows::{RawReadRowsRequest, RawReadRowsResponse};
use crate::grpc_wrapper::raw_table_service::rollback_transaction::RawRollbackTransactionRequest;
use crate::grpc_wrapper::raw_table_service::stream_read_table::RawStreamReadTableRequest;
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;
use tracing::trace;
use ydb_grpc::ydb_proto::table::v1::table_service_client::TableServiceClient;

pub(crate) struct RawTableClient {
    timeouts: TimeoutSettings,
    service: TableServiceClient<InterceptedChannel>,
}

impl WithGrpcMaxMessageSize for RawTableClient {
    fn with_grpc_max_message_size(mut self, bytes: usize) -> Self {
        self.service = self.service.with_grpc_max_message_size(bytes);
        self
    }
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

    pub async fn execute_data_query(
        &mut self,
        req: RawExecuteDataQueryRequest,
    ) -> RawResult<RawExecuteDataQueryResult> {
        request_with_result!(
            self.service.execute_data_query,
            req => ydb_grpc::ydb_proto::table::ExecuteDataQueryRequest,
            ydb_grpc::ydb_proto::table::ExecuteQueryResult => RawExecuteDataQueryResult
        );
    }

    pub async fn explain_data_query(
        &mut self,
        req: RawExplainDataQueryRequest,
    ) -> RawResult<RawExplainDataQueryResult> {
        request_with_result!(
            self.service.explain_data_query,
            req => ydb_grpc::ydb_proto::table::ExplainDataQueryRequest,
            ydb_grpc::ydb_proto::table::ExplainQueryResult => RawExplainDataQueryResult
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

    pub async fn read_rows(&mut self, req: RawReadRowsRequest) -> RawResult<RawReadRowsResponse> {
        request_with_result!(
            self.service.read_rows,
            req => ydb_grpc::ydb_proto::table::ReadRowsRequest,
            ydb_grpc::ydb_proto::table::ReadRowsResponse => RawReadRowsResponse
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

    pub async fn copy_table(&mut self, req: RawCopyTableRequest) -> RawResult<()> {
        request_without_result!(
            self.service.copy_table,
            req => ydb_grpc::ydb_proto::table::CopyTableRequest
        );
    }

    pub async fn copy_tables(&mut self, req: RawCopyTablesRequest) -> RawResult<()> {
        request_without_result!(
            self.service.copy_tables,
            req => ydb_grpc::ydb_proto::table::CopyTablesRequest
        );
    }

    pub async fn bulk_upsert(&mut self, req: RawBulkUpsertRequest) -> RawResult<()> {
        request_without_result!(
            self.service.bulk_upsert,
            req => ydb_grpc::ydb_proto::table::BulkUpsertRequest
        );
    }

    pub async fn describe_table(
        &mut self,
        req: RawDescribeTableRequest,
    ) -> RawResult<RawDescribeTableResult> {
        request_with_result!(
            self.service.describe_table,
            req => ydb_grpc::ydb_proto::table::DescribeTableRequest,
            ydb_grpc::ydb_proto::table::DescribeTableResult => RawDescribeTableResult
        );
    }

    pub async fn create_table(&mut self, req: RawCreateTableRequest) -> RawResult<()> {
        request_without_result!(
            self.service.create_table,
            req => ydb_grpc::ydb_proto::table::CreateTableRequest
        );
    }

    pub async fn drop_table(&mut self, req: RawDropTableRequest) -> RawResult<()> {
        request_without_result!(
            self.service.drop_table,
            req => ydb_grpc::ydb_proto::table::DropTableRequest
        );
    }

    pub async fn alter_table(&mut self, req: RawAlterTableRequest) -> RawResult<()> {
        request_without_result!(
            self.service.alter_table,
            req => ydb_grpc::ydb_proto::table::AlterTableRequest
        );
    }

    pub async fn prepare_data_query(
        &mut self,
        req: RawPrepareDataQueryRequest,
    ) -> RawResult<RawPrepareDataQueryResult> {
        request_with_result!(
            self.service.prepare_data_query,
            req => ydb_grpc::ydb_proto::table::PrepareDataQueryRequest,
            ydb_grpc::ydb_proto::table::PrepareQueryResult => RawPrepareDataQueryResult
        );
    }

    pub async fn describe_table_options(
        &mut self,
        req: RawDescribeTableOptionsRequest,
    ) -> RawResult<RawDescribeTableOptionsResult> {
        request_with_result!(
            self.service.describe_table_options,
            req => ydb_grpc::ydb_proto::table::DescribeTableOptionsRequest,
            ydb_grpc::ydb_proto::table::DescribeTableOptionsResult => RawDescribeTableOptionsResult
        );
    }

    pub async fn stream_read_table(
        &mut self,
        req: RawStreamReadTableRequest,
    ) -> RawResult<tonic::Streaming<ydb_grpc::ydb_proto::table::ReadTableResponse>> {
        let grpc_req = ydb_grpc::ydb_proto::table::ReadTableRequest::from(req);
        trace!(
            "stream_read_table request: {}",
            crate::trace_helpers::ensure_len_string(
                serde_json::to_string(&grpc_req).unwrap_or_else(|_| "bad json".into())
            )
        );
        Ok(self.service.stream_read_table(grpc_req).await?.into_inner())
    }
}

impl GrpcServiceForDiscovery for RawTableClient {
    fn get_grpc_discovery_service() -> Service {
        Service::Table
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
