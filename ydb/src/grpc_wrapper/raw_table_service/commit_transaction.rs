use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_table_service::client::{CollectStatsMode, RawQueryStats};
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;

pub(crate) struct RawCommitTransactionRequest {
    pub session_id: String,
    pub tx_id: String,
    pub operation_params: RawOperationParams,
    pub collect_stats: CollectStatsMode,
}

impl From<RawCommitTransactionRequest> for ydb_grpc::ydb_proto::table::CommitTransactionRequest {
    fn from(value: RawCommitTransactionRequest) -> Self {
        Self {
            session_id: value.session_id,
            tx_id: value.tx_id,
            operation_params: Some(value.operation_params.into()),
            collect_stats: value.collect_stats.into(),
        }
    }
}

pub(crate) struct RawCommitTransactionResult {
    pub query_stats: Option<RawQueryStats>,
}

impl TryFrom<ydb_grpc::ydb_proto::table::CommitTransactionResult> for RawCommitTransactionResult {
    type Error = RawError;

    fn try_from(
        value: ydb_grpc::ydb_proto::table::CommitTransactionResult,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            query_stats: value.query_stats.map(Into::into),
        })
    }
}
