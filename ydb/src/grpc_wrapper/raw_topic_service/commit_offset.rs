use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use ydb_grpc::ydb_proto::operations::OperationParams;
use ydb_grpc::ydb_proto::topic::CommitOffsetRequest;

#[derive(serde::Serialize)]
pub(crate) struct RawCommitOffsetRequest {
    pub operation_params: RawOperationParams,
    pub path: String,
    pub partition_id: i64,
    pub consumer: String,
    pub offset: i64,
    /// Read session identifier from a StreamRead RPC.
    ///
    /// Empty means "not tied to a read session", which makes the server
    /// interrupt any active read session for the partition.
    pub read_session_id: String,
}

impl From<RawCommitOffsetRequest> for CommitOffsetRequest {
    fn from(value: RawCommitOffsetRequest) -> Self {
        Self {
            operation_params: Some(OperationParams::from(value.operation_params)),
            path: value.path,
            partition_id: value.partition_id,
            consumer: value.consumer,
            offset: value.offset,
            read_session_id: value.read_session_id,
        }
    }
}
