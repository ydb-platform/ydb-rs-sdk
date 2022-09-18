use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;

pub(crate) struct RawRollbackTransactionRequest {
    pub session_id: String,
    pub tx_id: String,
    pub operation_params: RawOperationParams,
}

impl From<RawRollbackTransactionRequest>
    for ydb_grpc::ydb_proto::table::RollbackTransactionRequest
{
    fn from(value: RawRollbackTransactionRequest) -> Self {
        Self {
            session_id: value.session_id,
            tx_id: value.tx_id,
            operation_params: Some(value.operation_params.into()),
        }
    }
}
