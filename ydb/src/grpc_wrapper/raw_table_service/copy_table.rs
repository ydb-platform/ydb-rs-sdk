use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;

pub(crate) struct RawCopyTableRequest {
    pub session_id: String,
    pub source_path: String,
    pub destination_path: String,
    pub operation_params: RawOperationParams,
}

impl From<RawCopyTableRequest>
    for ydb_grpc::ydb_proto::table::CopyTableRequest
{
    fn from(value: RawCopyTableRequest) -> Self {
        Self {
            session_id: value.session_id,
            source_path: value.source_path,
            destination_path: value.destination_path,
            operation_params: Some(value.operation_params.into()),
        }
    }
}
