use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use ydb_grpc::ydb_proto::table::DropTableRequest;

pub(crate) struct RawDropTableRequest {
    pub session_id: String,
    pub path: String,
    pub operation_params: RawOperationParams,
}

impl From<RawDropTableRequest> for DropTableRequest {
    fn from(value: RawDropTableRequest) -> Self {
        Self {
            session_id: value.session_id,
            path: value.path,
            operation_params: Some(value.operation_params.into()),
        }
    }
}
