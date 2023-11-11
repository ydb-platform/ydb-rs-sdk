use ydb_grpc::ydb_proto::coordination::DropNodeRequest;

use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;

#[derive(serde::Serialize)]
pub(crate) struct RawDropNodeRequest {
    pub path: String,
    pub operation_params: RawOperationParams,
}

impl From<RawDropNodeRequest> for DropNodeRequest {
    fn from(value: RawDropNodeRequest) -> Self {
        Self {
            path: value.path,
            operation_params: Some(value.operation_params.into()),
        }
    }
}
