use ydb_grpc::ydb_proto::auth::LoginRequest;

use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;

#[derive(serde::Serialize)]
pub(crate) struct RawLoginRequest {
    pub operation_params: RawOperationParams,
    pub user: String,
    pub password: String,
}

impl From<RawLoginRequest> for LoginRequest {
    fn from(value: RawLoginRequest) -> Self {
        Self {
            operation_params: Some(value.operation_params.into()),
            user: value.user,
            password: value.password,
        }
    }
}