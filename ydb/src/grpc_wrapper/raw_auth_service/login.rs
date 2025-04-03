use ydb_grpc::ydb_proto::auth::LoginRequest;
use ydb_grpc::ydb_proto::auth::LoginResult;

use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_errors::RawResult;
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

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct RawLoginResult {
    pub token: String,
}

impl TryFrom<LoginResult> for RawLoginResult {
    type Error = RawError;

    fn try_from(value: LoginResult) -> RawResult<Self> {
        Ok(Self { token: value.token })
    }
}
