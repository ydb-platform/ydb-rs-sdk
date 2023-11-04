use ydb_grpc::ydb_proto::coordination::AlterNodeRequest;

use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;

use super::common::config::RawCoordinationNodeConfig;

#[derive(serde::Serialize)]
pub(crate) struct RawAlterNodeRequest {
    pub path: String,
    pub config: RawCoordinationNodeConfig,
    pub operation_params: RawOperationParams,
}

impl From<RawAlterNodeRequest> for AlterNodeRequest {
    fn from(value: RawAlterNodeRequest) -> Self {
        Self {
            path: value.path,
            config: Some(value.config.into()),
            operation_params: Some(value.operation_params.into()),
        }
    }
}
