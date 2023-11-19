use ydb_grpc::ydb_proto::coordination::CreateNodeRequest;

use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;

use super::config::RawCoordinationNodeConfig;

#[derive(serde::Serialize)]
pub(crate) struct RawCreateNodeRequest {
    pub path: String,
    pub operation_params: RawOperationParams,
    pub config: RawCoordinationNodeConfig,
}

impl From<RawCreateNodeRequest> for CreateNodeRequest {
    fn from(value: RawCreateNodeRequest) -> Self {
        Self {
            path: value.path,
            config: Some(value.config.into()),
            operation_params: Some(value.operation_params.into()),
        }
    }
}
