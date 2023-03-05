use ydb_grpc::ydb_proto::operations::OperationParams;
use ydb_grpc::ydb_proto::topic::DropTopicRequest;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;

#[derive(serde::Serialize)]
pub(crate) struct RawDropTopicRequest{
    pub operation_params: RawOperationParams,
    pub path: String
}

impl From<RawDropTopicRequest> for DropTopicRequest{
    fn from(value: RawDropTopicRequest) -> Self {
        Self{
            operation_params: Some(OperationParams::from(value.operation_params)),
            path: value.path
        }
    }
}
