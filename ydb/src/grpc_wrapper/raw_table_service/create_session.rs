use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use ydb_grpc::ydb_proto::operations::OperationParams;
use ydb_grpc::ydb_proto::table::{CreateSessionRequest, CreateSessionResult};

pub(crate) struct RawCreateSessionRequest {
    pub operation_params: RawOperationParams,
}

impl From<RawCreateSessionRequest> for CreateSessionRequest {
    fn from(r: RawCreateSessionRequest) -> Self {
        Self {
            operation_params: Some(OperationParams::from(r.operation_params)),
        }
    }
}

pub(crate) struct RawCreateSessionResult {
    pub id: String,
}

impl TryFrom<CreateSessionResult> for RawCreateSessionResult {
    type Error = RawError;

    fn try_from(value: CreateSessionResult) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.session_id,
        })
    }
}
