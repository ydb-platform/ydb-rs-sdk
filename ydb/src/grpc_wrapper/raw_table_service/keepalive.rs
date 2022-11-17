use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_table_service::client::SessionStatus;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use ydb_grpc::ydb_proto::operations::OperationParams;

pub(crate) struct RawKeepAliveRequest {
    pub operation_params: RawOperationParams,
    pub session_id: String,
}

impl From<RawKeepAliveRequest> for ydb_grpc::ydb_proto::table::KeepAliveRequest {
    fn from(r: RawKeepAliveRequest) -> Self {
        Self {
            session_id: r.session_id,
            operation_params: Some(OperationParams::from(r.operation_params)),
        }
    }
}

#[derive(Debug)]
pub(crate) struct RawKeepAliveResult {
    pub session_status: SessionStatus,
}

impl TryFrom<ydb_grpc::ydb_proto::table::KeepAliveResult> for RawKeepAliveResult {
    type Error = RawError;

    fn try_from(value: ydb_grpc::ydb_proto::table::KeepAliveResult) -> Result<Self, Self::Error> {
        Ok(Self {
            session_status: SessionStatus::from(value.session_status),
        })
    }
}
