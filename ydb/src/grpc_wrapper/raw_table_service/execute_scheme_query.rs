use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;

pub(crate) struct RawExecuteSchemeQueryRequest {
    pub session_id: String,
    pub yql_text: String,
    pub operation_params: RawOperationParams,
}

impl From<RawExecuteSchemeQueryRequest> for ydb_grpc::ydb_proto::table::ExecuteSchemeQueryRequest {
    fn from(v: RawExecuteSchemeQueryRequest) -> Self {
        Self {
            session_id: v.session_id,
            yql_text: v.yql_text,
            operation_params: Some(v.operation_params.into()),
        }
    }
}
