use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;

#[derive(serde::Serialize)]
pub(crate) struct RawExplainDataQueryRequest {
    pub session_id: String,
    pub yql_text: String,
    pub operation_params: RawOperationParams,
    pub collect_full_diagnostics: bool,
}

impl From<RawExplainDataQueryRequest> for ydb_grpc::ydb_proto::table::ExplainDataQueryRequest {
    fn from(v: RawExplainDataQueryRequest) -> Self {
        Self {
            session_id: v.session_id,
            yql_text: v.yql_text,
            operation_params: Some(v.operation_params.into()),
            collect_full_diagnostics: v.collect_full_diagnostics,
        }
    }
}

#[derive(serde::Serialize)]
pub(crate) struct RawExplainDataQueryResult {
    pub query_ast: String,
    pub query_plan: String,
    pub query_full_diagnostics: String,
}

impl TryFrom<ydb_grpc::ydb_proto::table::ExplainQueryResult> for RawExplainDataQueryResult {
    type Error = RawError;

    fn try_from(
        value: ydb_grpc::ydb_proto::table::ExplainQueryResult,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            query_ast: value.query_ast,
            query_plan: value.query_plan,
            query_full_diagnostics: value.query_full_diagnostics,
        })
    }
}
