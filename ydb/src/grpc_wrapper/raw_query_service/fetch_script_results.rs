use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_query_service::status::check_status;
use crate::grpc_wrapper::raw_table_service::value::RawResultSet;
use ydb_grpc::ydb_proto::query::{FetchScriptResultsRequest, FetchScriptResultsResponse};

#[derive(Clone, Debug)]
pub(crate) struct RawFetchScriptResultsRequest {
    pub operation_id: String,
    pub result_set_index: i64,
    pub fetch_token: String,
    pub rows_limit: i64,
}

impl RawFetchScriptResultsRequest {
    pub(crate) fn into_proto(self) -> FetchScriptResultsRequest {
        FetchScriptResultsRequest {
            operation_id: self.operation_id,
            result_set_index: self.result_set_index,
            fetch_token: self.fetch_token,
            rows_limit: self.rows_limit,
        }
    }
}

pub(crate) fn parse_response(
    response: FetchScriptResultsResponse,
) -> RawResult<(i64, RawResultSet, String)> {
    check_status(response.status, &response.issues)?;
    let result_set = match response.result_set {
        Some(proto) => RawResultSet::try_from(proto)?,
        None => RawResultSet::default(),
    };
    Ok((
        response.result_set_index,
        result_set,
        response.next_fetch_token,
    ))
}
