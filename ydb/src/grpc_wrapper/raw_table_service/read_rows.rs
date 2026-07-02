use ydb_grpc::ydb_proto::{
    status_ids::StatusCode,
    table::{ReadRowsRequest, ReadRowsResponse},
};

use crate::{
    grpc::proto_issues_to_ydb_issues,
    grpc_wrapper::{
        raw_errors::RawError,
        raw_table_service::value::{RawResultSet, RawTypedValue},
    },
    YdbIssue,
};

#[derive(Clone)]
pub(crate) struct RawReadRowsRequest {
    pub session_id: String,
    pub path: String,
    pub keys: RawTypedValue,
    pub columns: Vec<String>,
}

impl From<RawReadRowsRequest> for ReadRowsRequest {
    fn from(value: RawReadRowsRequest) -> Self {
        Self {
            session_id: value.session_id,
            path: value.path,
            keys: Some(value.keys.into()),
            columns: value.columns,
        }
    }
}

pub(crate) struct RawReadRowsResponse {
    pub status: StatusCode,
    pub issues: Vec<YdbIssue>,
    pub result_set: RawResultSet,
}

impl TryFrom<ReadRowsResponse> for RawReadRowsResponse {
    type Error = RawError;

    fn try_from(value: ReadRowsResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            status: value.status(),
            issues: proto_issues_to_ydb_issues(value.issues),
            result_set: value.result_set.unwrap_or_default().try_into()?,
        })
    }
}
