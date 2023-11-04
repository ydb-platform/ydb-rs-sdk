use ydb_grpc::ydb_proto::{
    coordination::session_response::CreateSemaphoreResult, status_ids::StatusCode,
};

use crate::{
    grpc_wrapper::{grpc::proto_issues_to_ydb_issues, raw_errors::RawError},
    YdbStatusError,
};

pub(crate) struct RawCreateSemaphoreResult {
    pub req_id: u64,
}

impl TryFrom<CreateSemaphoreResult> for RawCreateSemaphoreResult {
    type Error = RawError;

    fn try_from(value: CreateSemaphoreResult) -> Result<Self, Self::Error> {
        if value.status != StatusCode::Success as i32 {
            return Err(RawError::YdbStatus(YdbStatusError {
                message: "".to_string(),
                operation_status: value.status,
                issues: proto_issues_to_ydb_issues(value.issues),
            }));
        }
        Ok(RawCreateSemaphoreResult {
            req_id: value.req_id,
        })
    }
}
