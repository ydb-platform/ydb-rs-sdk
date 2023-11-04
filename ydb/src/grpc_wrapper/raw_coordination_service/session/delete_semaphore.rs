use ydb_grpc::ydb_proto::{
    coordination::session_response::DeleteSemaphoreResult, status_ids::StatusCode,
};

use crate::{
    grpc_wrapper::{grpc::proto_issues_to_ydb_issues, raw_errors::RawError},
    YdbStatusError,
};

pub(crate) struct RawDeleteSemaphoreResult {
    pub req_id: u64,
}

impl TryFrom<DeleteSemaphoreResult> for RawDeleteSemaphoreResult {
    type Error = RawError;

    fn try_from(value: DeleteSemaphoreResult) -> Result<Self, Self::Error> {
        if value.status != StatusCode::Success as i32 {
            return Err(RawError::YdbStatus(YdbStatusError {
                message: "".to_string(),
                operation_status: value.status,
                issues: proto_issues_to_ydb_issues(value.issues),
            }));
        }
        Ok(RawDeleteSemaphoreResult {
            req_id: value.req_id,
        })
    }
}
