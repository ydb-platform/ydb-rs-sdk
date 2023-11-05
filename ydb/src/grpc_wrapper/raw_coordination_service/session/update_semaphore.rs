use ydb_grpc::ydb_proto::{
    coordination::session_response::UpdateSemaphoreResult, status_ids::StatusCode,
};

use crate::{
    grpc_wrapper::{grpc::proto_issues_to_ydb_issues, raw_errors::RawError},
    YdbStatusError,
};

pub(crate) struct RawUpdateSemaphoreResult {
    pub req_id: u64,
}

impl TryFrom<UpdateSemaphoreResult> for RawUpdateSemaphoreResult {
    type Error = RawError;

    fn try_from(value: UpdateSemaphoreResult) -> Result<Self, Self::Error> {
        if value.status != StatusCode::Success as i32 {
            return Err(RawError::YdbStatus(YdbStatusError {
                message: "".to_string(),
                operation_status: value.status,
                issues: proto_issues_to_ydb_issues(value.issues),
            }));
        }
        Ok(RawUpdateSemaphoreResult {
            req_id: value.req_id,
        })
    }
}
