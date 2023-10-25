use ydb_grpc::ydb_proto::{
    coordination::session_response::DescribeSemaphoreResult, status_ids::StatusCode,
};

use crate::{
    grpc_wrapper::{
        grpc::proto_issues_to_ydb_issues,
        raw_coordination_service::common::semaphore::RawSemaphoreDescription, raw_errors::RawError,
    },
    YdbStatusError,
};

pub(crate) struct RawDescribeSemaphoreResult {
    pub req_id: u64,
    pub semaphore_description: RawSemaphoreDescription,
    pub watch_added: bool,
}

impl TryFrom<DescribeSemaphoreResult> for RawDescribeSemaphoreResult {
    type Error = RawError;

    fn try_from(value: DescribeSemaphoreResult) -> Result<Self, Self::Error> {
        if value.status != StatusCode::Success as i32 {
            return Err(RawError::YdbStatus(YdbStatusError {
                message: "".to_string(),
                operation_status: value.status,
                issues: proto_issues_to_ydb_issues(value.issues),
            }));
        }
        let description = value
            .semaphore_description
            .ok_or(RawError::ProtobufDecodeError(
                "semaphore description not found in result".to_string(),
            ))?;
        Ok(RawDescribeSemaphoreResult {
            req_id: value.req_id,
            semaphore_description: description.into(),
            watch_added: value.watch_added,
        })
    }
}
