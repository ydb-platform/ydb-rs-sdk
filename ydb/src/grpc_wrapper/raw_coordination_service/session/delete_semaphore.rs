use ydb_grpc::ydb_proto::{
    coordination::{session_request, session_response::DeleteSemaphoreResult},
    status_ids::StatusCode,
};

use crate::{
    client_coordination::session::session::IdentifiedMessage,
    grpc_wrapper::{grpc::proto_issues_to_ydb_issues, raw_errors::RawError},
    YdbStatusError,
};

#[derive(Debug)]
pub(crate) struct RawDeleteSemaphoreRequest {
    pub req_id: u64,
    pub name: String,
    pub force: bool,
}

#[derive(Debug)]
pub(crate) struct RawDeleteSemaphoreResult {
    pub req_id: u64,
}

impl RawDeleteSemaphoreRequest {
    pub fn new(name: String, force: bool) -> Self {
        Self {
            req_id: 0,
            name,
            force,
        }
    }
}

impl IdentifiedMessage for RawDeleteSemaphoreRequest {
    fn id(&self) -> u64 {
        self.req_id
    }

    fn set_id(&mut self, id: u64) {
        self.req_id = id
    }
}

impl From<RawDeleteSemaphoreRequest> for session_request::Request {
    fn from(value: RawDeleteSemaphoreRequest) -> Self {
        session_request::Request::DeleteSemaphore(session_request::DeleteSemaphore {
            req_id: value.req_id,
            name: value.name,
            force: value.force,
        })
    }
}

impl IdentifiedMessage for RawDeleteSemaphoreResult {
    fn id(&self) -> u64 {
        self.req_id
    }

    fn set_id(&mut self, id: u64) {
        self.req_id = id
    }
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
