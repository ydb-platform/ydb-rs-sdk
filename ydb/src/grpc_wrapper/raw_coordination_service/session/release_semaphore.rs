use ydb_grpc::ydb_proto::{
    coordination::{session_request, session_response::ReleaseSemaphoreResult},
    status_ids::StatusCode,
};

use crate::{
    client_coordination::session::session::IdentifiedMessage,
    grpc_wrapper::{grpc::proto_issues_to_ydb_issues, raw_errors::RawError},
    YdbStatusError,
};

#[derive(Debug)]
pub(crate) struct RawReleaseSemaphoreRequest {
    pub req_id: u64,
    pub name: String,
}

#[derive(Debug)]
pub(crate) struct RawReleaseSemaphoreResult {
    pub req_id: u64,
    pub released: bool,
}

impl RawReleaseSemaphoreRequest {
    pub fn new(name: String) -> Self {
        Self { req_id: 0, name }
    }
}

impl IdentifiedMessage for RawReleaseSemaphoreRequest {
    fn id(&self) -> u64 {
        self.req_id
    }

    fn set_id(&mut self, id: u64) {
        self.req_id = id
    }
}

impl From<RawReleaseSemaphoreRequest> for session_request::Request {
    fn from(value: RawReleaseSemaphoreRequest) -> Self {
        session_request::Request::ReleaseSemaphore(session_request::ReleaseSemaphore {
            req_id: value.req_id,
            name: value.name,
        })
    }
}

impl IdentifiedMessage for RawReleaseSemaphoreResult {
    fn id(&self) -> u64 {
        self.req_id
    }

    fn set_id(&mut self, id: u64) {
        self.req_id = id
    }
}

impl TryFrom<ReleaseSemaphoreResult> for RawReleaseSemaphoreResult {
    type Error = RawError;

    fn try_from(value: ReleaseSemaphoreResult) -> Result<Self, Self::Error> {
        if value.status != StatusCode::Success as i32 {
            return Err(RawError::YdbStatus(YdbStatusError {
                message: "".to_string(),
                operation_status: value.status,
                issues: proto_issues_to_ydb_issues(value.issues),
            }));
        }
        Ok(RawReleaseSemaphoreResult {
            req_id: value.req_id,
            released: value.released,
        })
    }
}
