use ydb_grpc::ydb_proto::{
    coordination::{session_request, session_response::CreateSemaphoreResult},
    status_ids::StatusCode,
};

use crate::{
    client_coordination::session::session::IdentifiedMessage,
    grpc_wrapper::{grpc::proto_issues_to_ydb_issues, raw_errors::RawError},
    SemaphoreLimit, YdbStatusError,
};

#[derive(Debug)]
pub(crate) struct RawCreateSemaphoreRequest {
    pub req_id: u64,
    pub name: String,
    pub limit: SemaphoreLimit,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct RawCreateSemaphoreResult {
    pub req_id: u64,
}

impl RawCreateSemaphoreRequest {
    pub fn new(name: String, limit: SemaphoreLimit, data: Vec<u8>) -> Self {
        Self {
            req_id: 0,
            name,
            limit,
            data,
        }
    }
}

impl IdentifiedMessage for RawCreateSemaphoreRequest {
    fn id(&self) -> u64 {
        self.req_id
    }

    fn set_id(&mut self, id: u64) {
        self.req_id = id
    }
}

impl From<RawCreateSemaphoreRequest> for session_request::Request {
    fn from(value: RawCreateSemaphoreRequest) -> Self {
        session_request::Request::CreateSemaphore(session_request::CreateSemaphore {
            req_id: value.req_id,
            name: value.name,
            limit: value.limit.into(),
            data: value.data,
        })
    }
}

impl IdentifiedMessage for RawCreateSemaphoreResult {
    fn id(&self) -> u64 {
        self.req_id
    }

    fn set_id(&mut self, id: u64) {
        self.req_id = id
    }
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
