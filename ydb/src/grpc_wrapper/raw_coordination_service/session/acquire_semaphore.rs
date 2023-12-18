use std::time::Duration;

use ydb_grpc::ydb_proto::{
    coordination::{session_request, session_response::AcquireSemaphoreResult},
    status_ids::StatusCode,
};

use crate::{
    client_coordination::session::session::IdentifiedMessage,
    grpc_wrapper::{grpc::proto_issues_to_ydb_issues, raw_errors::RawError},
    AcquireCount, YdbStatusError,
};

#[derive(Debug)]
pub(crate) struct RawAcquireSemaphoreRequest {
    pub req_id: u64,
    pub name: String,
    pub count: AcquireCount,
    pub timeout: Duration,
    pub ephemeral: bool,
    pub data: Option<Vec<u8>>,
}

#[derive(Debug)]
pub(crate) struct RawAcquireSemaphoreResult {
    pub req_id: u64,
    pub acquired: bool,
}

impl RawAcquireSemaphoreRequest {
    pub fn new(
        name: String,
        count: AcquireCount,
        timeout: Duration,
        ephemeral: bool,
        data: Option<Vec<u8>>,
    ) -> Self {
        Self {
            req_id: 0,
            name,
            count,
            timeout,
            ephemeral,
            data,
        }
    }
}

impl IdentifiedMessage for RawAcquireSemaphoreRequest {
    fn id(&self) -> u64 {
        self.req_id
    }

    fn set_id(&mut self, id: u64) {
        self.req_id = id
    }
}

impl From<RawAcquireSemaphoreRequest> for session_request::Request {
    fn from(value: RawAcquireSemaphoreRequest) -> Self {
        session_request::Request::AcquireSemaphore(session_request::AcquireSemaphore {
            req_id: value.req_id,
            name: value.name,
            count: value.count.into(),
            timeout_millis: value.timeout.as_millis() as u64,
            ephemeral: value.ephemeral,
            data: value.data.unwrap_or_default(),
        })
    }
}

impl IdentifiedMessage for RawAcquireSemaphoreResult {
    fn id(&self) -> u64 {
        self.req_id
    }

    fn set_id(&mut self, id: u64) {
        self.req_id = id
    }
}

impl TryFrom<AcquireSemaphoreResult> for RawAcquireSemaphoreResult {
    type Error = RawError;

    fn try_from(value: AcquireSemaphoreResult) -> Result<Self, Self::Error> {
        if value.status != StatusCode::Success as i32 {
            return Err(RawError::YdbStatus(YdbStatusError {
                message: "".to_string(),
                operation_status: value.status,
                issues: proto_issues_to_ydb_issues(value.issues),
            }));
        }
        Ok(RawAcquireSemaphoreResult {
            req_id: value.req_id,
            acquired: value.acquired,
        })
    }
}
