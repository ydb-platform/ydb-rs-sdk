use ydb_grpc::ydb_proto::{
    coordination::{session_request, session_response::UpdateSemaphoreResult},
    status_ids::StatusCode,
};

use crate::{
    client_coordination::session::controller::IdentifiedMessage,
    grpc_wrapper::{grpc::proto_issues_to_ydb_issues, raw_errors::RawError},
    YdbStatusError,
};

#[derive(Debug)]
pub(crate) struct RawUpdateSemaphoreRequest {
    pub req_id: u64,
    pub name: String,
    pub data: Option<Vec<u8>>,
}

#[derive(Debug)]
pub(crate) struct RawUpdateSemaphoreResult {
    pub req_id: u64,
}

impl RawUpdateSemaphoreRequest {
    pub fn new(name: String, data: Option<Vec<u8>>) -> Self {
        Self {
            req_id: 0,
            name,
            data,
        }
    }
}

impl IdentifiedMessage for RawUpdateSemaphoreRequest {
    fn id(&self) -> u64 {
        self.req_id
    }

    fn set_id(&mut self, id: u64) {
        self.req_id = id
    }
}

impl From<RawUpdateSemaphoreRequest> for session_request::Request {
    fn from(value: RawUpdateSemaphoreRequest) -> Self {
        session_request::Request::UpdateSemaphore(session_request::UpdateSemaphore {
            req_id: value.req_id,
            name: value.name,
            data: value.data.unwrap_or_default(),
        })
    }
}

impl IdentifiedMessage for RawUpdateSemaphoreResult {
    fn id(&self) -> u64 {
        self.req_id
    }

    fn set_id(&mut self, id: u64) {
        self.req_id = id
    }
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
