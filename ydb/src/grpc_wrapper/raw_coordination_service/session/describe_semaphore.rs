use ydb_grpc::ydb_proto::{
    coordination::{session_request, session_response::DescribeSemaphoreResult},
    status_ids::StatusCode,
};

use crate::{
    client_coordination::{list_types::SemaphoreDescription, session::session::IdentifiedMessage},
    grpc_wrapper::{grpc::proto_issues_to_ydb_issues, raw_errors::RawError},
    WatchMode, YdbStatusError,
};

#[derive(Debug)]
pub(crate) struct RawDescribeSemaphoreRequest {
    pub req_id: u64,
    pub name: String,
    pub include_owners: bool,
    pub include_waiters: bool,
    pub watch_mode: Option<WatchMode>,
}

#[derive(Debug)]
pub(crate) struct RawDescribeSemaphoreResult {
    pub req_id: u64,
    pub semaphore_description: SemaphoreDescription,
    pub watch_added: bool,
}

impl RawDescribeSemaphoreRequest {
    pub fn new(
        name: String,
        include_owners: bool,
        include_waiters: bool,
        watch_mode: Option<WatchMode>,
    ) -> Self {
        Self {
            req_id: 0,
            name,
            include_owners,
            include_waiters,
            watch_mode,
        }
    }
}

impl IdentifiedMessage for RawDescribeSemaphoreRequest {
    fn id(&self) -> u64 {
        self.req_id
    }

    fn set_id(&mut self, id: u64) {
        self.req_id = id
    }
}

impl From<RawDescribeSemaphoreRequest> for session_request::Request {
    fn from(value: RawDescribeSemaphoreRequest) -> Self {
        let (watch_data, watch_owners) = match value.watch_mode {
            Some(crate::WatchMode::All) => (true, true),
            Some(crate::WatchMode::Data) => (true, false),
            Some(crate::WatchMode::Owners) => (false, true),
            None => (false, false),
        };
        session_request::Request::DescribeSemaphore(session_request::DescribeSemaphore {
            req_id: value.req_id,
            name: value.name,
            include_owners: value.include_owners,
            include_waiters: value.include_waiters,
            watch_data,
            watch_owners,
        })
    }
}

impl IdentifiedMessage for RawDescribeSemaphoreResult {
    fn id(&self) -> u64 {
        self.req_id
    }

    fn set_id(&mut self, id: u64) {
        self.req_id = id
    }
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
