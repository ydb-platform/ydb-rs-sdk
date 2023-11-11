use ydb_grpc::ydb_proto::coordination::session_response::{
    AcquireSemaphorePending, DescribeSemaphoreChanged, PingPong, SessionStarted, SessionStopped,
};
use ydb_grpc::ydb_proto::coordination::{session_response, SessionResponse};

pub(crate) mod acquire_semaphore;
pub(crate) mod create_semaphore;
pub(crate) mod delete_semaphore;
pub(crate) mod describe_semaphore;
pub(crate) mod release_semaphore;
pub(crate) mod update_semaphore;

use crate::grpc_wrapper::{
    grpc::proto_issues_to_ydb_issues,
    raw_errors::{RawError, RawResult},
};
use crate::YdbStatusError;

use self::acquire_semaphore::RawAcquireSemaphoreResult;
use self::create_semaphore::RawCreateSemaphoreResult;
use self::delete_semaphore::RawDeleteSemaphoreResult;
use self::describe_semaphore::RawDescribeSemaphoreResult;
use self::release_semaphore::RawReleaseSemaphoreResult;
use self::update_semaphore::RawUpdateSemaphoreResult;

pub(crate) enum RawSessionResponse {
    Ping(PingPong),
    Pong(PingPong),
    SessionStarted(SessionStarted),
    SessionStopped(SessionStopped),
    AcquireSemaphorePending(AcquireSemaphorePending),
    AcquireSemaphoreResult(RawAcquireSemaphoreResult),
    ReleaseSemaphoreResult(RawReleaseSemaphoreResult),
    DescribeSemaphoreResult(RawDescribeSemaphoreResult),
    DescribeSemaphoreChanged(DescribeSemaphoreChanged),
    CreateSemaphoreResult(RawCreateSemaphoreResult),
    UpdateSemaphoreResult(RawUpdateSemaphoreResult),
    DeleteSemaphoreResult(RawDeleteSemaphoreResult),
}

impl TryFrom<SessionResponse> for RawSessionResponse {
    type Error = RawError;

    fn try_from(value: SessionResponse) -> RawResult<Self> {
        let response = value.response.ok_or(RawError::Custom(
            "Session response is absent in streaming response body".to_string(),
        ))?;

        match response {
            session_response::Response::Ping(ping) => Ok(RawSessionResponse::Ping(ping)),
            session_response::Response::Pong(pong) => Ok(RawSessionResponse::Pong(pong)),
            session_response::Response::Failure(fail) => Err(RawError::YdbStatus(YdbStatusError {
                message: "".to_string(), // TODO: what message?
                operation_status: fail.status,
                issues: proto_issues_to_ydb_issues(fail.issues),
            })),
            session_response::Response::SessionStarted(started) => {
                Ok(RawSessionResponse::SessionStarted(started))
            }
            session_response::Response::SessionStopped(stopped) => {
                Ok(RawSessionResponse::SessionStopped(stopped))
            }
            session_response::Response::AcquireSemaphorePending(pending) => {
                Ok(RawSessionResponse::AcquireSemaphorePending(pending))
            }
            session_response::Response::AcquireSemaphoreResult(acquired) => {
                RawAcquireSemaphoreResult::try_from(acquired)
                    .map(RawSessionResponse::AcquireSemaphoreResult)
            }
            session_response::Response::ReleaseSemaphoreResult(released) => {
                RawReleaseSemaphoreResult::try_from(released)
                    .map(RawSessionResponse::ReleaseSemaphoreResult)
            }
            session_response::Response::CreateSemaphoreResult(created) => {
                RawCreateSemaphoreResult::try_from(created)
                    .map(RawSessionResponse::CreateSemaphoreResult)
            }
            session_response::Response::DeleteSemaphoreResult(deleted) => {
                RawDeleteSemaphoreResult::try_from(deleted)
                    .map(RawSessionResponse::DeleteSemaphoreResult)
            }
            session_response::Response::UpdateSemaphoreResult(updated) => {
                RawUpdateSemaphoreResult::try_from(updated)
                    .map(RawSessionResponse::UpdateSemaphoreResult)
            }
            session_response::Response::DescribeSemaphoreResult(described) => {
                RawDescribeSemaphoreResult::try_from(described)
                    .map(RawSessionResponse::DescribeSemaphoreResult)
            }
            session_response::Response::DescribeSemaphoreChanged(changed) => {
                Ok(RawSessionResponse::DescribeSemaphoreChanged(changed))
            }
            session_response::Response::Unsupported6(..) => {
                Err(RawError::Custom("unsupported".to_string()))
            }
            session_response::Response::Unsupported7(..) => {
                Err(RawError::Custom("unsupported".to_string()))
            }
            session_response::Response::Unsupported16(..) => {
                Err(RawError::Custom("unsupported".to_string()))
            }
            session_response::Response::Unsupported17(..) => {
                Err(RawError::Custom("unsupported".to_string()))
            }
            session_response::Response::Unsupported18(..) => {
                Err(RawError::Custom("unsupported".to_string()))
            }
        }
    }
}
