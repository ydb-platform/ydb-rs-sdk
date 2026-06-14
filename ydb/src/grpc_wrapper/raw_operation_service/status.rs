use crate::errors::YdbStatusError;
use crate::grpc_wrapper::grpc::proto_issues_to_ydb_issues;
use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use ydb_grpc::ydb_proto::issue::IssueMessage;
use ydb_grpc::ydb_proto::status_ids::StatusCode;

pub(crate) fn check_status(status: i32, issues: &[IssueMessage]) -> RawResult<()> {
    let code = StatusCode::try_from(status)
        .map_err(|e| RawError::custom(format!("unknown status code: {e}")))?;
    if code != StatusCode::Success {
        return Err(RawError::YdbStatus(YdbStatusError {
            message: format!("operation service status: {code:?}"),
            operation_status: status,
            issues: proto_issues_to_ydb_issues(issues.to_vec()),
        }));
    }
    Ok(())
}
