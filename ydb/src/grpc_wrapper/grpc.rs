use crate::YdbIssue;
use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::trait_operation::{Operation, YdbGrpcStatus};
use ydb_grpc::ydb_proto::issue::IssueMessage;
use ydb_grpc::ydb_proto::status_ids::StatusCode;

pub(crate) fn grpc_check_status<T>(operation: &impl YdbGrpcStatus<T>) -> RawResult<()> {
    let status = operation.status()?;

    if status != StatusCode::Success {
        let issues = operation.issues()?;
        let issues = issues.to_vec();

        Err(RawError::YdbStatus(crate::errors::YdbStatusError {
            message: format!("{:?}", operation),
            operation_status: status.into(),
            issues: proto_issues_to_ydb_issues(issues),
        }))
    } else {
        Ok(())
    }
}

#[tracing::instrument]
pub(crate) fn grpc_read_operation_result<TOp, T>(resp: tonic::Response<TOp>) -> RawResult<T>
where
    TOp: YdbGrpcStatus<T>,
{
    let resp_inner = resp.into_inner();
    grpc_check_status(&resp_inner)?;

    resp_inner.into_result()
}

pub(crate) fn grpc_read_void_operation_result<TOp>(resp: tonic::Response<TOp>) -> RawResult<()>
where
    TOp: Operation,
{
    let resp_inner = resp.into_inner();
    let op = resp_inner
        .operation()
        .ok_or_else(|| RawError::Custom("no operation object in result".into()))?;
    if op.status() != StatusCode::Success {
        return Err(create_operation_error(op));
    }
    Ok(())
}

pub(crate) fn create_operation_error(op: ydb_grpc::ydb_proto::operations::Operation) -> RawError {
    RawError::YdbStatus(crate::errors::YdbStatusError {
        message: format!("{:?}", op),
        operation_status: op.status,
        issues: proto_issues_to_ydb_issues(op.issues),
    })
}

pub(crate) fn proto_issues_to_ydb_issues(proto_issues: Vec<IssueMessage>) -> Vec<YdbIssue> {
    proto_issues
        .into_iter()
        .map(|proto_issue| YdbIssue {
            issue_code: proto_issue.issue_code,
            message: proto_issue.message,
            issues: proto_issues_to_ydb_issues(proto_issue.issues),
            severity: proto_issue.severity.into(),
        })
        .collect()
}
