use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::trait_operation::Operation;
use crate::{YdbError, YdbIssue, YdbResult};
use ydb_grpc::ydb_proto::issue::IssueMessage;
use ydb_grpc::ydb_proto::status_ids::StatusCode;

#[tracing::instrument]
pub(crate) fn grpc_read_operation_result<TOp, T>(resp: tonic::Response<TOp>) -> RawResult<T>
where
    TOp: Operation,
    T: Default + prost::Message,
{
    let resp_inner = resp.into_inner();
    let op = resp_inner
        .operation()
        .ok_or(RawError::Custom("no operation object in result".into()))?;
    if op.status() != StatusCode::Success {
        return Err(create_operation_error(op));
    }
    let opres = op
        .result
        .ok_or(RawError::Custom("no result data in operation".into()))?;
    let res: T = T::decode(opres.value)?;
    return Ok(res);
}

pub(crate) fn grpc_read_void_operation_result<TOp>(resp: tonic::Response<TOp>) -> RawResult<()>
where
    TOp: Operation,
{
    let resp_inner = resp.into_inner();
    let op = resp_inner
        .operation()
        .ok_or(RawError::Custom("no operation object in result".into()))?;
    if op.status() != StatusCode::Success {
        return Err(create_operation_error(op));
    }
    return Ok(());
}

pub(crate) fn create_operation_error(op: ydb_grpc::ydb_proto::operations::Operation) -> RawError {
    return RawError::YdbStatus(crate::errors::YdbStatusError {
        message: format!("{:?}", &op),
        operation_status: op.status,
        issues: proto_issues_to_ydb_issues(op.issues),
    });
}

pub(crate) fn proto_issues_to_ydb_issues(proto_issues: Vec<IssueMessage>) -> Vec<YdbIssue> {
    proto_issues
        .into_iter()
        .map(|proto_issue| YdbIssue {
            issue_code: proto_issue.issue_code,
            message: proto_issue.message,
            issues: proto_issues_to_ydb_issues(proto_issue.issues),
            severity: proto_issue.severity,
        })
        .collect()
}
