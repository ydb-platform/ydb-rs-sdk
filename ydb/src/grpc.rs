use std::time::Duration;

use crate::errors::{YdbIssue, YdbResult};
use crate::grpc_wrapper;
use crate::trait_operation::Operation;
use ydb_grpc::ydb_proto::issue::IssueMessage;
use ydb_grpc::ydb_proto::operations::operation_params::OperationMode;
use ydb_grpc::ydb_proto::operations::OperationParams;

pub(crate) fn grpc_read_operation_result<TOp, T>(resp: tonic::Response<TOp>) -> YdbResult<T>
where
    TOp: Operation,
    T: Default + prost::Message,
{
    Ok(grpc_wrapper::grpc::grpc_read_operation_result(resp)?)
}

pub(crate) fn proto_issues_to_ydb_issues(proto_issues: Vec<IssueMessage>) -> Vec<YdbIssue> {
    grpc_wrapper::grpc::proto_issues_to_ydb_issues(proto_issues)
}

pub(crate) fn operation_params(timeout: Duration) -> Option<OperationParams> {
    Some(OperationParams {
        operation_mode: OperationMode::Sync.into(),
        operation_timeout: Some(timeout.into()),
        ..OperationParams::default()
    })
}
