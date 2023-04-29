use crate::errors::YdbIssue;
use crate::grpc_wrapper;
use ydb_grpc::ydb_proto::issue::IssueMessage;

pub(crate) fn proto_issues_to_ydb_issues(proto_issues: Vec<IssueMessage>) -> Vec<YdbIssue> {
    grpc_wrapper::grpc::proto_issues_to_ydb_issues(proto_issues)
}
