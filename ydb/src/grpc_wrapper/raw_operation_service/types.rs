use crate::grpc_wrapper::grpc::proto_issues_to_ydb_issues;
use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::YdbIssue;
use ydb_grpc::ydb_proto::operations::Operation;

#[derive(Debug, Clone)]
pub(crate) struct RawOperation {
    pub id: String,
    pub ready: bool,
    pub status: i32,
    pub issues: Vec<YdbIssue>,
    pub consumed_units: Option<f64>,
}

impl TryFrom<Operation> for RawOperation {
    type Error = RawError;

    fn try_from(op: Operation) -> RawResult<Self> {
        Ok(Self {
            id: op.id,
            ready: op.ready,
            status: op.status,
            issues: proto_issues_to_ydb_issues(op.issues),
            consumed_units: op.cost_info.map(|c| c.consumed_units),
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RawListOperationsRequest {
    pub kind: String,
    pub page_size: u64,
    pub page_token: String,
}

#[derive(Debug, Clone)]
pub(crate) struct RawListOperationsResult {
    pub operations: Vec<RawOperation>,
    pub next_page_token: String,
}
