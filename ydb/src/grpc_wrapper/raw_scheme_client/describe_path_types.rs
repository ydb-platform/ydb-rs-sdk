use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use crate::SchemeEntry;

use super::list_directory_types::from_grpc_to_scheme_entry;

#[derive(Debug)]
pub(crate) struct RawDescribePathRequest {
    pub(crate) operation_params: RawOperationParams,
    pub(crate) path: String,
}

impl From<RawDescribePathRequest> for ydb_grpc::ydb_proto::scheme::DescribePathRequest {
    fn from(v: RawDescribePathRequest) -> Self {
        Self {
            operation_params: Some(v.operation_params.into()),
            path: v.path,
        }
    }
}

#[derive(Debug)]
pub(crate) struct RawDescribePathResult {
    pub(crate) entry: SchemeEntry,
}

impl TryFrom<ydb_grpc::ydb_proto::scheme::DescribePathResult> for RawDescribePathResult {
    type Error = RawError;

    fn try_from(
        value: ydb_grpc::ydb_proto::scheme::DescribePathResult,
    ) -> Result<Self, Self::Error> {
        let entry = value.self_.ok_or(RawError::ProtobufDecodeError(
            "describe path self entry is empty".to_string(),
        ))?;

        Ok(Self {
            entry: from_grpc_to_scheme_entry(entry),
        })
    }
}
