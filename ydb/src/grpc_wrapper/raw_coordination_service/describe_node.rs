use ydb_grpc::ydb_proto::coordination::{DescribeNodeRequest, DescribeNodeResult};

use crate::grpc_wrapper::{
    raw_errors::{RawError, RawResult},
    raw_scheme_client::list_directory_types::from_grpc_to_scheme_entry,
    raw_ydb_operation::RawOperationParams,
};

use super::config::RawCoordinationNodeConfig;

#[derive(Debug)]
pub(crate) struct RawDescribeNodeRequest {
    pub path: String,
    pub operation_params: RawOperationParams,
}

impl From<RawDescribeNodeRequest> for DescribeNodeRequest {
    fn from(value: RawDescribeNodeRequest) -> Self {
        Self {
            path: value.path,
            operation_params: Some(value.operation_params.into()),
        }
    }
}

#[derive(Debug)]
pub(crate) struct RawDescribeNodeResult {
    pub self_: crate::SchemeEntry,
    pub config: RawCoordinationNodeConfig,
}

impl TryFrom<DescribeNodeResult> for RawDescribeNodeResult {
    type Error = RawError;

    fn try_from(value: DescribeNodeResult) -> RawResult<Self> {
        let entry = value.self_.ok_or(RawError::ProtobufDecodeError(
            "self scheme is absent in result".to_string(),
        ))?;

        let config = value.config.ok_or(RawError::ProtobufDecodeError(
            "config is absent in result".to_string(),
        ))?;

        Ok(Self {
            self_: from_grpc_to_scheme_entry(entry),
            config: config.try_into()?,
        })
    }
}
