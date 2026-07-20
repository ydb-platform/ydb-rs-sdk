use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use std::collections::HashMap;
use ydb_grpc::ydb_proto::table::DescribeTableOptionsRequest;

pub(crate) struct RawDescribeTableOptionsRequest {
    pub operation_params: RawOperationParams,
}

impl From<RawDescribeTableOptionsRequest> for DescribeTableOptionsRequest {
    fn from(value: RawDescribeTableOptionsRequest) -> Self {
        Self {
            operation_params: Some(value.operation_params.into()),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RawNamedPolicyDescription {
    pub name: String,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub(crate) struct RawDescribeTableOptionsResult {
    pub table_profile_presets: Vec<RawNamedPolicyDescription>,
    pub storage_policy_presets: Vec<RawNamedPolicyDescription>,
    pub compaction_policy_presets: Vec<RawNamedPolicyDescription>,
    pub partitioning_policy_presets: Vec<RawNamedPolicyDescription>,
    pub execution_policy_presets: Vec<RawNamedPolicyDescription>,
    pub replication_policy_presets: Vec<RawNamedPolicyDescription>,
    pub caching_policy_presets: Vec<RawNamedPolicyDescription>,
}

macro_rules! map_policy_field {
    ($value:expr_2021, $field:ident) => {
        $value
            .$field
            .into_iter()
            .map(|item| RawNamedPolicyDescription {
                name: item.name,
                labels: item.labels,
            })
            .collect()
    };
}

impl TryFrom<ydb_grpc::ydb_proto::table::DescribeTableOptionsResult>
    for RawDescribeTableOptionsResult
{
    type Error = RawError;

    fn try_from(
        value: ydb_grpc::ydb_proto::table::DescribeTableOptionsResult,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            table_profile_presets: map_policy_field!(value, table_profile_presets),
            storage_policy_presets: map_policy_field!(value, storage_policy_presets),
            compaction_policy_presets: map_policy_field!(value, compaction_policy_presets),
            partitioning_policy_presets: map_policy_field!(value, partitioning_policy_presets),
            execution_policy_presets: map_policy_field!(value, execution_policy_presets),
            replication_policy_presets: map_policy_field!(value, replication_policy_presets),
            caching_policy_presets: map_policy_field!(value, caching_policy_presets),
        })
    }
}
