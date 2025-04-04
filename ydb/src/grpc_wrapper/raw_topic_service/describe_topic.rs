use super::common::{
    codecs::RawSupportedCodecs, consumer::RawConsumer, metering_mode::RawMeteringMode,
    partition::RawPartitionInfo, partitioning_settings::RawPartitioningSettings,
    topic::RawTopicStats,
};
use crate::{
    client_topic::client::DescribeTopicOptions,
    grpc_wrapper::{
        raw_common_types::Duration,
        raw_errors::{RawError, RawResult},
        raw_scheme_client::list_directory_types::from_grpc_to_scheme_entry,
        raw_ydb_operation::RawOperationParams,
    },
};
use std::collections::HashMap;
use ydb_grpc::ydb_proto::topic::{DescribeTopicRequest, DescribeTopicResult};

#[derive(Debug)]
pub(crate) struct RawDescribeTopicRequest {
    pub path: String,
    pub operation_params: RawOperationParams,
    pub include_stats: bool,
    pub include_location: bool,
}

impl RawDescribeTopicRequest {
    pub(crate) fn new(
        path: String,
        operation_params: RawOperationParams,
        options: DescribeTopicOptions,
    ) -> Self {
        Self {
            operation_params,
            path,
            include_stats: options.include_stats,
            include_location: options.include_location,
        }
    }
}

impl From<RawDescribeTopicRequest> for DescribeTopicRequest {
    fn from(value: RawDescribeTopicRequest) -> Self {
        Self {
            path: value.path,
            operation_params: Some(value.operation_params.into()),
            include_stats: value.include_stats,
            include_location: value.include_location,
        }
    }
}

#[derive(Debug)]
pub(crate) struct RawDescribeTopicResult {
    pub self_: crate::SchemeEntry,
    pub partitioning_settings: RawPartitioningSettings,
    pub partitions: Vec<RawPartitionInfo>,
    pub retention_period: Duration,
    pub retention_storage_mb: i64,
    pub supported_codecs: RawSupportedCodecs,
    pub partition_write_speed_bytes_per_second: i64,
    pub partition_total_read_speed_bytes_per_second: i64,
    pub partition_consumer_read_speed_bytes_per_second: i64,
    pub partition_write_burst_bytes: i64,
    pub attributes: HashMap<String, String>,
    pub consumers: Vec<RawConsumer>,
    pub metering_mode: RawMeteringMode,
    pub topic_stats: Option<RawTopicStats>,
}

impl TryFrom<DescribeTopicResult> for RawDescribeTopicResult {
    type Error = RawError;

    fn try_from(value: DescribeTopicResult) -> RawResult<Self> {
        let entry = value.self_.ok_or(RawError::ProtobufDecodeError(
            "self scheme is absent in result".to_string(),
        ))?;

        let partitioning_settings =
            value
                .partitioning_settings
                .ok_or(RawError::ProtobufDecodeError(
                    "partitioning settings is absent in result".to_string(),
                ))?;

        let partitions = value
            .partitions
            .into_iter()
            .map(|partition| partition.try_into())
            .collect::<RawResult<Vec<RawPartitionInfo>>>()?;

        let retention_period = value.retention_period.ok_or(RawError::ProtobufDecodeError(
            "retention period is absent in result".to_string(),
        ))?;

        Ok(Self {
            self_: from_grpc_to_scheme_entry(entry),
            partitioning_settings: partitioning_settings.into(),
            partitions,
            retention_period: retention_period.into(),
            retention_storage_mb: value.retention_storage_mb,
            supported_codecs: value
                .supported_codecs
                .map_or_else(RawSupportedCodecs::default, |x| x.into()),
            partition_write_speed_bytes_per_second: value.partition_write_speed_bytes_per_second,
            partition_total_read_speed_bytes_per_second: value
                .partition_total_read_speed_bytes_per_second,
            partition_consumer_read_speed_bytes_per_second: value
                .partition_consumer_read_speed_bytes_per_second,
            partition_write_burst_bytes: value.partition_write_burst_bytes,
            attributes: value.attributes,
            consumers: value.consumers.into_iter().map(RawConsumer::from).collect(),
            metering_mode: value.metering_mode.try_into()?,
            topic_stats: value.topic_stats.map(|x| x.try_into()).transpose()?,
        })
    }
}
