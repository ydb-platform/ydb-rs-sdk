use std::collections::HashMap;

use super::common::codecs::RawSupportedCodecs;
use super::common::consumer::RawConsumer;
use super::common::metering_mode::RawMeteringMode;
use super::common::partitioning_settings::RawAlterPartitioningSettings;
use crate::client_topic::client::AlterTopicOptions;
use crate::grpc_wrapper::raw_common_types::Duration;
use crate::grpc_wrapper::raw_topic_service::common::consumer::RawAlterConsumer;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use ydb_grpc::ydb_proto::topic::{AlterConsumer, AlterTopicRequest, Consumer, MeteringMode};

#[derive(serde::Serialize)]
pub(crate) struct RawAlterTopicRequest {
    pub operation_params: RawOperationParams,
    pub path: String,
    pub alter_partitioning_settings: Option<RawAlterPartitioningSettings>,
    pub set_retention_period: Option<Duration>,
    pub set_retention_storage_mb: Option<i64>,
    pub set_supported_codecs: Option<RawSupportedCodecs>,
    pub set_partition_write_speed_bytes_per_second: Option<i64>,
    pub set_partition_write_burst_bytes: Option<i64>,
    pub alter_attributes: HashMap<String, String>,
    pub add_consumers: Vec<RawConsumer>,
    pub drop_consumers: Vec<String>,
    pub alter_consumers: Vec<RawAlterConsumer>,
    pub set_metering_mode: RawMeteringMode,
}

impl RawAlterTopicRequest {
    pub(crate) fn new(
        path: String,
        operation_params: RawOperationParams,
        options: AlterTopicOptions,
    ) -> Self {
        let alter_partitioning_settings = if options.set_min_active_partitions.is_some()
            || options.set_partition_count_limit.is_some()
        {
            Some(RawAlterPartitioningSettings {
                set_min_active_partitions: options.set_min_active_partitions,
                set_partition_count_limit: options.set_partition_count_limit,
            })
        } else {
            None
        };

        Self {
            operation_params,
            path,
            alter_partitioning_settings,
            set_retention_period: options.set_retention_period.map(|x| x.into()),
            set_retention_storage_mb: options.set_retention_storage_mb,
            set_supported_codecs: options.set_supported_codecs.map(|x| x.into()),
            set_partition_write_speed_bytes_per_second: options
                .set_partition_write_speed_bytes_per_second,
            set_partition_write_burst_bytes: options.set_partition_write_burst_bytes,
            alter_attributes: options.alter_attributes,
            add_consumers: options
                .add_consumers
                .into_iter()
                .map(RawConsumer::from)
                .collect(),
            drop_consumers: options.drop_consumers,
            alter_consumers: options
                .alter_consumers
                .into_iter()
                .map(RawAlterConsumer::from)
                .collect(),
            set_metering_mode: options.set_metering_mode.into(),
        }
    }
}

impl From<RawAlterTopicRequest> for AlterTopicRequest {
    fn from(value: RawAlterTopicRequest) -> Self {
        Self {
            operation_params: Some(value.operation_params.into()),
            path: value.path,
            alter_partitioning_settings: value.alter_partitioning_settings.map(|x| x.into()),
            set_retention_period: value.set_retention_period.map(|x| x.into()),
            set_retention_storage_mb: value.set_retention_storage_mb,
            set_supported_codecs: value.set_supported_codecs.map(|x| x.into()),
            set_partition_write_speed_bytes_per_second: value
                .set_partition_write_speed_bytes_per_second,
            set_partition_write_burst_bytes: value.set_partition_write_burst_bytes,
            alter_attributes: value.alter_attributes,
            add_consumers: value
                .add_consumers
                .into_iter()
                .map(Consumer::from)
                .collect(),
            drop_consumers: value.drop_consumers,
            alter_consumers: value
                .alter_consumers
                .into_iter()
                .map(AlterConsumer::from)
                .collect(),
            set_metering_mode: MeteringMode::from(value.set_metering_mode).into(),
        }
    }
}
