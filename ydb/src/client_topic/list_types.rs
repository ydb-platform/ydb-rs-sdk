use crate::grpc_wrapper::raw_topic_service::common::codecs::{RawCodec, RawSupportedCodecs};
use crate::grpc_wrapper::raw_topic_service::common::consumer::{RawAlterConsumer, RawConsumer};
use crate::grpc_wrapper::raw_topic_service::common::metering_mode::RawMeteringMode;
use crate::grpc_wrapper::raw_topic_service::common::partition::{
    RawPartitionInfo, RawPartitionLocation, RawPartitionStats,
};
use crate::grpc_wrapper::raw_topic_service::common::partitioning_settings::RawPartitioningSettings;
use crate::grpc_wrapper::raw_topic_service::common::topic::RawTopicStats;
use crate::grpc_wrapper::raw_topic_service::describe_topic::RawDescribeTopicResult;
use derive_builder::Builder;
use std::collections::HashMap;
use std::option::Option;
use std::time::SystemTime;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Codec {
    pub code: i32,
}

impl Codec {
    pub const RAW: Codec = Codec { code: 1 };
    pub const GZIP: Codec = Codec { code: 2 };
    pub const LZOP: Codec = Codec { code: 3 };
    pub const ZSTD: Codec = Codec { code: 4 };

    pub fn is_custom(&self) -> bool {
        self.code >= 10000 && self.code < 20000
    }
}

impl From<RawCodec> for Codec {
    fn from(value: RawCodec) -> Self {
        Self { code: value.code }
    }
}

impl From<Codec> for RawCodec {
    fn from(value: Codec) -> Self {
        Self { code: value.code }
    }
}

impl From<RawSupportedCodecs> for Vec<Codec> {
    fn from(value: RawSupportedCodecs) -> Vec<Codec> {
        value.codecs.into_iter().map(Codec::from).collect()
    }
}

impl From<Vec<Codec>> for RawSupportedCodecs {
    fn from(value: Vec<Codec>) -> RawSupportedCodecs {
        Self {
            codecs: value.into_iter().map(RawCodec::from).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeteringMode {
    ReservedCapacity,
    RequestUnits,
}

impl From<RawMeteringMode> for Option<MeteringMode> {
    fn from(value: RawMeteringMode) -> Self {
        match value {
            RawMeteringMode::Unspecified => None,
            RawMeteringMode::ReservedCapacity => Some(MeteringMode::ReservedCapacity),
            RawMeteringMode::RequestUnits => Some(MeteringMode::RequestUnits),
        }
    }
}

impl From<Option<MeteringMode>> for RawMeteringMode {
    fn from(value: Option<MeteringMode>) -> Self {
        match value {
            None => RawMeteringMode::Unspecified,
            Some(MeteringMode::RequestUnits) => RawMeteringMode::RequestUnits,
            Some(MeteringMode::ReservedCapacity) => RawMeteringMode::ReservedCapacity,
        }
    }
}

#[derive(Debug, Clone, Builder)]
#[builder(build_fn(error = "crate::errors::YdbError"))]
pub struct Consumer {
    pub name: String,

    #[builder(default)]
    pub important: bool,

    #[builder(default)]
    pub read_from: Option<SystemTime>,

    #[builder(default)]
    pub supported_codecs: Vec<Codec>,

    #[builder(default)]
    pub attributes: HashMap<String, String>,
}

impl From<RawConsumer> for Consumer {
    fn from(consumer: RawConsumer) -> Self {
        Self {
            name: consumer.name,
            important: consumer.important,
            read_from: consumer.read_from.map(|x| x.into()),
            supported_codecs: consumer.supported_codecs.into(),
            attributes: consumer.attributes,
        }
    }
}

impl From<Consumer> for RawConsumer {
    fn from(consumer: Consumer) -> Self {
        Self {
            name: consumer.name,
            important: consumer.important,
            read_from: consumer.read_from.map(|x| x.into()),
            supported_codecs: consumer.supported_codecs.into(),
            attributes: consumer.attributes,
        }
    }
}

#[derive(Debug, Clone, Builder)]
#[builder(build_fn(error = "crate::errors::YdbError"))]
pub struct AlterConsumer {
    pub name: String,

    #[builder(default)]
    pub set_important: Option<bool>,

    #[builder(default)]
    pub set_read_from: Option<SystemTime>,

    #[builder(default)]
    pub set_supported_codecs: Option<Vec<Codec>>,

    #[builder(default)]
    pub alter_attributes: HashMap<String, String>,
}

impl From<AlterConsumer> for RawAlterConsumer {
    fn from(consumer: AlterConsumer) -> Self {
        Self {
            name: consumer.name,
            set_important: consumer.set_important,
            set_read_from: consumer.set_read_from.map(|x| x.into()),
            set_supported_codecs: consumer.set_supported_codecs.map(|x| x.into()),
            alter_attributes: consumer.alter_attributes,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PartitioningSettings {
    pub min_active_partitions: i64,
    pub partition_count_limit: i64,
}

impl From<RawPartitioningSettings> for PartitioningSettings {
    fn from(value: RawPartitioningSettings) -> Self {
        Self {
            min_active_partitions: value.min_active_partitions,
            partition_count_limit: value.partition_count_limit,
        }
    }
}

impl From<PartitioningSettings> for RawPartitioningSettings {
    fn from(value: PartitioningSettings) -> Self {
        Self {
            min_active_partitions: value.min_active_partitions,
            partition_count_limit: value.partition_count_limit,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PartitionStats {
    pub start_offset: i64,
    pub end_offset: i64,
    pub store_size_bytes: i64,
    pub last_write_time: SystemTime,
    pub max_write_time_lag: std::time::Duration,
    pub bytes_written_per_minute: i64,
    pub bytes_written_per_hour: i64,
    pub bytes_written_per_day: i64,
}

impl From<RawPartitionStats> for PartitionStats {
    fn from(value: RawPartitionStats) -> Self {
        Self {
            start_offset: value.partition_offsets.start,
            end_offset: value.partition_offsets.end,
            store_size_bytes: value.store_size_bytes,
            last_write_time: value.last_write_time.into(),
            max_write_time_lag: value.max_write_time_lag.into(),
            bytes_written_per_minute: value.bytes_written.per_minute,
            bytes_written_per_hour: value.bytes_written.per_hour,
            bytes_written_per_day: value.bytes_written.per_day,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PartitionLocation {
    pub node_id: i32,
    pub generation: i64,
}

impl From<RawPartitionLocation> for PartitionLocation {
    fn from(value: RawPartitionLocation) -> Self {
        Self {
            node_id: value.node_id,
            generation: value.generation,
        }
    }
}

/// PartitionInfo contains info about partition.
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub partition_id: i64,
    pub active: bool,
    pub child_partition_ids: Vec<i64>,
    pub parent_partition_ids: Vec<i64>,
    pub stats: Option<PartitionStats>,
    pub location: Option<PartitionLocation>,
}

impl From<RawPartitionInfo> for PartitionInfo {
    fn from(value: RawPartitionInfo) -> Self {
        Self {
            partition_id: value.partition_id,
            active: value.active,
            child_partition_ids: value.child_partition_ids,
            parent_partition_ids: value.parent_partition_ids,
            stats: value.partition_stats.map(|x| x.into()),
            location: value.partition_location.map(|x| x.into()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TopicStats {
    pub store_size_bytes: i64,
    pub min_last_write_time: SystemTime,
    pub max_write_time_lag: std::time::Duration,
    pub bytes_written_per_minute: i64,
    pub bytes_written_per_hour: i64,
    pub bytes_written_per_day: i64,
}

impl From<RawTopicStats> for TopicStats {
    fn from(value: RawTopicStats) -> Self {
        Self {
            store_size_bytes: value.store_size_bytes,
            min_last_write_time: value.min_last_write_time.into(),
            max_write_time_lag: value.max_write_time_lag.into(),
            bytes_written_per_minute: value.bytes_written.per_minute,
            bytes_written_per_hour: value.bytes_written.per_hour,
            bytes_written_per_day: value.bytes_written.per_day,
        }
    }
}

/// TopicDescription contains info about topic.
#[derive(Debug, Clone)]
pub struct TopicDescription {
    pub path: String,
    pub partitioning_settings: PartitioningSettings,
    pub partitions: Vec<PartitionInfo>,
    pub retention_period: std::time::Duration,
    pub retention_storage_mb: Option<i64>,
    pub supported_codecs: Vec<Codec>,
    pub partition_write_speed_bytes_per_second: i64,
    pub partition_total_read_speed_bytes_per_second: i64,
    pub partition_consumer_read_speed_bytes_per_second: i64,
    pub partition_write_burst_bytes: i64,
    pub attributes: HashMap<String, String>,
    pub consumers: Vec<Consumer>,
    pub metering_mode: Option<MeteringMode>,
    pub stats: Option<TopicStats>,
}

impl From<RawDescribeTopicResult> for TopicDescription {
    fn from(value: RawDescribeTopicResult) -> Self {
        let retention_storage_mb = if value.retention_storage_mb > 0 {
            Some(value.retention_storage_mb)
        } else {
            None
        };

        Self {
            path: value.self_.name,
            partitioning_settings: value.partitioning_settings.into(),
            partitions: value.partitions.into_iter().map(|x| x.into()).collect(),
            retention_period: value.retention_period.into(),
            retention_storage_mb,
            supported_codecs: value
                .supported_codecs
                .codecs
                .into_iter()
                .map(|x| x.into())
                .collect(),
            partition_write_speed_bytes_per_second: value.partition_write_speed_bytes_per_second,
            partition_total_read_speed_bytes_per_second: value
                .partition_total_read_speed_bytes_per_second,
            partition_consumer_read_speed_bytes_per_second: value
                .partition_consumer_read_speed_bytes_per_second,
            partition_write_burst_bytes: value.partition_write_burst_bytes,
            attributes: value.attributes,
            consumers: value.consumers.into_iter().map(|x| x.into()).collect(),
            metering_mode: value.metering_mode.into(),
            stats: value.topic_stats.map(|x| x.into()),
        }
    }
}
