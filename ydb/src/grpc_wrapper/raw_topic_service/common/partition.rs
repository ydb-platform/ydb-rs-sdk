use super::multiple_window_stat::RawMultipleWindowsStat;
use crate::grpc_wrapper::{
    raw_common_types::{Duration, Timestamp},
    raw_errors::RawError,
};
use ydb_grpc::ydb_proto::topic::{
    describe_consumer_result, describe_topic_result, OffsetsRange, PartitionLocation,
    PartitionStats,
};

#[derive(serde::Serialize, Clone, Debug)]
pub(crate) struct RawOffsetsRange {
    pub start: i64,
    pub end: i64,
}

impl From<OffsetsRange> for RawOffsetsRange {
    fn from(value: OffsetsRange) -> Self {
        Self {
            start: value.start,
            end: value.end,
        }
    }
}

#[derive(serde::Serialize, Clone, Debug)]
pub(crate) struct RawPartitionStats {
    pub partition_offsets: RawOffsetsRange,
    pub store_size_bytes: i64,
    pub last_write_time: Timestamp,
    pub max_write_time_lag: Duration,
    pub bytes_written: RawMultipleWindowsStat,
}

impl TryFrom<PartitionStats> for RawPartitionStats {
    type Error = RawError;

    fn try_from(value: PartitionStats) -> Result<Self, Self::Error> {
        let partition_offsets = value.partition_offsets.ok_or_else(|| {
            RawError::ProtobufDecodeError("partition_offsets is absent".to_string())
        })?;

        let last_write_time = value.last_write_time.ok_or_else(|| {
            RawError::ProtobufDecodeError("last_write_time is absent".to_string())
        })?;

        let max_write_time_lag = value.max_write_time_lag.ok_or_else(|| {
            RawError::ProtobufDecodeError("max_write_time_lag is absent".to_string())
        })?;

        let bytes_written = value
            .bytes_written
            .ok_or_else(|| RawError::ProtobufDecodeError("bytes_written is absent".to_string()))?;

        Ok(Self {
            partition_offsets: partition_offsets.into(),
            store_size_bytes: value.store_size_bytes,
            last_write_time: last_write_time.into(),
            max_write_time_lag: max_write_time_lag.into(),
            bytes_written: bytes_written.into(),
        })
    }
}

#[derive(serde::Serialize, Clone, Debug)]
pub(crate) struct RawPartitionLocation {
    pub node_id: i32,
    pub generation: i64,
}

impl From<PartitionLocation> for RawPartitionLocation {
    fn from(value: PartitionLocation) -> Self {
        Self {
            node_id: value.node_id,
            generation: value.generation,
        }
    }
}

#[derive(serde::Serialize, Clone, Debug)]
pub(crate) struct RawPartitionInfo {
    pub partition_id: i64,
    pub active: bool,
    pub child_partition_ids: Vec<i64>,
    pub parent_partition_ids: Vec<i64>,
    pub partition_stats: Option<RawPartitionStats>,
    pub partition_location: Option<RawPartitionLocation>,
}

impl TryFrom<describe_topic_result::PartitionInfo> for RawPartitionInfo {
    type Error = RawError;

    fn try_from(value: describe_topic_result::PartitionInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            partition_id: value.partition_id,
            active: value.active,
            child_partition_ids: value.child_partition_ids,
            parent_partition_ids: value.parent_partition_ids,
            partition_stats: value.partition_stats.map(|x| x.try_into()).transpose()?,
            partition_location: value.partition_location.map(|x| x.into()),
        })
    }
}

impl TryFrom<describe_consumer_result::PartitionInfo> for RawPartitionInfo {
    type Error = RawError;

    fn try_from(value: describe_consumer_result::PartitionInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            partition_id: value.partition_id,
            active: value.active,
            child_partition_ids: value.child_partition_ids,
            parent_partition_ids: value.parent_partition_ids,
            partition_stats: value.partition_stats.map(|x| x.try_into()).transpose()?,
            partition_location: value.partition_location.map(|x| x.into()),
        })
    }
}
