use super::multiple_window_stat::RawMultipleWindowsStat;
use crate::grpc_wrapper::{
    raw_common_types::{Duration, Timestamp},
    raw_errors::RawError,
};
use ydb_grpc::ydb_proto::topic::describe_topic_result::TopicStats;

#[derive(serde::Serialize, Clone, Debug)]
pub(crate) struct RawTopicStats {
    pub store_size_bytes: i64,
    pub min_last_write_time: Timestamp,
    pub max_write_time_lag: Duration,
    pub bytes_written: RawMultipleWindowsStat,
}

impl TryFrom<TopicStats> for RawTopicStats {
    type Error = RawError;

    fn try_from(value: TopicStats) -> Result<Self, Self::Error> {
        let min_last_write_time = value.min_last_write_time.ok_or_else(|| {
            RawError::ProtobufDecodeError("min_last_write_time is absent".to_string())
        })?;

        let max_write_time_lag = value.max_write_time_lag.ok_or_else(|| {
            RawError::ProtobufDecodeError("max_write_time_lag is absent".to_string())
        })?;

        let bytes_written = value
            .bytes_written
            .ok_or_else(|| RawError::ProtobufDecodeError("bytes_written is absent".to_string()))?;

        Ok(Self {
            store_size_bytes: value.store_size_bytes,
            min_last_write_time: min_last_write_time.into(),
            max_write_time_lag: max_write_time_lag.into(),
            bytes_written: bytes_written.into(),
        })
    }
}
