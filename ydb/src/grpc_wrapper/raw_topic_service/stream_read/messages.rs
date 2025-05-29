use crate::grpc_wrapper::raw_common_types::{Duration, Timestamp};
use crate::grpc_wrapper::raw_topic_service::common::codecs::RawCodec;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use ydb_grpc::ydb_proto::topic::stream_read_message;

pub(crate) struct RawInitRequest {
    pub topics_read_settings: Vec<RawTopicReadSettings>,
    pub consumer: String,
    pub reader_name: String,
}

impl From<RawInitRequest> for stream_read_message::InitRequest {
    fn from(value: RawInitRequest) -> Self {
        stream_read_message::InitRequest {
            topics_read_settings: value
                .topics_read_settings
                .into_iter()
                .map(|x| x.into())
                .collect(),
            consumer: value.consumer,
            reader_name: value.reader_name,
            direct_read: false,
        }
    }
}

pub(crate) struct RawTopicReadSettings {
    pub path: String,
    pub partition_ids: Vec<i64>,
    pub max_lag: Option<Duration>,
    pub read_from: Option<Timestamp>,
}

impl From<RawTopicReadSettings> for stream_read_message::init_request::TopicReadSettings {
    fn from(value: RawTopicReadSettings) -> Self {
        stream_read_message::init_request::TopicReadSettings {
            path: value.path,
            partition_ids: value.partition_ids,
            max_lag: value.max_lag.map(|val| val.into()),
            read_from: value.read_from.map(|val| val.into()),
        }
    }
}

pub(crate) struct RawReadRequest {
    pub bytes_size: i64,
}

impl From<RawReadRequest> for stream_read_message::ReadRequest {
    fn from(value: RawReadRequest) -> Self {
        stream_read_message::ReadRequest {
            bytes_size: value.bytes_size,
        }
    }
}

pub(crate) struct RawReadResponse {
    pub bytes_size: i64,

    pub partition_data: Vec<RawPartitionData>,
}

impl From<stream_read_message::ReadResponse> for RawReadResponse {
    fn from(value: stream_read_message::ReadResponse) -> Self {
        RawReadResponse {
            bytes_size: value.bytes_size,
            partition_data: value.partition_data.into_iter().map(|x| x.into()).collect(),
        }
    }
}

pub(crate) struct RawPartitionData {
    pub partition_session_id: i64,
    pub batches: Vec<RawBatch>,
}

impl From<stream_read_message::read_response::PartitionData> for RawPartitionData {
    fn from(value: stream_read_message::read_response::PartitionData) -> Self {
        RawPartitionData {
            partition_session_id: value.partition_session_id,
            batches: value.batches.into_iter().map(|x| x.into()).collect(),
        }
    }
}

pub(crate) struct RawBatch {
    pub producer_id: String,
    pub write_session_meta: HashMap<String, String>,
    pub codec: RawCodec,
    pub written_at: Timestamp,

    pub message_data: Vec<RawMessageData>,
}

impl From<stream_read_message::read_response::Batch> for RawBatch {
    fn from(value: stream_read_message::read_response::Batch) -> Self {
        RawBatch {
            producer_id: value.producer_id,
            write_session_meta: value
                .write_session_meta
                .into_iter()
                .map(|x| x.into())
                .collect(),
            codec: RawCodec { code: value.codec },
            written_at: value
                .written_at
                .map_or(Timestamp::from(UNIX_EPOCH), |x| x.into()),
            message_data: value.message_data.into_iter().map(|x| x.into()).collect(),
        }
    }
}

pub(crate) struct RawMessageData {
    offset: i64,
    seq_no: i64,
    created_at: Option<Timestamp>,
    uncompressed_size: i64,
    metadata_items: Vec<MetaItem>,
    pub data: Vec<u8>,
}

impl From<stream_read_message::read_response::MessageData> for RawMessageData {
    fn from(value: stream_read_message::read_response::MessageData) -> Self {
        RawMessageData {
            offset: value.offset,
            seq_no: value.seq_no,
            created_at: value.created_at.map(|x| x.into()),
            uncompressed_size: value.uncompressed_size,
            metadata_items: value.metadata_items.into_iter().map(|x| x.into()).collect(),
            data: value.data.into_iter().map(|x| x.into()).collect(),
        }
    }
}

pub(crate) struct MetaItem {
    key: String,
    value: String,
}

impl From<stream_read_message::read_response::MessageData::> for MetaItem {
    fn from(value: stream_read_message::read_response::MetaItem) -> Self {
        MetaItem {
            key: value.key,
            value: value.value,
        }
    }
}