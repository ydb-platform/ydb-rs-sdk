use std::time::{self, UNIX_EPOCH};

use derive_builder::Builder;
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

use crate::{errors, YdbError, YdbResult};

#[derive(Builder)]
#[builder(build_fn(error = "errors::YdbError", validate = "Self::validate"))]
#[allow(dead_code)]
pub struct TopicWriterMessage {
    #[builder(default = "None")]
    pub(crate) seq_no: Option<i64>,
    #[builder(default = "time::SystemTime::now()")]
    pub(crate) created_at: time::SystemTime,

    pub(crate) data: Vec<u8>,
}

impl TopicWriterMessageBuilder {
    fn validate(&self) -> YdbResult<()> {
        Ok(())
    }
}

impl TryFrom<TopicWriterMessage> for MessageData {
    type Error = YdbError;

    fn try_from(value: TopicWriterMessage) -> Result<Self, Self::Error> {
        let data_size = value.data.len() as i64;

        let seq_no = value
            .seq_no
            .ok_or_else(|| YdbError::custom("empty message seq_no is provided"))?;

        let duration = value.created_at.duration_since(UNIX_EPOCH)?;

        Ok(MessageData {
            seq_no: seq_no,
            created_at: Some(ydb_grpc::google_proto_workaround::protobuf::Timestamp {
                seconds: duration.as_secs() as i64,
                nanos: duration.subsec_nanos() as i32,
            }),
            metadata_items: vec![],
            data: value.data,
            uncompressed_size: data_size,
            partitioning: None,
        })
    }
}
