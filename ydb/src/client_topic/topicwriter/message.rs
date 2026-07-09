use std::time::{self, UNIX_EPOCH};

use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

use crate::YdbError;

#[derive(bon::Builder)]
pub struct TopicWriterMessage {
    // required
    pub(crate) data: Vec<u8>,

    // optional metadata
    pub(crate) seq_no: Option<i64>,
    #[builder(default = time::SystemTime::now())]
    pub(crate) created_at: time::SystemTime,
}

impl TryFrom<TopicWriterMessage> for MessageData {
    type Error = YdbError;

    fn try_from(value: TopicWriterMessage) -> Result<Self, Self::Error> {
        let data_size = value.data.len() as i64;

        let seq_no = value
            .seq_no
            .ok_or_else(|| YdbError::custom("empty message seq_no is provided"))?;

        let created_at = value.created_at.duration_since(UNIX_EPOCH)?;

        Ok(MessageData {
            seq_no,
            created_at: Some(ydb_grpc::google_proto_workaround::protobuf::Timestamp {
                seconds: created_at.as_secs() as i64,
                nanos: created_at.subsec_nanos() as i32,
            }),
            metadata_items: vec![],
            data: value.data,
            uncompressed_size: data_size,
            partitioning: None,
        })
    }
}
