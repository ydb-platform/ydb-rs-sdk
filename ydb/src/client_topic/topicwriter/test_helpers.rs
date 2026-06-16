use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

use crate::client_topic::topicwriter::message_write_status::{MessageWriteStatus, WriteAck};

pub(crate) fn create_message(seq_no: i64, data: Vec<u8>) -> MessageData {
    MessageData {
        seq_no,
        created_at: None,
        data,
        uncompressed_size: 0,
        metadata_items: vec![],
        partitioning: None,
    }
}

pub(crate) fn write_ack(seq_no: i64) -> WriteAck {
    WriteAck {
        seq_no,
        status: MessageWriteStatus::Unknown,
    }
}
