use std::time::Duration;

use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

use crate::client_topic::topicwriter::message_write_status::{MessageWriteStatus, WriteAck};
use crate::client_topic::topicwriter::writer_options::{
    AutoFlushSettings, TopicWriterOptions, WriterFlowControl,
};

pub(crate) fn writer_flow_control(
    inflight_messages: usize,
    inflight_bytes: usize,
    auto_flush_messages: usize,
    auto_flush_bytes: usize,
    auto_flush_interval: Duration,
) -> WriterFlowControl {
    let options = TopicWriterOptions::builder()
        .topic_path("test-topic")
        .max_inflight_messages(inflight_messages)
        .max_inflight_bytes(inflight_bytes)
        .auto_flush_messages(auto_flush_messages)
        .auto_flush_bytes(auto_flush_bytes)
        .auto_flush_interval(auto_flush_interval)
        .build();

    WriterFlowControl::try_from(&options).unwrap()
}

pub(crate) fn auto_flush_settings(
    messages: usize,
    bytes: usize,
    interval: Duration,
) -> AutoFlushSettings {
    writer_flow_control(messages, bytes, messages, bytes, interval).auto_flush()
}

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
