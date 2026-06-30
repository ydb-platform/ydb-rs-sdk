use super::handler::TopicReply;
use std::collections::HashMap;
use ydb_grpc::google_proto_workaround::protobuf::Timestamp;
use ydb_grpc::ydb_proto::operations::Operation;
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::topic;
use ydb_grpc::ydb_proto::topic::{stream_read_message, stream_write_message};

// --- StreamRead messages (server → client over the bidi stream) ---

pub fn init_response(stream_id: u64, session_id: impl Into<String>) -> TopicReply {
    let session_id = session_id.into();
    stream_read(
        stream_id,
        stream_read_message::from_server::ServerMessage::InitResponse(
            stream_read_message::InitResponse { session_id },
        ),
    )
}

pub fn update_token_response(stream_id: u64) -> TopicReply {
    stream_read(
        stream_id,
        stream_read_message::from_server::ServerMessage::UpdateTokenResponse(
            topic::UpdateTokenResponse {},
        ),
    )
}

pub fn start_partition_session_request(
    stream_id: u64,
    partition_session_id: i64,
    topic_path: impl Into<String>,
    partition_id: i64,
    committed_offset: i64,
) -> TopicReply {
    let topic_path = topic_path.into();
    stream_read(
        stream_id,
        stream_read_message::from_server::ServerMessage::StartPartitionSessionRequest(
            stream_read_message::StartPartitionSessionRequest {
                partition_session: Some(stream_read_message::PartitionSession {
                    partition_session_id,
                    path: topic_path,
                    partition_id,
                }),
                committed_offset,
                partition_offsets: None,
                partition_location: None,
            },
        ),
    )
}

pub fn read_response(
    stream_id: u64,
    partition_session_id: i64,
    offset: i64,
    data: impl Into<Vec<u8>>,
) -> TopicReply {
    let data = data.into();
    read_response_with_codec(
        stream_id,
        partition_session_id,
        offset,
        data.len() as i64,
        data,
        ydb::Codec::RAW,
    )
}

pub fn read_response_with_codec(
    stream_id: u64,
    partition_session_id: i64,
    offset: i64,
    uncompressed_size: i64,
    data: impl Into<Vec<u8>>,
    codec: ydb::Codec,
) -> TopicReply {
    read_response_batch_with_codec(
        stream_id,
        partition_session_id,
        vec![(offset, uncompressed_size, data.into())],
        codec,
    )
}

pub fn read_response_batch_with_codec(
    stream_id: u64,
    partition_session_id: i64,
    messages: Vec<(i64, i64, Vec<u8>)>,
    codec: ydb::Codec,
) -> TopicReply {
    let message_data = messages
        .into_iter()
        .map(
            |(offset, uncompressed_size, data)| stream_read_message::read_response::MessageData {
                offset,
                seq_no: offset,
                created_at: None,
                data,
                uncompressed_size,
                message_group_id: String::new(),
                metadata_items: Vec::new(),
            },
        )
        .collect::<Vec<_>>();

    let bytes_size = message_data
        .iter()
        .map(|message| message.data.len() as i64)
        .sum();

    stream_read(
        stream_id,
        stream_read_message::from_server::ServerMessage::ReadResponse(
            stream_read_message::ReadResponse {
                bytes_size,
                partition_data: vec![stream_read_message::read_response::PartitionData {
                    partition_session_id,
                    batches: vec![stream_read_message::read_response::Batch {
                        message_data,
                        producer_id: "mock-producer".to_string(),
                        write_session_meta: HashMap::new(),
                        codec: codec.code,
                        written_at: Some(Timestamp {
                            seconds: 0,
                            nanos: 0,
                        }),
                    }],
                }],
            },
        ),
    )
}

pub fn commit_offset_response(
    stream_id: u64,
    partition_session_id: i64,
    committed_offset: i64,
) -> TopicReply {
    stream_read(
        stream_id,
        stream_read_message::from_server::ServerMessage::CommitOffsetResponse(
            stream_read_message::CommitOffsetResponse {
                partitions_committed_offsets: vec![
                    stream_read_message::commit_offset_response::PartitionCommittedOffset {
                        partition_session_id,
                        committed_offset,
                    },
                ],
            },
        ),
    )
}

pub fn stop_partition_session_request(
    stream_id: u64,
    partition_session_id: i64,
    graceful: bool,
    committed_offset: i64,
) -> TopicReply {
    stream_read(
        stream_id,
        stream_read_message::from_server::ServerMessage::StopPartitionSessionRequest(
            stream_read_message::StopPartitionSessionRequest {
                partition_session_id,
                graceful,
                committed_offset,
                last_direct_read_id: 0,
            },
        ),
    )
}

pub fn empty_with_status(stream_id: u64, status: StatusCode) -> TopicReply {
    TopicReply::StreamRead {
        stream_id,
        msg: stream_read_message::FromServer {
            status: status as i32,
            issues: Vec::new(),
            server_message: None,
        },
    }
}

fn stream_read(stream_id: u64, msg: stream_read_message::from_server::ServerMessage) -> TopicReply {
    TopicReply::StreamRead {
        stream_id,
        msg: stream_read_message::FromServer {
            status: StatusCode::Success as i32,
            issues: Vec::new(),
            server_message: Some(msg),
        },
    }
}

// --- StreamWrite messages (server → client over the bidi stream) ---

pub fn write_init_response(
    stream_id: u64,
    session_id: impl Into<String>,
    partition_id: i64,
) -> TopicReply {
    stream_write(
        stream_id,
        stream_write_message::from_server::ServerMessage::InitResponse(
            stream_write_message::InitResponse {
                last_seq_no: 0,
                session_id: session_id.into(),
                partition_id,
                supported_codecs: None,
            },
        ),
    )
}

pub fn write_ack_written_in_tx(stream_id: u64, seq_no: i64) -> TopicReply {
    use stream_write_message::write_response::write_ack;
    stream_write(
        stream_id,
        stream_write_message::from_server::ServerMessage::WriteResponse(
            stream_write_message::WriteResponse {
                acks: vec![stream_write_message::write_response::WriteAck {
                    seq_no,
                    message_write_status: Some(write_ack::MessageWriteStatus::WrittenInTx(
                        write_ack::WrittenInTx {},
                    )),
                }],
                partition_id: 0,
                write_statistics: None,
            },
        ),
    )
}

pub fn write_ack_written(stream_id: u64, seq_no: i64, offset: i64) -> TopicReply {
    use stream_write_message::write_response::write_ack;
    stream_write(
        stream_id,
        stream_write_message::from_server::ServerMessage::WriteResponse(
            stream_write_message::WriteResponse {
                acks: vec![stream_write_message::write_response::WriteAck {
                    seq_no,
                    message_write_status: Some(write_ack::MessageWriteStatus::Written(
                        write_ack::Written { offset },
                    )),
                }],
                partition_id: 0,
                write_statistics: None,
            },
        ),
    )
}

pub fn write_ack_skipped_already_written(stream_id: u64, seq_no: i64) -> TopicReply {
    use stream_write_message::write_response::write_ack;
    stream_write(
        stream_id,
        stream_write_message::from_server::ServerMessage::WriteResponse(
            stream_write_message::WriteResponse {
                acks: vec![stream_write_message::write_response::WriteAck {
                    seq_no,
                    message_write_status: Some(write_ack::MessageWriteStatus::Skipped(
                        write_ack::Skipped {
                            reason: write_ack::skipped::Reason::AlreadyWritten as i32,
                        },
                    )),
                }],
                partition_id: 0,
                write_statistics: None,
            },
        ),
    )
}

fn stream_write(
    stream_id: u64,
    msg: stream_write_message::from_server::ServerMessage,
) -> TopicReply {
    TopicReply::StreamWrite {
        stream_id,
        msg: stream_write_message::FromServer {
            status: StatusCode::Success as i32,
            issues: Vec::new(),
            server_message: Some(msg),
        },
    }
}

// --- Unary RPC responses ---

pub fn unary_commit_offset_response() -> TopicReply {
    TopicReply::CommitOffset(topic::CommitOffsetResponse {
        operation: Some(success_operation()),
    })
}

pub fn update_offsets_in_transaction_response() -> TopicReply {
    TopicReply::UpdateOffsetsInTransaction(topic::UpdateOffsetsInTransactionResponse {
        operation: Some(success_operation()),
    })
}

pub fn create_topic_response() -> TopicReply {
    TopicReply::CreateTopic(topic::CreateTopicResponse {
        operation: Some(success_operation()),
    })
}

pub fn describe_topic_response() -> TopicReply {
    TopicReply::DescribeTopic(topic::DescribeTopicResponse {
        operation: Some(success_operation()),
    })
}

pub fn describe_consumer_response() -> TopicReply {
    TopicReply::DescribeConsumer(topic::DescribeConsumerResponse {
        operation: Some(success_operation()),
    })
}

pub fn alter_topic_response() -> TopicReply {
    TopicReply::AlterTopic(topic::AlterTopicResponse {
        operation: Some(success_operation()),
    })
}

pub fn drop_topic_response() -> TopicReply {
    TopicReply::DropTopic(topic::DropTopicResponse {
        operation: Some(success_operation()),
    })
}

fn success_operation() -> Operation {
    Operation {
        id: String::new(),
        ready: true,
        status: StatusCode::Success as i32,
        issues: Vec::new(),
        result: None,
        metadata: None,
        cost_info: None,
    }
}
