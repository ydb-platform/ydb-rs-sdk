use std::collections::HashMap;
use ydb_grpc::google_proto_workaround::protobuf::Timestamp;
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::topic::stream_read_message;

pub fn init_response(session_id: impl Into<String>) -> stream_read_message::FromServer {
    let session_id = session_id.into();
    stream_read_message::FromServer {
        status: StatusCode::Success as i32,
        issues: Vec::new(),
        server_message: Some(
            stream_read_message::from_server::ServerMessage::InitResponse(
                stream_read_message::InitResponse { session_id },
            ),
        ),
    }
}

pub fn start_partition_session_request(
    partition_session_id: i64,
    topic_path: impl Into<String>,
    partition_id: i64,
    committed_offset: i64,
) -> stream_read_message::FromServer {
    let topic_path = topic_path.into();
    stream_read_message::FromServer {
        status: StatusCode::Success as i32,
        issues: Vec::new(),
        server_message: Some(
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
        ),
    }
}

pub fn read_response(
    partition_session_id: i64,
    offset: i64,
    data: impl Into<Vec<u8>>,
) -> stream_read_message::FromServer {
    read_response_batch(partition_session_id, vec![(offset, data.into())])
}

pub fn read_response_batch(
    partition_session_id: i64,
    messages: Vec<(i64, Vec<u8>)>,
) -> stream_read_message::FromServer {
    let message_data = messages
        .into_iter()
        .map(|(offset, data)| {
            let uncompressed_size = data.len() as i64;
            stream_read_message::read_response::MessageData {
                offset,
                seq_no: offset,
                created_at: None,
                data,
                uncompressed_size,
                message_group_id: String::new(),
                metadata_items: Vec::new(),
            }
        })
        .collect::<Vec<_>>();

    let bytes_size = message_data
        .iter()
        .map(|message| message.data.len() as i64)
        .sum();

    stream_read_message::FromServer {
        status: StatusCode::Success as i32,
        issues: Vec::new(),
        server_message: Some(
            stream_read_message::from_server::ServerMessage::ReadResponse(
                stream_read_message::ReadResponse {
                    bytes_size,
                    partition_data: vec![stream_read_message::read_response::PartitionData {
                        partition_session_id,
                        batches: vec![stream_read_message::read_response::Batch {
                            message_data,
                            producer_id: "mock-producer".to_string(),
                            write_session_meta: HashMap::new(),
                            codec: 0,
                            written_at: Some(Timestamp {
                                seconds: 0,
                                nanos: 0,
                            }),
                        }],
                    }],
                },
            ),
        ),
    }
}

pub fn commit_offset_response(
    partition_session_id: i64,
    committed_offset: i64,
) -> stream_read_message::FromServer {
    stream_read_message::FromServer {
        status: StatusCode::Success as i32,
        issues: Vec::new(),
        server_message: Some(
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
        ),
    }
}

pub fn stop_partition_session_request(
    partition_session_id: i64,
    graceful: bool,
    committed_offset: i64,
) -> stream_read_message::FromServer {
    stream_read_message::FromServer {
        status: StatusCode::Success as i32,
        issues: Vec::new(),
        server_message: Some(
            stream_read_message::from_server::ServerMessage::StopPartitionSessionRequest(
                stream_read_message::StopPartitionSessionRequest {
                    partition_session_id,
                    graceful,
                    committed_offset,
                    last_direct_read_id: 0,
                },
            ),
        ),
    }
}

pub fn status_response(status: StatusCode) -> stream_read_message::FromServer {
    stream_read_message::FromServer {
        status: status as i32,
        issues: Vec::new(),
        server_message: None,
    }
}
