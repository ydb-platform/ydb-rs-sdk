use ydb_grpc::ydb_proto::topic::stream_read_message;

#[derive(Debug)]
pub enum TopicClientEvent {
    StreamReadOpened {
        stream_id: u64,
    },
    StreamReadMessage {
        stream_id: u64,
        message: stream_read_message::FromClient,
    },
    StreamReadClosed {
        stream_id: u64,
    },
    StreamReadError {
        stream_id: u64,
        status: tonic::Status,
    },
}
