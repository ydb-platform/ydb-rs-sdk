use ydb_grpc::ydb_proto::topic::stream_read_message;

#[derive(Debug)]
pub enum TopicClientEvent {
    Opened {
        stream_id: u64,
    },
    Message {
        stream_id: u64,
        message: stream_read_message::FromClient,
    },
    Closed {
        stream_id: u64,
    },
    Error {
        stream_id: u64,
        status: tonic::Status,
    },
}
