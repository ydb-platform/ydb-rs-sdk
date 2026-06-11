use ydb_grpc::ydb_proto::topic::stream_read_message;

pub enum TopicServerCommand {
    SendReadStreamMessage(stream_read_message::FromServer),
    CloseReadStream,
    FailReadStream(tonic::Status),
}
