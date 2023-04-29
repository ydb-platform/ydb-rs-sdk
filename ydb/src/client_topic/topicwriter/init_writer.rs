use ydb_grpc::ydb_proto::topic::stream_write_message::FromServer;
use ydb_grpc::ydb_proto::topic::stream_write_message::from_server::ServerMessage;
use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_topic_service::common::codecs::RawSupportedCodecs;

pub(crate) struct RawInitResponse {
    pub last_seq_no: i64,
    pub session_id: String,
    pub partition_id: i64,
    pub supported_codecs: RawSupportedCodecs,
}

impl TryFrom<FromServer> for RawInitResponse {
    type Error = RawError;

    fn try_from(value: FromServer) -> Result<Self, Self::Error> {
        if let Some(ServerMessage::InitResponse(init_resp)) = value.server_message {
            Ok(
                Self {
                    last_seq_no: init_resp.last_seq_no,
                    session_id: init_resp.session_id,
                    partition_id: init_resp.partition_id,
                    supported_codecs: init_resp.supported_codecs.unwrap_or_default().into(),
                }
            )
        } else {
            Err(RawError::Custom("Expected writer::FromServer to have actual InitResponse".to_string()))
        }
    }
}