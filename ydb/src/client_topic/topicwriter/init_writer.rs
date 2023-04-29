use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_topic_service::common::codecs::RawSupportedCodecs;
use ydb_grpc::ydb_proto::topic::stream_write_message::from_server::ServerMessage;
use ydb_grpc::ydb_proto::topic::stream_write_message::FromServer;

pub(crate) struct RawInitResponse {
    pub last_seq_no: i64,
    pub session_id: String,
    pub partition_id: i64,
    pub supported_codecs: RawSupportedCodecs,
}

impl TryFrom<ServerMessage> for RawInitResponse {
    type Error = RawError;

    fn try_from(value: ServerMessage) -> Result<Self, Self::Error> {
        match value {
            ServerMessage::InitResponse(body) => Ok(RawInitResponse {
                last_seq_no: body.last_seq_no,
                session_id: body.session_id,
                partition_id: body.partition_id,
                supported_codecs: RawSupportedCodecs::from(
                    body.supported_codecs.unwrap_or_default(),
                ),
            }),
            ServerMessage::WriteResponse(_) => Err(RawError::Custom(
                "Expected to get InitResponse, got WriteResponse instead".to_string(),
            )),
            ServerMessage::UpdateTokenResponse(_) => Err(RawError::Custom(
                "Expected to get InitResponse, got UpdateTokenResponse instead".to_string(),
            )),
        }
    }
}
