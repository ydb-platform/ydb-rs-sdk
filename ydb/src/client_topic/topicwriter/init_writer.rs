use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_topic_service::common::codecs::RawSupportedCodecs;
use ydb_grpc::ydb_proto::topic::stream_write_message::from_server::ServerMessage;


pub(crate) struct RawInitResponse {
    pub last_seq_no: i64,
    pub session_id: String,
    pub partition_id: i64,
    pub supported_codecs: RawSupportedCodecs,
}

impl TryFrom<ServerMessage> for RawInitResponse {
    type Error = RawError;

    fn try_from(value: ServerMessage) -> Result<Self, Self::Error> {
        if let ServerMessage::InitResponse(body) = value {
            Ok(RawInitResponse {
                last_seq_no: body.last_seq_no,
                session_id: body.session_id,
                partition_id: body.partition_id,
                supported_codecs: RawSupportedCodecs::from(
                    body.supported_codecs.unwrap_or_default(),
                ),
            })
        } else {
            let message_string = match serde_json::to_string(&value) {
                Ok(str) => str,
                Err(err) => format!("Failed to serialize message: {}", err),
            };
            Err(RawError::Custom(format!(
                "Expected to get InitResponse, got: {}",
                message_string,
            )))
        }
    }
}
