use ydb_grpc::ydb_proto::topic::stream_write_message::from_server::ServerMessage;
use ydb_grpc::ydb_proto::topic::stream_write_message::FromServer;

use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_topic_service::common::codecs::RawSupportedCodecs;
use crate::grpc_wrapper::raw_topic_service::stream_write::from_grpc_to_server_message;

pub(crate) struct RawInitResponse {
    pub last_seq_no: i64,
    pub session_id: String,
    pub partition_id: i64,
    pub supported_codecs: RawSupportedCodecs,
}

impl TryFrom<FromServer> for RawInitResponse {
    type Error = RawError;

    fn try_from(value: FromServer) -> Result<Self, Self::Error> {
        let value = from_grpc_to_server_message(value)?;
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
