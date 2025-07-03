use ydb_grpc::ydb_proto::topic::stream_write_message::InitResponse;

use crate::grpc_wrapper::{
    raw_errors::{RawError, RawResult},
    raw_topic_service::common::codecs::RawSupportedCodecs,
};

use super::RawServerMessage;

#[derive(serde::Serialize)]
pub(crate) struct RawInitResponse {
    pub last_seq_no: i64,
    pub session_id: String,
    pub partition_id: i64,
    pub supported_codecs: RawSupportedCodecs,
}

impl TryFrom<InitResponse> for RawInitResponse {
    type Error = RawError;

    fn try_from(value: InitResponse) -> RawResult<Self> {
        Ok(Self {
            last_seq_no: value.last_seq_no,
            session_id: value.session_id,
            partition_id: value.partition_id,
            supported_codecs: RawSupportedCodecs::from(value.supported_codecs.unwrap_or_default()),
        })
    }
}

impl TryFrom<RawServerMessage> for RawInitResponse {
    type Error = RawError;

    fn try_from(value: RawServerMessage) -> RawResult<Self> {
        if let RawServerMessage::Init(response) = value {
            Ok(response)
        } else {
            let message_string = match serde_json::to_string(&value) {
                Ok(str) => str,
                Err(err) => format!("Failed to serialize message: {err}"),
            };
            Err(RawError::Custom(format!(
                "Expected to get InitResponse, got: {message_string}",
            )))
        }
    }
}
