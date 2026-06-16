use crate::grpc_wrapper::{
    raw_errors::{RawError, RawResult},
    raw_topic_service::{
        common::codecs::RawSupportedCodecs,
        stream_write::{init::RawInitResponse, RawServerMessage},
    },
};

#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
pub(crate) struct ConnectionInfo {
    pub(crate) partition_id: i64,
    pub(crate) session_id: String,
    pub(crate) last_seq_no_assigned: i64,
    pub(crate) codecs_from_server: RawSupportedCodecs,
}

impl TryFrom<RawServerMessage> for ConnectionInfo {
    type Error = RawError;

    fn try_from(value: RawServerMessage) -> RawResult<Self> {
        let raw_init_response = RawInitResponse::try_from(value)?;

        Ok(Self {
            partition_id: raw_init_response.partition_id,
            session_id: raw_init_response.session_id,
            last_seq_no_assigned: raw_init_response.last_seq_no,
            codecs_from_server: raw_init_response.supported_codecs,
        })
    }
}
