use crate::grpc_wrapper::raw_topic_service::common::codecs::RawSupportedCodecs;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct ConnectionInfo {
    pub(crate) partition_id: i64,
    pub(crate) session_id: String,
    pub(crate) last_seq_no_assigned: i64,
    pub(crate) codecs_from_server: RawSupportedCodecs,
}
