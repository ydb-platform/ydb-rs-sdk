use crate::grpc_wrapper::raw_topic_service::{
    common::codecs::RawSupportedCodecs, stream_write::init::RawInitResponse,
};

#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
pub(crate) struct ConnectionInfo {
    pub(crate) partition_id: i64,
    pub(crate) session_id: String,
    pub(crate) last_seq_no_assigned: Option<i64>,
    pub(crate) codecs_from_server: RawSupportedCodecs,
}

impl ConnectionInfo {
    // Updates fields from init response.
    //
    // last_seq_no from init response is only respected during first connection.
    // In further reconnections, the last_seq_no_assigned is not updated with it
    // in order to maintain the established internal seq_no order.
    pub(crate) fn update_from_init_response(&mut self, init_response: RawInitResponse) {
        self.partition_id = init_response.partition_id;
        self.session_id = init_response.session_id;
        self.last_seq_no_assigned
            .get_or_insert(init_response.last_seq_no);
        self.codecs_from_server = init_response.supported_codecs;
    }
}
