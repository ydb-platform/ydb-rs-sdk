use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawStartPartitionSessionRequest;

pub(crate) struct PartitionSession {
    pub partition_session_id: i64,
    pub partition_id: i64,
    pub topic: String,

    // Each offset up to and including (committed_offset - 1) was fully processed.
    pub next_commit_offset_start: i64,
}

impl From<RawStartPartitionSessionRequest> for PartitionSession {
    fn from(request: RawStartPartitionSessionRequest) -> Self {
        Self {
            partition_session_id: request.partition_session.partition_session_id,
            partition_id: request.partition_session.partition_id,
            topic: request.partition_session.path,
            next_commit_offset_start: request.committed_offset,
        }
    }
}
