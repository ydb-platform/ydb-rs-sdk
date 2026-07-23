use crate::client_topic::topicreader::ids::{PartitionId, PartitionSessionId};
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawStartPartitionSessionRequest;

pub(crate) struct PartitionSession {
    pub partition_session_id: PartitionSessionId,
    pub partition_id: PartitionId,
    pub topic: String,

    // Each offset up to and including (committed_offset - 1) was fully processed.
    pub next_commit_offset_start: i64,
}

#[cfg(test)]
impl PartitionSession {
    pub(crate) fn from_message(
        message: &crate::client_topic::topicreader::messages::TopicReaderMessage,
    ) -> Self {
        let marker = message.get_commit_marker();
        Self {
            partition_session_id: marker.partition_session_id,
            partition_id: marker.partition_id,
            topic: marker.topic,
            next_commit_offset_start: marker.end_offset,
        }
    }
}

impl From<RawStartPartitionSessionRequest> for PartitionSession {
    fn from(request: RawStartPartitionSessionRequest) -> Self {
        Self {
            partition_session_id: PartitionSessionId::from_raw(
                request.partition_session.partition_session_id,
            ),
            partition_id: PartitionId::from_raw(request.partition_session.partition_id),
            topic: request.partition_session.path,
            next_commit_offset_start: request.committed_offset,
        }
    }
}
