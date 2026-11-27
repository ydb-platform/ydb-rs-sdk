use crate::client_metrics::names::TopicReaderMetrics;
use crate::client_topic::topicreader::ids::{PartitionId, PartitionSessionId};
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawStartPartitionSessionRequest;

pub(crate) struct PartitionSession {
    pub partition_session_id: PartitionSessionId,
    pub partition_id: PartitionId,
    pub topic: String,
    pub delivered_messages: metrics::Counter,

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
            delivered_messages: metrics::Counter::noop(),
            next_commit_offset_start: marker.end_offset,
        }
    }
}

impl PartitionSession {
    pub(crate) fn new(
        request: RawStartPartitionSessionRequest,
        metrics: &TopicReaderMetrics,
    ) -> Self {
        Self {
            partition_session_id: request.partition_session.partition_session_id,
            partition_id: request.partition_session.partition_id,
            delivered_messages: metrics.delivered_messages(&request.partition_session.path),
            topic: request.partition_session.path,
            next_commit_offset_start: request.committed_offset,
        }
    }
}
