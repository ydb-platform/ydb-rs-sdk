use tokio::sync::mpsc;

use crate::client_topic::topicreader::reader::TopicReaderCommitMarker;
use crate::grpc_wrapper::raw_topic_service::common::partition::RawOffsetsRange;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::{
    PartitionCommitOffset, RawCommitOffsetRequest, RawFromClientOneOf, RawReadRequest,
};
use crate::{YdbError, YdbResult};

use super::pending_commits::{CommitAckReceiver, PendingCommits};

pub(super) struct ConnectionState {
    outgoing_tx: mpsc::UnboundedSender<RawFromClientOneOf>,
    connection_epoch: usize,
}

impl ConnectionState {
    pub(super) fn new(
        outgoing_tx: mpsc::UnboundedSender<RawFromClientOneOf>,
        connection_epoch: usize,
    ) -> Self {
        Self {
            outgoing_tx,
            connection_epoch,
        }
    }

    pub(super) fn request_bytes(&self, bytes_to_release: i64, epoch: usize) {
        if bytes_to_release > 0 && self.connection_epoch == epoch {
            // Read credit belongs to the current grpc attempt. If its channel
            // is already closed, the attempt is dying and GrpcStreamer will
            // drive reconnect; buffered messages must still be returned.
            let _ = self
                .outgoing_tx
                .send(RawFromClientOneOf::ReadRequest(RawReadRequest {
                    bytes_size: bytes_to_release,
                }));
        }
    }

    pub(super) fn commit(
        &self,
        commit_marker: &TopicReaderCommitMarker,
        pending_commits: &mut PendingCommits,
    ) -> YdbResult<CommitAckReceiver> {
        if commit_marker.epoch != self.connection_epoch {
            return Err(YdbError::custom(format!(
                "topic reader commit for partition session {} belongs to connection epoch {}, current epoch {}",
                commit_marker.partition_session_id,
                commit_marker.epoch,
                self.connection_epoch,
            )));
        }

        let receiver =
            pending_commits.push(commit_marker.partition_session_id, commit_marker.end_offset);
        let commit_message = RawFromClientOneOf::CommitOffsetRequest(RawCommitOffsetRequest {
            commit_offsets: vec![PartitionCommitOffset {
                partition_session_id: commit_marker.partition_session_id,
                offsets: vec![RawOffsetsRange {
                    start: commit_marker.start_offset,
                    end: commit_marker.end_offset,
                }],
            }],
        });

        if let Err(err) = self.outgoing_tx.send(commit_message) {
            let err = YdbError::Transport(format!("topic reader commit send failed: {err}"));
            pending_commits.fail_one(
                commit_marker.partition_session_id,
                commit_marker.end_offset,
                &err,
            );
            return Err(err);
        }

        Ok(receiver)
    }
}
