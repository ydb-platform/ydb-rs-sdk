use std::collections::{HashMap, VecDeque};

use crate::client_topic::topicreader::ids::{PartitionId, PartitionSessionId};
use crate::client_topic::topicreader::messages::{TopicReaderBatch, TopicReaderMessage};
use crate::client_topic::topicreader::partition_state::PartitionSession;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::{
    RawBatch, RawEndPartitionSession,
};
use crate::{YdbError, YdbResult};

use super::round_robin::RoundRobin;

pub(super) struct BufferedBatch {
    pub(super) messages: Vec<TopicReaderMessage>,
    pub(super) bytes_to_release: i64,
    pub(super) epoch: usize,
}

enum InputState {
    Open,
    Closed,
}

struct PartitionEntry {
    session: PartitionSession,
    queue: VecDeque<TopicReaderMessage>,
    input: InputState,
}

impl PartitionEntry {
    fn new(session: PartitionSession) -> Self {
        Self {
            session,
            queue: VecDeque::new(),
            input: InputState::Open,
        }
    }

    fn is_closed(&self) -> bool {
        matches!(self.input, InputState::Closed)
    }

    fn close_input(&mut self) -> YdbResult<()> {
        let psid = self.session.partition_session_id;

        match self.input {
            InputState::Open => {
                self.input = InputState::Closed;
                Ok(())
            }
            InputState::Closed => Err(YdbError::custom(format!(
                "topic reader close session: session {psid} already closed"
            ))),
        }
    }

    /// Removes up to `cap` messages from the front of the queue.
    fn take_batch(&mut self, cap: usize) -> Option<BufferedBatch> {
        if cap == 0 || self.queue.is_empty() {
            return None;
        }

        let take = cap.min(self.queue.len());
        let mut messages = Vec::with_capacity(take);
        let mut bytes_to_release = 0;
        for _ in 0..take {
            let Some(message) = self.queue.pop_front() else {
                break;
            };
            bytes_to_release += message.bytes_to_release;
            messages.push(message);
        }
        let first = messages.first()?;
        let first_epoch = first.get_commit_marker().epoch;

        Some(BufferedBatch {
            messages,
            bytes_to_release,
            epoch: first_epoch,
        })
    }
}

/// Buffers per-partition messages and decides the order they reach the reader.
///
/// Delivery has two tiers. Sessions ended by the server (`EndPartitionSession`)
/// are fully drained, in end order, before any other session is served; the
/// remaining active sessions share a round-robin. A split/merge child is just
/// another active session, so draining every ended parent first keeps a
/// parent's data ahead of its child without tracking lineage; the child and
/// adjacent partition ids on the end event are not used for routing.
///
/// An ended session keeps its entry (and stays committable) after its queue is
/// drained; only an explicit `stop` removes it.
#[derive(Default)]
pub(super) struct MessageBuffer {
    /// All partition sessions owned by this reader, ended or not, until `stop`.
    entries: HashMap<PartitionSessionId, PartitionEntry>,

    /// Partition id -> owning session id, kept in sync with `entries`.
    partition_to_session: HashMap<PartitionId, PartitionSessionId>,

    /// Ended (input-closed) sessions in end order. Drained front-first before
    /// any round-robin session, which keeps each parent ahead of its children.
    priority_parent_sessions: VecDeque<PartitionSessionId>,

    /// Active, non-ended sessions, served round-robin once the priority queue
    /// is empty. Disjoint from `priority_parent_sessions`.
    round_robin: RoundRobin,
}

impl MessageBuffer {
    pub(super) fn start(&mut self, session: PartitionSession) -> YdbResult<()> {
        let psid = session.partition_session_id;
        let pid = session.partition_id;

        if let Some(existing) = self.partition_to_session.get(&pid) {
            return Err(YdbError::custom(format!(
                "topic reader start partition session: duplicate partition {pid}, new session {psid}, existing session {existing}"
            )));
        }

        if self.entries.contains_key(&psid) {
            return Err(YdbError::custom(format!(
                "topic reader start partition session: duplicate partition session {psid}"
            )));
        }

        self.round_robin.push(psid);

        self.partition_to_session.insert(pid, psid);
        self.entries.insert(psid, PartitionEntry::new(session));

        Ok(())
    }

    pub(super) fn stop(&mut self, partition_session_id: PartitionSessionId) -> YdbResult<()> {
        self.remove_entry(partition_session_id, "stop")?;
        self.priority_parent_sessions
            .retain(|&psid| psid != partition_session_id);
        Ok(())
    }

    pub(super) fn is_active_session(&self, partition_session_id: PartitionSessionId) -> bool {
        self.entries.contains_key(&partition_session_id)
    }

    /// Closes a session's input. If it still has buffered messages, moves it into
    /// the priority queue so they are delivered before any round-robin (child)
    /// session; an ended session with an empty queue is dropped from rotation
    /// without being enqueued, since its child is already gated by the priority
    /// queue.
    pub(super) fn end(&mut self, end_partition: RawEndPartitionSession) -> YdbResult<()> {
        let RawEndPartitionSession {
            partition_session_id,
            ..
        } = end_partition;

        let entry = self.entry_mut(partition_session_id, "end partition session")?;

        entry.close_input()?;
        let has_buffered_messages = !entry.queue.is_empty();

        self.round_robin.remove(partition_session_id);
        if has_buffered_messages {
            self.priority_parent_sessions
                .push_back(partition_session_id);
        }

        Ok(())
    }

    fn entry(
        &self,
        partition_session_id: PartitionSessionId,
        action: &str,
    ) -> YdbResult<&PartitionEntry> {
        self.entries.get(&partition_session_id).ok_or_else(|| {
            YdbError::custom(format!(
                "topic reader {action}: unknown partition session {partition_session_id}"
            ))
        })
    }

    fn entry_mut(
        &mut self,
        partition_session_id: PartitionSessionId,
        action: &str,
    ) -> YdbResult<&mut PartitionEntry> {
        self.entries.get_mut(&partition_session_id).ok_or_else(|| {
            YdbError::custom(format!(
                "topic reader {action}: unknown partition session {partition_session_id}"
            ))
        })
    }

    pub(super) fn push_raw_batch(
        &mut self,
        batch: RawBatch,
        partition_session_id: PartitionSessionId,
        reader_id: usize,
        epoch: usize,
    ) -> YdbResult<bool> {
        if batch.message_data.is_empty() {
            return Ok(false);
        }

        let batch_bytes = batch.get_read_session_size();

        let entry = self.entry_mut(partition_session_id, "push batch")?;

        if entry.is_closed() {
            return Err(YdbError::custom(format!(
                "topic reader push batch: partition session {partition_session_id} is closed"
            )));
        }

        let batch = TopicReaderBatch::new(batch, &mut entry.session, reader_id, epoch);
        let mut messages = batch.messages;
        if let Some(last) = messages.last_mut() {
            last.bytes_to_release = batch_bytes;
        }
        entry.queue.extend(messages);

        Ok(true)
    }

    pub(super) fn is_input_closed(&self, partition_session_id: PartitionSessionId) -> bool {
        self.entries
            .get(&partition_session_id)
            .is_some_and(PartitionEntry::is_closed)
    }

    #[cfg(test)]
    pub(super) fn push_batch(&mut self, messages: Vec<TopicReaderMessage>) {
        for message in messages {
            let partition_session_id = message.get_commit_marker().partition_session_id;
            if !self.entries.contains_key(&partition_session_id) {
                let session = PartitionSession::from_message(&message);
                self.start(session)
                    .expect("test message partition session should start");
            }
            let entry = self
                .entries
                .get_mut(&partition_session_id)
                .expect("test message partition session should exist");
            entry.queue.push_back(message);
            self.round_robin.push(partition_session_id);
        }
    }

    /// Serves ended (priority) sessions front-first: every ended parent is fully
    /// drained before any round-robin session runs. Returns `None` only when the
    /// priority queue is empty, so the caller can fall back to round-robin.
    pub(super) fn pop_batch(&mut self, cap: usize) -> YdbResult<Option<BufferedBatch>> {
        if cap == 0 {
            return Err(YdbError::custom(
                "topic reader pop batch: cap must be greater than zero",
            ));
        }

        if let Some(result) = self.pop_priority_batch(cap)? {
            Ok(Some(result))
        } else {
            self.pop_round_robin_batch(cap)
        }
    }

    fn pop_priority_batch(&mut self, cap: usize) -> YdbResult<Option<BufferedBatch>> {
        let Some(front_psid) = self.priority_parent_sessions.front().copied() else {
            return Ok(None);
        };

        let entry = self.entry_mut(front_psid, "priority pop batch")?;

        let Some(batch) = entry.take_batch(cap) else {
            return Err(YdbError::custom(
                "topic reader priority pop batch: empty queue",
            ));
        };

        if entry.queue.is_empty() {
            self.priority_parent_sessions.pop_front();
        }

        Ok(Some(batch))
    }

    fn pop_round_robin_batch(&mut self, cap: usize) -> YdbResult<Option<BufferedBatch>> {
        for _rr_iterations in 0..self.round_robin.len() {
            let Some(psid) = self.round_robin.next() else {
                break;
            };

            let entry = self.entry_mut(psid, "round robin poll")?;
            if let Some(batch) = entry.take_batch(cap) {
                return Ok(Some(batch));
            }
        }

        Ok(None)
    }

    fn remove_entry(
        &mut self,
        partition_session_id: PartitionSessionId,
        action: &str,
    ) -> YdbResult<PartitionEntry> {
        let entry = self.entry(partition_session_id, action)?;
        let partition_id = entry.session.partition_id;

        match self.partition_to_session.get(&partition_id) {
            Some(stored_session_id) if *stored_session_id == partition_session_id => {}
            Some(stored_session_id) => {
                return Err(YdbError::custom(format!(
                    "topic reader {action}: partition {partition_id} maps to session {stored_session_id}, not {partition_session_id}"
                )));
            }
            None => {
                return Err(YdbError::custom(format!(
                    "topic reader {action}: missing partition mapping for {partition_id}"
                )));
            }
        }

        self.partition_to_session.remove(&partition_id);
        self.round_robin.remove(partition_session_id);

        self.entries.remove(&partition_session_id).ok_or_else(|| {
            YdbError::custom(format!(
                "topic reader {action}: unknown partition session {partition_session_id}"
            ))
        })
    }

    #[cfg(test)]
    pub(super) fn replace_partition_mapping(
        &mut self,
        partition_id: PartitionId,
        session_id: PartitionSessionId,
    ) {
        self.partition_to_session.insert(partition_id, session_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc_wrapper::raw_common_types::Timestamp;
    use crate::grpc_wrapper::raw_topic_service::common::codecs::RawCodec;
    use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawMessageData;
    use std::collections::HashMap;
    use std::time::UNIX_EPOCH;
    use ydb_grpc::ydb_proto::topic::Codec;

    fn psid(value: i64) -> PartitionSessionId {
        PartitionSessionId::from_raw(value)
    }

    fn pid(value: i64) -> PartitionId {
        PartitionId::from_raw(value)
    }

    fn session(partition_session_id: i64, partition_id: i64) -> PartitionSession {
        PartitionSession {
            partition_session_id: psid(partition_session_id),
            partition_id: pid(partition_id),
            topic: String::new(),
            next_commit_offset_start: 0,
        }
    }

    fn end(
        partition_session_id: i64,
        child_partition_ids: impl IntoIterator<Item = i64>,
        adjacent_partition_ids: impl IntoIterator<Item = i64>,
    ) -> RawEndPartitionSession {
        RawEndPartitionSession {
            partition_session_id: psid(partition_session_id),
            child_partition_ids: child_partition_ids.into_iter().map(pid).collect(),
            adjacent_partition_ids: adjacent_partition_ids.into_iter().map(pid).collect(),
        }
    }

    fn raw_batch(messages: impl IntoIterator<Item = (i64, i64)>) -> RawBatch {
        RawBatch {
            producer_id: String::new(),
            write_session_meta: HashMap::new(),
            codec: RawCodec {
                code: i32::from(Codec::Raw),
            },
            written_at: Timestamp::from(UNIX_EPOCH),
            message_data: messages
                .into_iter()
                .map(|(offset, read_session_size_bytes)| RawMessageData {
                    offset,
                    seq_no: offset,
                    created_at: None,
                    uncompressed_size: 0,
                    data: Vec::new(),
                    read_session_size_bytes,
                })
                .collect(),
        }
    }

    #[test]
    fn pop_batch_round_robins_between_sessions() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 1)).unwrap();
        buffer.start(session(2, 2)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1), (1, 1)]), psid(1), 0, 0)
            .unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(2), 0, 0)
            .unwrap();

        let first = buffer.pop_batch(1).unwrap().unwrap();
        let second = buffer.pop_batch(1).unwrap().unwrap();
        let third = buffer.pop_batch(1).unwrap().unwrap();

        assert_eq!(
            first.messages[0].get_commit_marker().partition_session_id,
            psid(1)
        );
        assert_eq!(
            second.messages[0].get_commit_marker().partition_session_id,
            psid(2)
        );
        assert_eq!(
            third.messages[0].get_commit_marker().partition_session_id,
            psid(1)
        );
    }

    #[test]
    fn unknown_session_batch_returns_error() {
        let mut buffer = MessageBuffer::default();

        assert!(
            buffer
                .push_raw_batch(raw_batch([(0, 1)]), psid(1), 0, 0)
                .is_err()
        );
        assert!(buffer.pop_batch(1).unwrap().is_none());
    }

    #[test]
    fn end_empty_parent_releases_child_immediately() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 10)).unwrap();

        buffer.end(end(1, [20], [])).unwrap();

        buffer.start(session(2, 20)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(2), 0, 0)
            .unwrap();

        let batch = buffer.pop_batch(1).unwrap().unwrap();
        assert_eq!(
            batch.messages[0].get_commit_marker().partition_session_id,
            psid(2)
        );
    }

    #[test]
    fn ended_parent_releases_child_after_queue_drains() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 10)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1), (1, 1)]), psid(1), 0, 0)
            .unwrap();

        buffer.end(end(1, [20], [])).unwrap();

        buffer.start(session(2, 20)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(2), 0, 0)
            .unwrap();

        let first = buffer.pop_batch(1).unwrap().unwrap();
        assert_eq!(
            first.messages[0].get_commit_marker().partition_session_id,
            psid(1)
        );

        let second = buffer.pop_batch(1).unwrap().unwrap();
        assert_eq!(
            second.messages[0].get_commit_marker().partition_session_id,
            psid(1)
        );

        let child = buffer.pop_batch(1).unwrap().unwrap();
        assert_eq!(
            child.messages[0].get_commit_marker().partition_session_id,
            psid(2)
        );
    }

    #[test]
    fn child_started_after_parent_end_stays_blocked_until_parent_drains() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 10)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(1), 0, 0)
            .unwrap();
        buffer.end(end(1, [20], [])).unwrap();

        buffer.start(session(2, 20)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(2), 0, 0)
            .unwrap();

        let parent = buffer.pop_batch(1).unwrap().unwrap();
        assert_eq!(
            parent.messages[0].get_commit_marker().partition_session_id,
            psid(1)
        );

        let child = buffer.pop_batch(1).unwrap().unwrap();
        assert_eq!(
            child.messages[0].get_commit_marker().partition_session_id,
            psid(2)
        );
    }

    #[test]
    fn multiple_ended_parents_gate_child_by_end_order() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 10)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(1), 0, 0)
            .unwrap();
        buffer
            .push_raw_batch(raw_batch([(1, 1)]), psid(1), 0, 0)
            .unwrap();

        buffer.start(session(2, 20)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(2), 0, 0)
            .unwrap();

        buffer.end(end(1, [30], [])).unwrap();
        buffer.end(end(2, [30], [])).unwrap();

        buffer.start(session(3, 30)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(3), 0, 0)
            .unwrap();

        let first = buffer.pop_batch(1).unwrap().unwrap();
        assert_eq!(
            first.messages[0].get_commit_marker().partition_session_id,
            psid(1)
        );

        let second = buffer.pop_batch(1).unwrap().unwrap();
        assert_eq!(
            second.messages[0].get_commit_marker().partition_session_id,
            psid(1)
        );

        let third = buffer.pop_batch(1).unwrap().unwrap();
        assert_eq!(
            third.messages[0].get_commit_marker().partition_session_id,
            psid(2)
        );

        let child = buffer.pop_batch(1).unwrap().unwrap();
        assert_eq!(
            child.messages[0].get_commit_marker().partition_session_id,
            psid(3)
        );
    }

    #[test]
    fn stop_ended_parent_keeps_buffer_progressing_to_next_priority_or_rr() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 10)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(1), 0, 0)
            .unwrap();
        buffer.end(end(1, [20], [])).unwrap();

        buffer.start(session(2, 20)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(2), 0, 0)
            .unwrap();

        buffer.stop(psid(1)).unwrap();

        let child = buffer.pop_batch(1).unwrap().unwrap();
        assert_eq!(
            child.messages[0].get_commit_marker().partition_session_id,
            psid(2)
        );
    }

    #[test]
    fn closed_partition_rejects_new_messages() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 10)).unwrap();
        buffer.end(end(1, [], [])).unwrap();

        assert!(
            buffer
                .push_raw_batch(raw_batch([(0, 1)]), psid(1), 0, 0)
                .is_err()
        );
    }

    #[test]
    fn duplicate_end_returns_error() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 10)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(1), 0, 0)
            .unwrap();

        buffer.end(end(1, [20], [])).unwrap();
        assert!(buffer.end(end(1, [30], [])).is_err());
    }

    #[test]
    fn restarted_partition_id_does_not_capture_stopped_session_messages() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 10)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(1), 0, 0)
            .unwrap();
        buffer.end(end(1, [20], [])).unwrap();

        buffer.start(session(2, 20)).unwrap();
        buffer.end(end(2, [30], [])).unwrap();
        buffer.stop(psid(2)).unwrap();

        buffer.start(session(3, 30)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(3), 0, 0)
            .unwrap();

        buffer.start(session(4, 20)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(4), 0, 0)
            .unwrap();

        let parent = buffer.pop_batch(1).unwrap().unwrap();
        assert_eq!(
            parent.messages[0].get_commit_marker().partition_session_id,
            psid(1)
        );

        let child = buffer.pop_batch(1).unwrap().unwrap();
        assert_eq!(
            child.messages[0].get_commit_marker().partition_session_id,
            psid(3)
        );
    }

    #[test]
    fn end_unknown_session_returns_error() {
        let mut buffer = MessageBuffer::default();
        assert!(buffer.end(end(1, [20], [])).is_err());
    }

    #[test]
    fn stop_rr_session_removes_only_stopped_session() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 10)).unwrap();
        buffer.start(session(2, 20)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(1), 0, 0)
            .unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(2), 0, 0)
            .unwrap();
        buffer.stop(psid(1)).unwrap();

        let batch = buffer.pop_batch(1).unwrap().unwrap();
        assert_eq!(
            batch.messages[0].get_commit_marker().partition_session_id,
            psid(2)
        );
    }

    #[test]
    fn stop_unknown_session_is_error() {
        let mut buffer = MessageBuffer::default();
        assert!(buffer.stop(psid(1)).is_err());
    }

    #[test]
    fn stop_with_wrong_partition_mapping_returns_error_without_removing_entry() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 10)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(1), 0, 0)
            .unwrap();
        buffer.partition_to_session.insert(pid(10), psid(2));

        assert!(buffer.stop(psid(1)).is_err());
        assert!(buffer.is_active_session(psid(1)));
        assert_eq!(buffer.partition_to_session.get(&pid(10)), Some(&psid(2)));
    }

    #[test]
    fn pop_batch_zero_capacity_returns_error() {
        let mut buffer = MessageBuffer::default();
        assert!(buffer.pop_batch(0).is_err());
    }

    #[test]
    fn take_batch_empty_or_zero_capacity_returns_none() {
        let mut entry = PartitionEntry::new(session(1, 10));
        entry
            .queue
            .push_back(TopicReaderMessage::test_message(0, 0));

        assert!(entry.take_batch(0).is_none());
        assert_eq!(
            entry
                .take_batch(1)
                .as_ref()
                .map(|batch| batch.messages.len()),
            Some(1)
        );
        assert!(entry.take_batch(1).is_none());
    }

    #[test]
    fn drained_ended_session_stays_committable_until_stop() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 10)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(1), 0, 0)
            .unwrap();
        buffer.end(end(1, [20], [])).unwrap();

        let batch = buffer.pop_batch(1).unwrap().unwrap();
        assert_eq!(
            batch.messages[0].get_commit_marker().partition_session_id,
            psid(1)
        );

        // Queue drained, but the entry is retained so its messages stay
        // committable until the server stops the session.
        assert!(buffer.is_active_session(psid(1)));
        assert!(buffer.pop_batch(1).unwrap().is_none());

        buffer.stop(psid(1)).unwrap();
        assert!(!buffer.is_active_session(psid(1)));
    }

    #[test]
    fn empty_later_ended_parent_waits_behind_earlier_ended_parent() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 10)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(1), 0, 0)
            .unwrap();
        buffer.start(session(2, 20)).unwrap();
        buffer.start(session(3, 30)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(3), 0, 0)
            .unwrap();

        // Merge of partitions 10 and 20 into 30. Parent A (session 1) still has a
        // buffered message; parent B (session 2) ends empty behind it.
        buffer.end(end(1, [30], [20])).unwrap();
        buffer.end(end(2, [30], [10])).unwrap();

        let first = buffer.pop_batch(1).unwrap().unwrap();
        assert_eq!(
            first.messages[0].get_commit_marker().partition_session_id,
            psid(1)
        );

        // Empty parent B is skipped; only now is the child served.
        let child = buffer.pop_batch(1).unwrap().unwrap();
        assert_eq!(
            child.messages[0].get_commit_marker().partition_session_id,
            psid(3)
        );

        assert!(buffer.pop_batch(1).unwrap().is_none());
    }
}
