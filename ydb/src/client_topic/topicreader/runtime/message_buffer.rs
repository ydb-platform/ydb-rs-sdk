use std::collections::{HashMap, VecDeque};

use crate::client_topic::topicreader::ids::{PartitionId, PartitionSessionId};
use crate::client_topic::topicreader::messages::{TopicReaderBatch, TopicReaderMessage};
use crate::client_topic::topicreader::partition_state::PartitionSession;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawBatch;
use crate::{YdbError, YdbResult};

use super::round_robin::RoundRobin;

pub(super) struct BufferedBatch {
    pub(super) messages: Vec<TopicReaderMessage>,
    pub(super) bytes_to_release: i64,
    pub(super) epoch: usize,
}

#[derive(Default)]
pub(super) struct BufferActions {
    pub(super) messages_became_available: bool,
}

impl BufferActions {
    pub(super) fn merge(&mut self, other: Self) {
        self.messages_became_available |= other.messages_became_available;
    }
}

pub(super) struct PopBatchResult {
    pub(super) batch: Option<BufferedBatch>,
    pub(super) actions: BufferActions,
}

enum InputState {
    Open,
    Closed(ClosedPlan),
}

struct ClosedPlan {
    release_children_on_drained: Vec<PartitionId>,
}

struct PartitionEntry {
    session: PartitionSession,
    queue: VecDeque<TopicReaderMessage>,
    input: InputState,
    blocked_by: usize,
}

impl PartitionEntry {
    fn new(session: PartitionSession, blocked_by: usize) -> Self {
        Self {
            session,
            queue: VecDeque::new(),
            input: InputState::Open,
            blocked_by,
        }
    }

    fn can_accept_messages(&self) -> bool {
        matches!(self.input, InputState::Open)
    }

    fn close_input(
        &mut self,
        psid: PartitionSessionId,
        release_children_on_drained: Vec<PartitionId>,
    ) -> YdbResult<()> {
        match self.input {
            InputState::Open => {
                self.input = InputState::Closed(ClosedPlan {
                    release_children_on_drained,
                });
                Ok(())
            }
            InputState::Closed(_) => Err(YdbError::custom(format!(
                "topic reader duplicate close for partition session {psid}"
            ))),
        }
    }

    fn take_children_to_release_on_drained(&mut self) -> Vec<PartitionId> {
        match &mut self.input {
            InputState::Open => Vec::new(),
            InputState::Closed(plan) => std::mem::take(&mut plan.release_children_on_drained),
        }
    }

    fn input_is_closed(&self) -> bool {
        matches!(self.input, InputState::Closed(_))
    }
}

#[derive(Default)]
pub(super) struct MessageBuffer {
    entries: HashMap<PartitionSessionId, PartitionEntry>,
    partition_to_session: HashMap<PartitionId, PartitionSessionId>,
    pending_child_blocks: HashMap<PartitionId, usize>,
    round_robin: RoundRobin,
}

impl MessageBuffer {
    pub(super) fn start(&mut self, session: PartitionSession) -> YdbResult<()> {
        let psid = session.partition_session_id;
        let pid = session.partition_id;
        if let Some(existing) = self.partition_to_session.get(&pid) {
            return Err(YdbError::custom(format!(
                "topic reader duplicate start for partition {pid}: new session {psid}, existing session {existing}"
            )));
        }
        if self.entries.contains_key(&psid) {
            return Err(YdbError::custom(format!(
                "topic reader duplicate start partition session {psid}"
            )));
        }

        let blocked_by = self.pending_child_blocks.remove(&pid).unwrap_or(0);
        if blocked_by == 0 {
            self.round_robin.push(psid);
        }
        self.partition_to_session.insert(pid, psid);
        self.entries
            .insert(psid, PartitionEntry::new(session, blocked_by));
        Ok(())
    }

    pub(super) fn stop(
        &mut self,
        partition_session_id: PartitionSessionId,
    ) -> YdbResult<Option<BufferActions>> {
        self.round_robin.remove(partition_session_id);
        let Some(mut entry) = self.entries.remove(&partition_session_id) else {
            return Ok(None);
        };
        self.remove_partition_mapping(partition_session_id, entry.session.partition_id, "stop")?;
        let children = entry.take_children_to_release_on_drained();
        Ok(Some(
            self.release_child_blocks(partition_session_id, children)?,
        ))
    }

    pub(super) fn is_active_session(&self, partition_session_id: PartitionSessionId) -> bool {
        self.entries.contains_key(&partition_session_id)
    }

    pub(super) fn end(
        &mut self,
        partition_session_id: PartitionSessionId,
        child_partition_ids: Vec<PartitionId>,
    ) -> YdbResult<BufferActions> {
        let queue_is_empty = {
            let entry = self.entry_mut(partition_session_id, "close input")?;
            let queue_is_empty = entry.queue.is_empty();
            entry.close_input(partition_session_id, child_partition_ids.clone())?;
            queue_is_empty
        };

        let mut actions = BufferActions::default();
        for child_partition_id in child_partition_ids {
            self.register_child_block(partition_session_id, child_partition_id)?;
        }

        if queue_is_empty {
            actions.merge(self.on_partition_drained(partition_session_id)?);
        }
        Ok(actions)
    }

    pub(super) fn push_raw_batch(
        &mut self,
        batch: RawBatch,
        partition_session_id: PartitionSessionId,
        reader_id: usize,
        epoch: usize,
    ) -> YdbResult<Option<BufferActions>> {
        if batch.message_data.is_empty() {
            return Ok(Some(BufferActions::default()));
        }

        let batch_bytes = batch.get_read_session_size();
        let Some(entry) = self.entries.get_mut(&partition_session_id) else {
            return Ok(None);
        };
        if !entry.can_accept_messages() {
            return Err(YdbError::custom(format!(
                "topic reader received messages for closed partition session {partition_session_id}"
            )));
        }
        let messages_became_available = entry.blocked_by == 0;

        let batch = TopicReaderBatch::new(batch, &mut entry.session, reader_id, epoch);
        let mut messages = batch.messages;
        if let Some(last) = messages.last_mut() {
            last.bytes_to_release = batch_bytes;
        }
        entry.queue.extend(messages);
        Ok(Some(BufferActions {
            messages_became_available,
        }))
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
            if entry.blocked_by == 0 {
                self.round_robin.push(partition_session_id);
            }
        }
    }

    pub(super) fn pop_batch(&mut self, cap: usize) -> YdbResult<PopBatchResult> {
        for _ in 0..self.round_robin.len() {
            let Some(psid) = self.round_robin.next() else {
                return Ok(PopBatchResult {
                    batch: None,
                    actions: BufferActions::default(),
                });
            };
            let Some(entry) = self.entries.get_mut(&psid) else {
                return Err(YdbError::custom(format!(
                    "topic reader round robin contains unknown partition session {psid}"
                )));
            };

            if entry.queue.is_empty() {
                continue;
            }

            let take = cap.min(entry.queue.len());
            let mut out = Vec::with_capacity(take);
            let mut bytes = 0;
            for _ in 0..take {
                let Some(message) = entry.queue.pop_front() else {
                    break;
                };
                bytes += message.bytes_to_release;
                out.push(message);
            }

            let epoch = out
                .first()
                .ok_or_else(|| YdbError::custom("topic reader produced empty buffered batch"))?
                .get_commit_marker()
                .epoch;

            let drained = entry.queue.is_empty() && entry.input_is_closed();
            let actions = if drained {
                self.on_partition_drained(psid)?
            } else {
                BufferActions::default()
            };

            return Ok(PopBatchResult {
                batch: Some(BufferedBatch {
                    messages: out,
                    bytes_to_release: bytes,
                    epoch,
                }),
                actions,
            });
        }

        Ok(PopBatchResult {
            batch: None,
            actions: BufferActions::default(),
        })
    }

    fn entry_mut(
        &mut self,
        partition_session_id: PartitionSessionId,
        action: &str,
    ) -> YdbResult<&mut PartitionEntry> {
        self.entries.get_mut(&partition_session_id).ok_or_else(|| {
            YdbError::custom(format!(
                "topic reader {action} for unknown partition session {partition_session_id}"
            ))
        })
    }

    fn register_child_block(
        &mut self,
        parent_session_id: PartitionSessionId,
        child_partition_id: PartitionId,
    ) -> YdbResult<()> {
        let Some(&child_session_id) = self.partition_to_session.get(&child_partition_id) else {
            let count = self
                .pending_child_blocks
                .entry(child_partition_id)
                .or_insert(0);
            *count = count.checked_add(1).ok_or_else(|| {
                YdbError::custom(format!(
                    "topic reader child partition {child_partition_id} block count overflow for parent {parent_session_id}"
                ))
            })?;
            return Ok(());
        };

        let child_entry = self.entries.get_mut(&child_session_id).ok_or_else(|| {
            YdbError::custom(format!(
                "topic reader child partition {child_partition_id} maps to missing session {child_session_id}"
            ))
        })?;
        child_entry.blocked_by = child_entry.blocked_by.checked_add(1).ok_or_else(|| {
            YdbError::custom(format!(
                "topic reader child session {child_session_id} block count overflow for parent {parent_session_id}"
            ))
        })?;
        self.round_robin.remove(child_session_id);
        Ok(())
    }

    fn on_partition_drained(
        &mut self,
        partition_session_id: PartitionSessionId,
    ) -> YdbResult<BufferActions> {
        self.round_robin.remove(partition_session_id);
        let children = self
            .entry_mut(partition_session_id, "drain")?
            .take_children_to_release_on_drained();
        self.release_child_blocks(partition_session_id, children)
    }

    fn release_child_blocks(
        &mut self,
        parent_session_id: PartitionSessionId,
        child_partition_ids: Vec<PartitionId>,
    ) -> YdbResult<BufferActions> {
        let mut actions = BufferActions::default();
        for child_partition_id in child_partition_ids {
            if let Some(&child_session_id) = self.partition_to_session.get(&child_partition_id) {
                let child_entry = self.entries.get_mut(&child_session_id).ok_or_else(|| {
                    YdbError::custom(format!(
                        "topic reader child partition {child_partition_id} maps to missing session {child_session_id}"
                    ))
                })?;
                child_entry.blocked_by = child_entry.blocked_by.checked_sub(1).ok_or_else(|| {
                    YdbError::custom(format!(
                        "topic reader child session {child_session_id} block count underflow for parent {parent_session_id}"
                    ))
                })?;
                if child_entry.blocked_by == 0 {
                    self.round_robin.push(child_session_id);
                    actions.messages_became_available |= !child_entry.queue.is_empty();
                }
                continue;
            }

            let Some(count) = self.pending_child_blocks.get_mut(&child_partition_id) else {
                continue;
            };
            *count = count.checked_sub(1).ok_or_else(|| {
                YdbError::custom(format!(
                    "topic reader pending child partition {child_partition_id} block count underflow for parent {parent_session_id}"
                ))
            })?;
            if *count == 0 {
                self.pending_child_blocks.remove(&child_partition_id);
            }
        }

        Ok(actions)
    }

    fn remove_partition_mapping(
        &mut self,
        partition_session_id: PartitionSessionId,
        partition_id: PartitionId,
        action: &str,
    ) -> YdbResult<()> {
        match self.partition_to_session.remove(&partition_id) {
            Some(mapped) if mapped == partition_session_id => Ok(()),
            Some(mapped) => Err(YdbError::custom(format!(
                "topic reader {action} partition session {partition_session_id} for partition {partition_id}, but partition belongs to session {mapped}"
            ))),
            None => Err(YdbError::custom(format!(
                "topic reader {action} partition session {partition_session_id} for unknown partition {partition_id}"
            ))),
        }
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

        let first = buffer.pop_batch(1).unwrap().batch.unwrap();
        let second = buffer.pop_batch(1).unwrap().batch.unwrap();
        let third = buffer.pop_batch(1).unwrap().batch.unwrap();

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
    fn unknown_session_batch_is_ignored() {
        let mut buffer = MessageBuffer::default();

        assert!(buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(1), 0, 0)
            .unwrap()
            .is_none());
        assert!(buffer.pop_batch(1).unwrap().batch.is_none());
    }

    #[test]
    fn end_empty_parent_releases_child_immediately() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 10)).unwrap();
        buffer.start(session(2, 20)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(2), 0, 0)
            .unwrap();

        let actions = buffer.end(psid(1), vec![pid(20)]).unwrap();
        assert!(actions.messages_became_available);

        let batch = buffer.pop_batch(1).unwrap().batch.unwrap();
        assert_eq!(
            batch.messages[0].get_commit_marker().partition_session_id,
            psid(2)
        );
    }

    #[test]
    fn ended_parent_releases_child_after_queue_drains() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 10)).unwrap();
        buffer.start(session(2, 20)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1), (1, 1)]), psid(1), 0, 0)
            .unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(2), 0, 0)
            .unwrap();

        let actions = buffer.end(psid(1), vec![pid(20)]).unwrap();
        assert!(!actions.messages_became_available);

        let first = buffer.pop_batch(1).unwrap();
        assert_eq!(
            first.batch.unwrap().messages[0]
                .get_commit_marker()
                .partition_session_id,
            psid(1)
        );

        let second = buffer.pop_batch(1).unwrap();
        assert!(second.actions.messages_became_available);
        assert_eq!(
            second.batch.unwrap().messages[0]
                .get_commit_marker()
                .partition_session_id,
            psid(1)
        );

        let child = buffer.pop_batch(1).unwrap().batch.unwrap();
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
        buffer.end(psid(1), vec![pid(20)]).unwrap();

        buffer.start(session(2, 20)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(2), 0, 0)
            .unwrap();

        let parent = buffer.pop_batch(1).unwrap();
        assert!(parent.actions.messages_became_available);
        assert_eq!(
            parent.batch.unwrap().messages[0]
                .get_commit_marker()
                .partition_session_id,
            psid(1)
        );

        let child = buffer.pop_batch(1).unwrap().batch.unwrap();
        assert_eq!(
            child.messages[0].get_commit_marker().partition_session_id,
            psid(2)
        );
    }

    #[test]
    fn merge_child_waits_for_all_parent_queues_to_drain() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 10)).unwrap();
        buffer.start(session(2, 20)).unwrap();
        buffer.start(session(3, 30)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(1), 0, 0)
            .unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(2), 0, 0)
            .unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(3), 0, 0)
            .unwrap();

        buffer.end(psid(1), vec![pid(30)]).unwrap();
        buffer.end(psid(2), vec![pid(30)]).unwrap();

        let first_parent = buffer.pop_batch(1).unwrap();
        assert!(!first_parent.actions.messages_became_available);
        assert_ne!(
            first_parent.batch.unwrap().messages[0]
                .get_commit_marker()
                .partition_session_id,
            psid(3)
        );

        let second_parent = buffer.pop_batch(1).unwrap();
        assert!(second_parent.actions.messages_became_available);
        assert_ne!(
            second_parent.batch.unwrap().messages[0]
                .get_commit_marker()
                .partition_session_id,
            psid(3)
        );

        let child = buffer.pop_batch(1).unwrap().batch.unwrap();
        assert_eq!(
            child.messages[0].get_commit_marker().partition_session_id,
            psid(3)
        );
    }

    #[test]
    fn closed_partition_rejects_new_messages() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 10)).unwrap();
        buffer.end(psid(1), Vec::new()).unwrap();

        assert!(buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(1), 0, 0)
            .is_err());
    }

    #[test]
    fn stop_closed_parent_releases_child_blocks_once() {
        let mut buffer = MessageBuffer::default();
        buffer.start(session(1, 10)).unwrap();
        buffer.start(session(2, 20)).unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(1), 0, 0)
            .unwrap();
        buffer
            .push_raw_batch(raw_batch([(0, 1)]), psid(2), 0, 0)
            .unwrap();
        buffer.end(psid(1), vec![pid(20)]).unwrap();

        let actions = buffer.stop(psid(1)).unwrap().unwrap();
        assert!(actions.messages_became_available);
        assert!(buffer.stop(psid(1)).unwrap().is_none());

        let child = buffer.pop_batch(1).unwrap().batch.unwrap();
        assert_eq!(
            child.messages[0].get_commit_marker().partition_session_id,
            psid(2)
        );
    }
}
