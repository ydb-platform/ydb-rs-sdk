use std::collections::{HashMap, VecDeque};

use crate::client_topic::topicreader::ids::{PartitionId, PartitionSessionId};
use crate::client_topic::topicreader::messages::{TopicReaderBatch, TopicReaderMessage};
use crate::client_topic::topicreader::partition_state::PartitionSession;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawBatch;
use crate::{YdbError, YdbResult};
use itertools::Itertools;

use super::round_robin::RoundRobin;

pub(super) struct BufferedBatch {
    pub(super) messages: Vec<TopicReaderMessage>,
    pub(super) bytes_to_release: i64,
    pub(super) epoch: usize,
}

enum PartitionLifecycle {
    Reading,
    Ending {
        children_to_unblock: Option<Vec<PartitionId>>,
    },
}

struct PartitionEntry {
    session: PartitionSession,
    queue: VecDeque<TopicReaderMessage>,
    /// Number of parent sessions that must end and drain before this partition can be read.
    blocked_by: usize,
    lifecycle: PartitionLifecycle,
    /// Highest `committed_offset` received from the server for this session. Initialized to the
    /// session's starting committed offset so that sessions with no messages are always "acked".
    last_acked_offset: i64,
}

struct ParentBlockRelease {
    unblocked: bool,
    wake_reader: bool,
}

impl PartitionEntry {
    fn new(session: PartitionSession, blocked_by: usize) -> Self {
        let last_acked_offset = session.next_commit_offset_start;
        Self {
            session,
            queue: VecDeque::new(),
            blocked_by,
            lifecycle: PartitionLifecycle::Reading,
            last_acked_offset,
        }
    }

    fn terminal_offset(&self) -> i64 {
        self.session.next_commit_offset_start
    }

    fn is_ending(&self) -> bool {
        matches!(&self.lifecycle, PartitionLifecycle::Ending { .. })
    }

    fn can_accept_messages(&self) -> bool {
        matches!(&self.lifecycle, PartitionLifecycle::Reading)
    }

    fn begin_ending(
        &mut self,
        psid: PartitionSessionId,
        child_pids: Vec<PartitionId>,
    ) -> YdbResult<bool> {
        match &self.lifecycle {
            PartitionLifecycle::Reading => {
                let queue_empty = self.queue.is_empty();
                self.lifecycle = PartitionLifecycle::Ending {
                    children_to_unblock: Some(child_pids),
                };
                Ok(queue_empty)
            }
            PartitionLifecycle::Ending { .. } => Err(YdbError::custom(format!(
                "topic reader duplicate end partition session {psid}"
            ))),
        }
    }

    fn take_children_to_unblock(&mut self) -> Option<Vec<PartitionId>> {
        match &mut self.lifecycle {
            PartitionLifecycle::Reading => None,
            PartitionLifecycle::Ending {
                children_to_unblock,
            } => children_to_unblock.take(),
        }
    }

    fn observe_ack(&mut self, committed_offset: i64) {
        if committed_offset > self.last_acked_offset {
            self.last_acked_offset = committed_offset;
        }
    }

    fn stopped_before_terminal_commit(&self, committed_offset: i64) -> bool {
        self.is_ending() && committed_offset < self.terminal_offset()
    }

    fn can_close(&self) -> bool {
        self.queue.is_empty()
            && self.last_acked_offset >= self.terminal_offset()
            && matches!(
                &self.lifecycle,
                PartitionLifecycle::Ending {
                    children_to_unblock: None,
                }
            )
    }

    fn release_parent_block(
        &mut self,
        child_psid: PartitionSessionId,
        child_pid: PartitionId,
        parent_psid: PartitionSessionId,
    ) -> YdbResult<ParentBlockRelease> {
        self.blocked_by = self.blocked_by.checked_sub(1).ok_or_else(|| {
            YdbError::custom(format!(
                "topic reader child session {child_psid} (partition {child_pid}) block count underflow when parent {parent_psid} finished"
            ))
        })?;

        let unblocked = self.blocked_by == 0;
        Ok(ParentBlockRelease {
            unblocked,
            wake_reader: unblocked && !self.queue.is_empty(),
        })
    }

    fn add_parent_block(
        &mut self,
        child_psid: PartitionSessionId,
        child_pid: PartitionId,
        parent_psid: PartitionSessionId,
    ) -> YdbResult<()> {
        if self.blocked_by == 0 {
            return Err(YdbError::Transport(format!(
                "partition session {parent_psid} ended after child partition {child_pid} session {child_psid} became readable, reconnecting"
            )));
        }

        self.blocked_by = self.blocked_by.checked_add(1).ok_or_else(|| {
            YdbError::custom(format!(
                "topic reader child session {child_psid} (partition {child_pid}) block count overflow when parent {parent_psid} ended"
            ))
        })?;
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct PartitionActions {
    /// A state transition made at least one queued partition readable.
    pub(super) wake_reader: bool,
}

impl PartitionActions {
    pub(super) fn merge(&mut self, other: Self) {
        self.wake_reader |= other.wake_reader;
    }
}

/// Unified per-connection state for all partition sessions.
///
/// Owns the YDB session metadata, per-partition message queues, the round-robin schedule,
/// and the parent→child blocking relationships. All partition lifecycle events go through here.
///
/// # Child-readability invariant
///
/// A child partition enters the round-robin after every declaring parent has ended and all
/// locally buffered parent messages have been delivered to the user. The parent session itself
/// remains in `entries` until the terminal commit offset is observed, so the user can still
/// commit the last delivered batch and get a clear error after the partition is fully closed.
///
/// Three valid paths to child unblocking:
/// 1. Normal: `End` received → queue drains.
/// 2. Safe stop: `StopPartitionSessionRequest` for an ended parent releases any remaining blocks.
/// 3. Reconnect: stale dependency graphs are discarded; the server re-assigns partitions
///    from committed state on the new stream.
#[derive(Default)]
pub(super) struct PartitionSessions {
    entries: HashMap<PartitionSessionId, PartitionEntry>,

    /// Maps partition_id → session_id. Needed to find a child session when a parent finishes.
    partition_to_session: HashMap<PartitionId, PartitionSessionId>,

    /// Maps child partition_id → pending block count for children that haven't started yet.
    /// Once a child starts, the count moves into its `PartitionEntry::blocked_by`.
    pending_child_blocks: HashMap<PartitionId, usize>,

    /// Round-robin schedule over partition sessions that are readable.
    /// Blocked child sessions are excluded until all parent sessions have ended and drained.
    round_robin: RoundRobin,
}

impl PartitionSessions {
    /// Registers a new partition session. If the partition ID is registered as a pending
    /// child block, the session starts blocked and is kept out of the
    /// round-robin until its parent sessions have ended and drained.
    pub(super) fn start(&mut self, session: PartitionSession) -> YdbResult<()> {
        let psid = session.partition_session_id;
        let pid = session.partition_id;

        if let Some(existing) = self.partition_to_session.get(&pid) {
            return Err(YdbError::custom(format!(
                "topic reader duplicate start for partition {pid}: new session {psid}, existing session {existing}",
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

    /// Removes the partition session, releasing any child blocks it held.
    ///
    /// `committed_offset` is the server-reported acked offset at stop time. For an ending
    /// partition, a stop before the terminal offset is observed drops the current stream.
    pub(super) fn stop(
        &mut self,
        psid: PartitionSessionId,
        committed_offset: i64,
    ) -> YdbResult<PartitionActions> {
        if self
            .entry_mut(psid, "stop")?
            .stopped_before_terminal_commit(committed_offset)
        {
            return Err(YdbError::Transport(format!(
                "partition session {psid} stopped before terminal offset committed, reconnecting"
            )));
        }

        let actions = self.release_children_if_ending(psid)?;
        self.remove_entry(psid, "stop")?;

        Ok(actions)
    }

    fn entry_mut(
        &mut self,
        psid: PartitionSessionId,
        action: &str,
    ) -> YdbResult<&mut PartitionEntry> {
        self.entries.get_mut(&psid).ok_or_else(|| {
            YdbError::custom(format!(
                "topic reader {action} for unknown partition session {psid}"
            ))
        })
    }

    fn remove_entry(
        &mut self,
        psid: PartitionSessionId,
        action: &str,
    ) -> YdbResult<PartitionEntry> {
        self.round_robin.remove(psid);
        let Some(entry) = self.entries.remove(&psid) else {
            return Err(YdbError::custom(format!(
                "topic reader {action} for unknown partition session {psid}"
            )));
        };
        let pid = entry.session.partition_id;

        match self.partition_to_session.remove(&pid) {
            Some(mapped) if mapped == psid => Ok(entry),
            Some(mapped) => Err(YdbError::custom(format!(
                "topic reader {action} partition session {psid} for partition {pid}, \
                 but partition belongs to session {mapped}"
            ))),
            None => Err(YdbError::custom(format!(
                "topic reader {action} partition session {psid} for unknown partition {pid}"
            ))),
        }
    }

    fn release_child_blocks(
        &mut self,
        psid: PartitionSessionId,
        children_to_unblock: Vec<PartitionId>,
    ) -> YdbResult<PartitionActions> {
        let mut actions = PartitionActions::default();
        for child_pid in children_to_unblock {
            if let Some(&child_psid) = self.partition_to_session.get(&child_pid) {
                let Some(child_entry) = self.entries.get_mut(&child_psid) else {
                    return Err(YdbError::custom(format!(
                        "topic reader child session {child_psid} (partition {child_pid}) has no entry"
                    )));
                };

                let release = child_entry.release_parent_block(child_psid, child_pid, psid)?;
                if release.unblocked {
                    self.round_robin.push(child_psid);
                }
                actions.wake_reader |= release.wake_reader;
            } else {
                // Child has not started yet: decrement pending block count.
                if let Some(count) = self.pending_child_blocks.get_mut(&child_pid) {
                    *count = count.checked_sub(1).ok_or_else(|| {
                        YdbError::custom(format!(
                            "topic reader pending child partition {child_pid} block count underflow when parent {psid} finished"
                        ))
                    })?;
                    if *count == 0 {
                        self.pending_child_blocks.remove(&child_pid);
                    }
                }
                // If neither map has the child, it was already stopped — ignore.
            }
        }

        Ok(actions)
    }

    fn release_children_if_ending(
        &mut self,
        psid: PartitionSessionId,
    ) -> YdbResult<PartitionActions> {
        let Some(children_to_unblock) = self
            .entry_mut(psid, "release children")?
            .take_children_to_unblock()
        else {
            return Ok(PartitionActions::default());
        };

        self.release_child_blocks(psid, children_to_unblock)
    }

    fn register_child_block(
        &mut self,
        parent_psid: PartitionSessionId,
        child_pid: PartitionId,
    ) -> YdbResult<PartitionActions> {
        let Some(&child_psid) = self.partition_to_session.get(&child_pid) else {
            *self.pending_child_blocks.entry(child_pid).or_insert(0) += 1;
            return Ok(PartitionActions::default());
        };

        let Some(child_entry) = self.entries.get_mut(&child_psid) else {
            return Err(YdbError::custom(format!(
                "topic reader child session {child_psid} (partition {child_pid}) has no entry"
            )));
        };

        child_entry.add_parent_block(child_psid, child_pid, parent_psid)?;
        self.round_robin.remove(child_psid);
        Ok(PartitionActions::default())
    }

    fn remove_if_fully_closed(&mut self, psid: PartitionSessionId) -> YdbResult<()> {
        let Some(entry) = self.entries.get(&psid) else {
            return Ok(());
        };

        if entry.can_close() {
            self.remove_entry(psid, "close ending partition")?;
        }
        Ok(())
    }

    fn on_parent_drained(&mut self, psid: PartitionSessionId) -> YdbResult<PartitionActions> {
        self.round_robin.remove(psid);
        let actions = self.release_children_if_ending(psid)?;
        self.remove_if_fully_closed(psid)?;
        Ok(actions)
    }

    /// Called on every `CommitOffsetResponse` entry. Tracks the ack watermark for live entries
    /// and closes an ended, drained parent once its terminal offset is observed.
    pub(super) fn observe_commit_ack(
        &mut self,
        psid: PartitionSessionId,
        committed_offset: i64,
    ) -> YdbResult<()> {
        if let Some(entry) = self.entries.get_mut(&psid) {
            entry.observe_ack(committed_offset);
        }
        self.remove_if_fully_closed(psid)?;
        Ok(())
    }

    /// Records that the parent session is ending and registers its child partitions.
    ///
    /// If the parent queue is empty, child blocks are released immediately. The parent entry
    /// remains alive until the terminal offset is committed and observed. If the parent is
    /// already in `Ending` state, that is a protocol error.
    pub(super) fn end(
        &mut self,
        psid: PartitionSessionId,
        child_pids: Vec<PartitionId>,
    ) -> YdbResult<PartitionActions> {
        let queue_empty = self
            .entry_mut(psid, "end")?
            .begin_ending(psid, child_pids.clone())?;

        let mut actions = PartitionActions::default();

        // Register child blocks after the ending transition succeeds.
        for &pid in &child_pids {
            actions.merge(self.register_child_block(psid, pid)?);
        }

        if queue_empty {
            actions.merge(self.on_parent_drained(psid)?);
        }
        Ok(actions)
    }

    /// Builds messages from a raw decompressed batch and enqueues them.
    /// Returns whether the newly added messages are readable now.
    pub(super) fn push_raw_batch(
        &mut self,
        batch: RawBatch,
        psid: PartitionSessionId,
        reader_id: usize,
        epoch: usize,
    ) -> YdbResult<PartitionActions> {
        if batch.message_data.is_empty() {
            return Ok(PartitionActions::default());
        }

        let batch_bytes = batch.get_read_session_size();

        let Some(entry) = self.entries.get_mut(&psid) else {
            return Err(YdbError::custom(format!(
                "topic reader push batch: session {psid} already stopped"
            )));
        };

        if !entry.can_accept_messages() {
            return Err(YdbError::custom(format!(
                "topic reader received messages for ended partition session {psid}"
            )));
        }

        let wake_reader = entry.blocked_by == 0;
        let tb = TopicReaderBatch::new(batch, &mut entry.session, reader_id, epoch);
        let mut messages = tb.messages;
        if let Some(last) = messages.last_mut() {
            last.bytes_to_release = batch_bytes;
        }
        entry.queue.extend(messages);

        Ok(PartitionActions { wake_reader })
    }

    pub(super) fn pop_batch(&mut self, cap: usize) -> YdbResult<Option<BufferedBatch>> {
        for _ in 0..self.round_robin.len() {
            let Some(psid) = self.round_robin.next() else {
                return Ok(None);
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
            let out = entry.queue.drain(..take).collect_vec();
            let epoch = out[0].commit_marker.epoch;
            let bytes: i64 = out.iter().map(|m| m.bytes_to_release).sum();

            let parent_drained = entry.queue.is_empty() && entry.is_ending();

            if parent_drained {
                self.on_parent_drained(psid)?;
            }

            return Ok(Some(BufferedBatch {
                messages: out,
                bytes_to_release: bytes,
                epoch,
            }));
        }

        Ok(None)
    }

    pub(super) fn has_session(&self, psid: PartitionSessionId) -> bool {
        self.entries.contains_key(&psid)
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

    fn session(psid_val: i64, pid_val: i64) -> PartitionSession {
        PartitionSession {
            partition_session_id: psid(psid_val),
            partition_id: pid(pid_val),
            topic: String::new(),
            next_commit_offset_start: 0,
        }
    }

    fn push_messages(
        ps: &mut PartitionSessions,
        session_id: i64,
        messages: impl IntoIterator<Item = (i64, i64)>,
    ) {
        ps.push_raw_batch(raw_batch(messages), psid(session_id), 0, 0)
            .unwrap();
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
    fn push_routes_by_session() {
        let mut ps = PartitionSessions::default();
        ps.start(session(1, 10)).unwrap();
        ps.start(session(2, 20)).unwrap();
        push_messages(&mut ps, 1, [(0, 0), (1, 0)]);
        push_messages(&mut ps, 2, [(0, 0)]);

        let b0 = ps.pop_batch(1).unwrap().unwrap();
        assert_eq!(b0.messages[0].commit_marker.partition_session_id, psid(1));

        let b1 = ps.pop_batch(1).unwrap().unwrap();
        assert_eq!(b1.messages[0].commit_marker.partition_session_id, psid(2));

        let b2 = ps.pop_batch(1).unwrap().unwrap();
        assert_eq!(b2.messages[0].commit_marker.partition_session_id, psid(1));
    }

    #[test]
    fn merge_child_blocked_until_both_parents_drain() {
        let mut ps = PartitionSessions::default();

        // Parent 1 (session 1, partition 10): 5 messages, terminal offset = 5.
        // Parent 2 (session 2, partition 20): 1 message, terminal offset = 1.
        // Both declare partition 30 as a child.
        ps.start(session(1, 10)).unwrap();
        ps.start(session(2, 20)).unwrap();
        push_messages(&mut ps, 1, [(0, 0), (1, 0), (2, 0), (3, 0), (4, 0)]);
        push_messages(&mut ps, 2, [(0, 0)]);
        ps.end(psid(1), vec![pid(30)]).unwrap();
        ps.end(psid(2), vec![pid(30)]).unwrap();

        // Child (session 3, partition 30): 2 messages.
        ps.start(session(3, 30)).unwrap();
        push_messages(&mut ps, 3, [(0, 0), (1, 0)]);

        assert!(
            !ps.round_robin.contains(psid(3)),
            "child must be blocked before either parent drains"
        );

        // Drain all 6 parent messages two-at-a-time; child must stay blocked throughout.
        let mut parent_msgs_seen = 0;
        let mut pops = 0;
        loop {
            assert!(
                !ps.round_robin.contains(psid(3)),
                "child must stay blocked while parents have messages"
            );
            let b = ps.pop_batch(2).unwrap().unwrap();
            assert_ne!(
                b.messages[0].commit_marker.partition_session_id,
                psid(3),
                "child must not be served before both parents drain"
            );
            assert!(b.messages.len() <= 2, "cap=2 must be respected");
            parent_msgs_seen += b.messages.len();
            pops += 1;
            if parent_msgs_seen == 6 {
                break;
            }
        }

        // ceil(5/2) + ceil(1/2) = 3 + 1 = 4 pops.
        assert_eq!(pops, 4);

        assert!(
            ps.round_robin.contains(psid(3)),
            "child must unblock after both parents drain"
        );

        let b = ps.pop_batch(2).unwrap().unwrap();
        assert_eq!(b.messages.len(), 2);
        assert_eq!(b.messages[0].commit_marker.partition_session_id, psid(3));
        assert!(
            ps.pop_batch(2).unwrap().is_none(),
            "buffer must be empty after child drains"
        );
    }

    #[test]
    fn later_parent_end_adds_block_to_existing_blocked_child() {
        let mut ps = PartitionSessions::default();

        ps.start(session(1, 10)).unwrap();
        ps.start(session(2, 20)).unwrap();
        push_messages(&mut ps, 1, [(0, 0)]);
        push_messages(&mut ps, 2, [(0, 0)]);

        ps.end(psid(1), vec![pid(30)]).unwrap();
        ps.start(session(3, 30)).unwrap();
        push_messages(&mut ps, 3, [(0, 0)]);

        ps.end(psid(2), vec![pid(30)]).unwrap();
        assert!(
            !ps.round_robin.contains(psid(3)),
            "child must still be blocked after later parent declares it"
        );

        let first_parent = ps.pop_batch(10).unwrap().unwrap();
        assert_ne!(
            first_parent.messages[0].commit_marker.partition_session_id,
            psid(3)
        );
        assert!(
            !ps.round_robin.contains(psid(3)),
            "child must stay blocked until both parents drain"
        );

        let second_parent = ps.pop_batch(10).unwrap().unwrap();
        assert_ne!(
            second_parent.messages[0].commit_marker.partition_session_id,
            psid(3)
        );
        assert!(
            ps.round_robin.contains(psid(3)),
            "child must unblock after the later parent drains"
        );

        let child = ps.pop_batch(10).unwrap().unwrap();
        assert_eq!(
            child.messages[0].commit_marker.partition_session_id,
            psid(3)
        );
    }

    #[test]
    fn later_parent_end_for_readable_child_requires_reconnect() {
        let mut ps = PartitionSessions::default();

        ps.start(session(1, 10)).unwrap();
        ps.start(session(2, 20)).unwrap();
        push_messages(&mut ps, 2, [(0, 0)]);
        assert!(
            ps.round_robin.contains(psid(2)),
            "child is readable before the late dependency appears"
        );

        assert!(ps.end(psid(1), vec![pid(20)]).is_err());
    }

    #[test]
    fn bytes_to_release_accumulated() {
        let mut ps = PartitionSessions::default();
        ps.start(session(1, 10)).unwrap();
        push_messages(&mut ps, 1, [(0, 0), (1, 100)]);
        let b = ps.pop_batch(10).unwrap().unwrap();
        assert_eq!(b.bytes_to_release, 100);
    }

    #[test]
    fn pop_returns_none_when_all_empty() {
        let mut ps = PartitionSessions::default();
        assert!(ps.pop_batch(10).unwrap().is_none());
        ps.start(session(1, 10)).unwrap();
        push_messages(&mut ps, 1, [(0, 0)]);
        ps.pop_batch(10).unwrap();
        assert!(ps.pop_batch(10).unwrap().is_none());
    }

    #[test]
    fn pop_skips_started_sessions_without_messages() {
        let mut ps = PartitionSessions::default();
        ps.start(session(1, 10)).unwrap();
        ps.start(session(2, 20)).unwrap();
        push_messages(&mut ps, 2, [(0, 0)]);

        let b = ps.pop_batch(10).unwrap().unwrap();
        assert_eq!(b.messages[0].commit_marker.partition_session_id, psid(2));
    }

    #[test]
    fn stopped_child_is_not_unblocked_later() {
        let mut ps = PartitionSessions::default();
        ps.start(session(1, 10)).unwrap();
        push_messages(&mut ps, 1, [(0, 0)]);

        ps.end(psid(1), vec![pid(20)]).unwrap();
        ps.start(session(2, 20)).unwrap();
        push_messages(&mut ps, 2, [(0, 0)]);
        ps.stop(psid(2), 0).unwrap();
        ps.pop_batch(10).unwrap().unwrap();

        assert!(!ps.round_robin.contains(psid(2)));
    }

    #[test]
    fn child_unblocked_via_pending_when_start_comes_after_parent_drain() {
        // End arrives before child starts: block lives in pending_child_blocks.
        let mut ps = PartitionSessions::default();
        ps.start(session(1, 10)).unwrap();
        push_messages(&mut ps, 1, [(0, 0)]);
        ps.end(psid(1), vec![pid(20)]).unwrap();

        // Child has NOT started yet. Parent drain clears the pending child block.
        ps.pop_batch(10).unwrap().unwrap();

        // Child starts after the parent drained, so it is immediately readable.
        ps.start(session(2, 20)).unwrap();
        assert!(
            ps.round_robin.contains(psid(2)),
            "child must enter round-robin after parent drain"
        );
    }

    #[test]
    fn draining_parent_unblocks_child_without_stop() {
        let mut ps = PartitionSessions::default();
        ps.start(session(1, 10)).unwrap();
        push_messages(&mut ps, 1, [(0, 0)]);
        ps.end(psid(1), vec![pid(20)]).unwrap();

        // Child starts while parent still alive.
        ps.start(session(2, 20)).unwrap();
        assert!(
            !ps.round_robin.contains(psid(2)),
            "child must stay blocked while parent still has messages"
        );

        // Parent drain releases the child before commit ack.
        ps.pop_batch(10).unwrap().unwrap();
        assert!(
            ps.round_robin.contains(psid(2)),
            "child must enter round-robin after parent drain"
        );
    }

    #[test]
    fn stopping_ending_parent_releases_child_blocks() {
        let mut ps = PartitionSessions::default();
        ps.start(session(1, 10)).unwrap();
        push_messages(&mut ps, 1, [(0, 0)]);
        ps.end(psid(1), vec![pid(20)]).unwrap();

        ps.start(session(2, 20)).unwrap();
        push_messages(&mut ps, 2, [(0, 0)]);
        assert!(!ps.round_robin.contains(psid(2)));

        // Stop arrives with committed_offset = 1 >= terminal (1): children released.
        ps.stop(psid(1), 1).unwrap();
        assert!(
            ps.round_robin.contains(psid(2)),
            "stopping an ended parent with sufficient committed offset must release child blocks"
        );

        let child = ps.pop_batch(10).unwrap().unwrap();
        assert_eq!(
            child.messages[0].commit_marker.partition_session_id,
            psid(2)
        );
    }

    #[test]
    fn messages_after_end_are_protocol_error() {
        let mut ps = PartitionSessions::default();
        ps.start(session(1, 10)).unwrap();
        ps.end(psid(1), vec![]).unwrap();

        assert!(ps
            .push_raw_batch(raw_batch([(0, 0)]), psid(1), 0, 0)
            .is_err());
    }

    #[test]
    fn duplicate_end_after_empty_queue_removal_is_error() {
        let mut ps = PartitionSessions::default();
        ps.start(session(1, 10)).unwrap();

        // No messages pushed: terminal == last_acked == 0 → fast path removes session.
        ps.end(psid(1), vec![pid(20)]).unwrap();
        assert!(ps.end(psid(1), vec![pid(20)]).is_err());
    }

    #[test]
    fn end_on_drained_uncommitted_parent_unblocks_child_but_keeps_parent_alive() {
        let mut ps = PartitionSessions::default();
        ps.start(session(1, 10)).unwrap();
        push_messages(&mut ps, 1, [(0, 0)]);

        // Pop the message — queue now empty but last_acked (0) < terminal (1).
        ps.pop_batch(10).unwrap().unwrap();

        // End arrives with an empty queue and unacked messages.
        ps.end(psid(1), vec![pid(20)]).unwrap();

        // Child starts after the parent drained, so it is immediately readable.
        ps.start(session(2, 20)).unwrap();
        assert!(
            ps.round_robin.contains(psid(2)),
            "child must be unblocked after parent drain"
        );

        // has_session must still return true so commit() can validate the marker.
        assert!(ps.has_session(psid(1)));

        // Ack below terminal — parent stays alive.
        ps.observe_commit_ack(psid(1), 0).unwrap();
        assert!(ps.has_session(psid(1)));

        // Ack at terminal — parent is closed.
        ps.observe_commit_ack(psid(1), 1).unwrap();
        assert!(!ps.has_session(psid(1)));
    }

    #[test]
    fn end_on_already_acked_parent_uses_fast_path() {
        let mut ps = PartitionSessions::default();
        ps.start(session(1, 10)).unwrap();
        push_messages(&mut ps, 1, [(0, 0)]);

        // Pop message, then ack it (last_acked = 1 = terminal).
        ps.pop_batch(10).unwrap().unwrap();
        ps.observe_commit_ack(psid(1), 1).unwrap();

        // End arrives after all messages are acked — fast path, no child block.
        ps.end(psid(1), vec![pid(20)]).unwrap();

        // Child starts immediately unblocked.
        ps.start(session(2, 20)).unwrap();
        assert!(
            ps.round_robin.contains(psid(2)),
            "child must be immediately unblocked when parent committed before End"
        );
    }

    #[test]
    fn blocked_child_can_also_be_ending_parent() {
        let mut ps = PartitionSessions::default();

        ps.start(session(1, 10)).unwrap();
        push_messages(&mut ps, 1, [(0, 0)]);
        ps.end(psid(1), vec![pid(20)]).unwrap();

        ps.start(session(2, 20)).unwrap();
        push_messages(&mut ps, 2, [(0, 0)]);
        ps.end(psid(2), vec![pid(30)]).unwrap();

        ps.start(session(3, 30)).unwrap();
        push_messages(&mut ps, 3, [(0, 0)]);

        assert!(!ps.round_robin.contains(psid(2)));
        assert!(!ps.round_robin.contains(psid(3)));

        // Drain parent 1. psid(2) becomes readable, but psid(3) is still blocked by psid(2).
        ps.pop_batch(10).unwrap().unwrap();
        assert!(ps.round_robin.contains(psid(2)));
        assert!(!ps.round_robin.contains(psid(3)));

        let child = ps.pop_batch(10).unwrap().unwrap();
        assert_eq!(
            child.messages[0].commit_marker.partition_session_id,
            psid(2)
        );

        // Drain parent 2 (which is now the child from above).
        assert!(ps.round_robin.contains(psid(3)));

        let grandchild = ps.pop_batch(10).unwrap().unwrap();
        assert_eq!(
            grandchild.messages[0].commit_marker.partition_session_id,
            psid(3)
        );
    }

    // --- Tests for drain-time unblocking and commit-time closing behavior ---

    #[test]
    fn stop_ending_parent_before_commit_triggers_reconnect() {
        let mut ps = PartitionSessions::default();
        ps.start(session(1, 10)).unwrap();
        push_messages(&mut ps, 1, [(0, 0)]);
        ps.end(psid(1), vec![pid(20)]).unwrap();

        ps.start(session(2, 20)).unwrap();
        push_messages(&mut ps, 2, [(0, 0)]);

        // Stop arrives with committed_offset = 0 < terminal (1).
        assert!(ps.stop(psid(1), 0).is_err());
        assert!(
            !ps.round_robin.contains(psid(2)),
            "child must stay blocked when reconnect is required"
        );
    }

    #[test]
    fn stop_drained_ending_parent_before_commit_triggers_reconnect() {
        let mut ps = PartitionSessions::default();
        ps.start(session(1, 10)).unwrap();
        push_messages(&mut ps, 1, [(0, 0)]);
        ps.end(psid(1), vec![pid(20)]).unwrap();

        ps.start(session(2, 20)).unwrap();
        push_messages(&mut ps, 2, [(0, 0)]);

        // Drain parent. Child is already unblocked, but the parent is still alive for commits.
        ps.pop_batch(10).unwrap().unwrap();
        assert!(ps.round_robin.contains(psid(2)));
        assert!(ps.has_session(psid(1)));

        // Stop arrives with committed_offset = 0 < terminal (1).
        assert!(ps.stop(psid(1), 0).is_err());
    }

    #[test]
    fn stop_drained_ending_parent_after_commit_closes_parent() {
        let mut ps = PartitionSessions::default();
        ps.start(session(1, 10)).unwrap();
        push_messages(&mut ps, 1, [(0, 0)]);
        ps.end(psid(1), vec![pid(20)]).unwrap();

        ps.start(session(2, 20)).unwrap();
        push_messages(&mut ps, 2, [(0, 0)]);

        // Drain parent. Child is already unblocked, but the parent is still alive for commits.
        ps.pop_batch(10).unwrap().unwrap();
        assert!(ps.round_robin.contains(psid(2)));
        assert!(ps.has_session(psid(1)));

        // Stop arrives with committed_offset = 1 >= terminal (1).
        ps.stop(psid(1), 1).unwrap();
        assert!(!ps.has_session(psid(1)));
    }

    #[test]
    fn commit_ack_closes_parent_after_drain() {
        let mut ps = PartitionSessions::default();
        ps.start(session(1, 10)).unwrap();
        push_messages(&mut ps, 1, [(0, 0)]);
        ps.end(psid(1), vec![pid(20)]).unwrap();

        ps.start(session(2, 20)).unwrap();
        push_messages(&mut ps, 2, [(0, 0)]);

        assert!(!ps.round_robin.contains(psid(2)));
        ps.pop_batch(10).unwrap().unwrap();
        assert!(
            ps.round_robin.contains(psid(2)),
            "child must unblock after parent drain"
        );
        assert!(ps.has_session(psid(1)));

        // Ack below terminal — parent stays alive.
        ps.observe_commit_ack(psid(1), 0).unwrap();
        assert!(ps.has_session(psid(1)));

        // Ack at terminal — parent closes.
        ps.observe_commit_ack(psid(1), 1).unwrap();
        assert!(!ps.has_session(psid(1)));
    }
}
