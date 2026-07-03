use std::collections::{HashMap, VecDeque};

use itertools::Itertools;

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
    pub(super) messages_became_available: bool,
}

enum PartitionLifecycle {
    Reading,
    Ending {
        children_to_unblock: Vec<PartitionId>,
    },
    // TODO: Support graceful `StopPartitionRequest` via additional enum variant -- Stopping -- and
    // expected commit offset
}

struct PartitionEntry {
    session: PartitionSession,
    queue: VecDeque<TopicReaderMessage>,
    /// Number of parent sessions whose buffered messages must drain before this partition can be read.
    blocked_by: usize,
    lifecycle: PartitionLifecycle,
}

/// Unified per-connection state for all partition sessions.
///
/// Owns the YDB session metadata, per-partition message queues, the round-robin schedule,
/// and the parent→child blocking relationships. All partition lifecycle events go through here.
#[derive(Default)]
pub(super) struct PartitionSessions {
    entries: HashMap<PartitionSessionId, PartitionEntry>,

    /// Maps partition_id → session_id. Needed to find a child session when a parent drains.
    partition_to_session: HashMap<PartitionId, PartitionSessionId>,

    /// Maps child partition_id → pending block count for children that haven't started yet.
    /// Once a child starts, the count moves into its `PartitionEntry::blocked_by`.
    pending_child_blocks: HashMap<PartitionId, usize>,

    /// Round-robin schedule over partition sessions that are readable.
    /// Blocked child sessions are excluded until all their parents drain.
    round_robin: RoundRobin,
}

impl PartitionSessions {
    /// Registers a new partition session. If the partition ID is registered as a pending
    /// child block, the session starts blocked and is kept out of the
    /// round-robin until its parents drain.
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

        let entry = PartitionEntry {
            session,
            queue: VecDeque::new(),
            blocked_by,
            lifecycle: PartitionLifecycle::Reading,
        };

        self.partition_to_session.insert(pid, psid);
        self.entries.insert(psid, entry);
        Ok(())
    }

    /// Removes the partition session.
    pub(super) fn stop(&mut self, psid: PartitionSessionId) -> YdbResult<bool> {
        let entry = self.remove_entry(psid, "stop")?;

        let children_to_unblock = match entry.lifecycle {
            PartitionLifecycle::Reading => Vec::new(),
            PartitionLifecycle::Ending {
                children_to_unblock,
            } => children_to_unblock,
        };

        self.release_child_blocks(psid, children_to_unblock)
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
    ) -> YdbResult<bool> {
        let mut messages_became_available = false;
        for child_pid in children_to_unblock {
            if let Some(&child_psid) = self.partition_to_session.get(&child_pid) {
                let Some(child_entry) = self.entries.get_mut(&child_psid) else {
                    return Err(YdbError::custom(format!(
                        "topic reader child session {child_psid} (partition {child_pid}) has no entry"
                    )));
                };

                child_entry.blocked_by = child_entry.blocked_by.checked_sub(1).ok_or_else(|| {
                    YdbError::custom(format!(
                        "topic reader child session {child_psid} (partition {child_pid}) block count underflow when parent {psid} drained"
                    ))
                })?;

                if child_entry.blocked_by == 0 {
                    messages_became_available |= !child_entry.queue.is_empty();
                    self.round_robin.push(child_psid);
                }
            } else {
                // Child has not started yet: decrement pending block count.
                if let Some(count) = self.pending_child_blocks.get_mut(&child_pid) {
                    *count = count.checked_sub(1).ok_or_else(|| {
                        YdbError::custom(format!(
                            "topic reader pending child partition {child_pid} block count underflow when parent {psid} drained"
                        ))
                    })?;
                    if *count == 0 {
                        self.pending_child_blocks.remove(&child_pid);
                    }
                }
                // If neither map has the child, it was already stopped — ignore.
            }
        }

        Ok(messages_became_available)
    }

    /// Records that the parent session is ending and registers its child partitions.
    ///
    /// If the parent queue is empty there is nothing to order against, so no blocking is
    /// registered and the children can start unblocked. If the parent is already in
    /// `Ending` state, that is a protocol error.
    pub(super) fn end(
        &mut self,
        psid: PartitionSessionId,
        child_pids: Vec<PartitionId>,
    ) -> YdbResult<()> {
        let Some(entry) = self.entries.get_mut(&psid) else {
            return Err(YdbError::custom(format!(
                "topic reader end for unknown partition session {psid}"
            )));
        };

        match entry.lifecycle {
            PartitionLifecycle::Reading => {}
            PartitionLifecycle::Ending { .. } => {
                return Err(YdbError::custom(format!(
                    "topic reader duplicate end partition session {psid}"
                )));
            }
        }

        // If the queue is already drained, children can start unblocked.
        if entry.queue.is_empty() {
            self.remove_entry(psid, "empty ending partition")?;
            return Ok(());
        }

        for &pid in &child_pids {
            *self.pending_child_blocks.entry(pid).or_insert(0) += 1;
        }

        entry.lifecycle = PartitionLifecycle::Ending {
            children_to_unblock: child_pids,
        };

        Ok(())
    }

    /// Builds messages from a raw decompressed batch and enqueues them.
    /// Returns `Ok(true)` if any messages were added.
    pub(super) fn push_raw_batch(
        &mut self,
        batch: RawBatch,
        psid: PartitionSessionId,
        reader_id: usize,
        epoch: usize,
    ) -> YdbResult<bool> {
        if batch.message_data.is_empty() {
            return Ok(false);
        }

        let batch_bytes = batch.get_read_session_size();

        let Some(entry) = self.entries.get_mut(&psid) else {
            return Err(YdbError::custom(format!(
                "topic reader push batch: session {psid} already stopped"
            )));
        };

        if matches!(&entry.lifecycle, PartitionLifecycle::Ending { .. }) {
            return Err(YdbError::custom(format!(
                "topic reader received messages for ended partition session {psid}"
            )));
        }

        let tb = TopicReaderBatch::new(batch, &mut entry.session, reader_id, epoch);
        let mut messages = tb.messages;
        if let Some(last) = messages.last_mut() {
            last.bytes_to_release = batch_bytes;
        }
        entry.queue.extend(messages);

        Ok(true)
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

            let messages_became_available = if entry.queue.is_empty()
                && matches!(entry.lifecycle, PartitionLifecycle::Ending { .. })
            {
                self.stop(psid)?
            } else {
                false
            };

            return Ok(Some(BufferedBatch {
                messages: out,
                bytes_to_release: bytes,
                epoch,
                messages_became_available,
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

        // Parent 1 (session 1, partition 10): 5 messages.
        // Parent 2 (session 2, partition 20): 1 message.
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
            "child must unblock after all parents drain"
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
        ps.stop(psid(2)).unwrap();
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

        // Child has NOT started yet. Parent drains.
        ps.pop_batch(10).unwrap().unwrap();

        // Now child starts — should be Active (no pending block left).
        ps.start(session(2, 20)).unwrap();
        assert!(
            ps.round_robin.contains(psid(2)),
            "child must be active once its only parent already drained"
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

        ps.pop_batch(10).unwrap().unwrap();
        assert!(
            ps.round_robin.contains(psid(2)),
            "child must enter round-robin after parent drains"
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

        ps.stop(psid(1)).unwrap();
        assert!(
            ps.round_robin.contains(psid(2)),
            "stopping an ended parent must not leave child blocks stranded"
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
    fn duplicate_end_with_empty_queue_is_error() {
        let mut ps = PartitionSessions::default();
        ps.start(session(1, 10)).unwrap();

        ps.end(psid(1), vec![pid(20)]).unwrap();
        assert!(ps.end(psid(1), vec![pid(20)]).is_err());
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

        ps.pop_batch(10).unwrap().unwrap();
        assert!(ps.round_robin.contains(psid(2)));
        assert!(!ps.round_robin.contains(psid(3)));

        let child = ps.pop_batch(10).unwrap().unwrap();
        assert_eq!(
            child.messages[0].commit_marker.partition_session_id,
            psid(2)
        );

        assert!(ps.round_robin.contains(psid(3)));

        let grandchild = ps.pop_batch(10).unwrap().unwrap();
        assert_eq!(
            grandchild.messages[0].commit_marker.partition_session_id,
            psid(3)
        );
    }
}
