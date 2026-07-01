use std::collections::{HashMap, VecDeque};

use tracing::debug;

use crate::client_topic::topicreader::ids::{PartitionId, PartitionSessionId};
use crate::client_topic::topicreader::messages::TopicReaderMessage;
use crate::{YdbError, YdbResult};

use super::round_robin::RoundRobin;

pub(super) struct BufferedBatch {
    pub(super) messages: Vec<TopicReaderMessage>,
    pub(super) bytes_to_release: i64,
    pub(super) epoch: usize,
}

#[derive(Default)]
pub(super) struct MessageBuffer {
    /// Buffered messages per partition session, in arrival order.
    queues: HashMap<PartitionSessionId, VecDeque<TopicReaderMessage>>,

    /// Round-robin schedule over active partition session IDs that are not blocked.
    /// The queue may be empty because sessions enter the schedule on Start.
    round_robin: RoundRobin,

    /// Maps partition_id → session_id. Updated on Start and read responses so that
    /// Stop can find the session for a child partition when unblocking it.
    partition_to_session: HashMap<PartitionId, PartitionSessionId>,

    /// Maps parent session_id → child partition_ids registered via `EndPartitionSession`.
    /// Only populated when the parent still has buffered messages; empty-parent cases
    /// skip registration entirely because there is nothing to order against.
    pending_children: HashMap<PartitionSessionId, Vec<PartitionId>>,

    /// Maps child partition_id → number of parent sessions that must stop before
    /// the child may enter the round-robin. Decremented on Stop; the child becomes
    /// readable only when the count reaches zero.
    blocked_partition_ids: HashMap<PartitionId, usize>,
}

impl MessageBuffer {
    /// Push a batch onto its session's queue.
    pub(super) fn push_batch(&mut self, messages: Vec<TopicReaderMessage>) -> YdbResult<bool> {
        let Some(first) = messages.first() else {
            return Ok(false);
        };

        let session_id = first.commit_marker.partition_session_id;
        let partition_id = first.commit_marker.partition_id;

        let Some(&registered_session_id) = self.partition_to_session.get(&partition_id) else {
            debug!(
                %session_id,
                %partition_id,
                dropped = messages.len(),
                "topic reader dropping in-flight batch: partition already released"
            );
            return Ok(false);
        };
        if registered_session_id != session_id {
            debug!(
                %session_id,
                %partition_id,
                %registered_session_id,
                dropped = messages.len(),
                "topic reader dropping in-flight batch: partition reassigned"
            );
            return Ok(false);
        }

        let Some(queue) = self.queues.get_mut(&session_id) else {
            debug!(
                %session_id,
                dropped = messages.len(),
                "topic reader dropping in-flight batch: session already stopped"
            );
            return Ok(false);
        };

        queue.extend(messages);
        Ok(true)
    }

    pub(super) fn pop_batch(&mut self, cap: usize) -> YdbResult<Option<BufferedBatch>> {
        for _ in 0..self.round_robin.len() {
            let Some(sid) = self.round_robin.next() else {
                return Ok(None);
            };
            let Some(queue) = self.queues.get_mut(&sid) else {
                return Err(YdbError::custom(format!(
                    "topic reader round robin contains unknown partition session {sid}"
                )));
            };

            if queue.is_empty() {
                continue;
            }

            let take = cap.min(queue.len());
            let out: Vec<_> = queue.drain(..take).collect();
            let epoch = out[0].commit_marker.epoch;
            let bytes: i64 = out.iter().map(|m| m.bytes_to_release).sum();

            return Ok(Some(BufferedBatch {
                messages: out,
                bytes_to_release: bytes,
                epoch,
            }));
        }

        Ok(None)
    }

    fn queue_non_empty(&self, session_id: PartitionSessionId) -> YdbResult<bool> {
        let Some(queue) = self.queues.get(&session_id) else {
            return Err(YdbError::custom(format!(
                "topic reader end partition session for unknown partition session {session_id}"
            )));
        };

        Ok(!queue.is_empty())
    }

    pub(super) fn has_session(&self, psid: PartitionSessionId) -> bool {
        self.queues.contains_key(&psid)
    }

    fn queue_has_messages(&self, session_id: PartitionSessionId) -> bool {
        self.queues
            .get(&session_id)
            .map(|queue| !queue.is_empty())
            .unwrap_or(false)
    }

    pub(super) fn register_starting(
        &mut self,
        psid: PartitionSessionId,
        pid: PartitionId,
    ) -> YdbResult<()> {
        if self.queues.contains_key(&psid) {
            return Err(YdbError::custom(format!(
                "topic reader duplicate start partition session {psid}"
            )));
        }
        if let Some(&existing_psid) = self.partition_to_session.get(&pid) {
            return Err(YdbError::custom(format!(
                "topic reader duplicate start for partition {pid}: session {psid}, existing session {existing_psid}"
            )));
        }

        self.partition_to_session.insert(pid, psid);
        self.queues.insert(psid, VecDeque::new());

        if self.blocked_partition_ids.contains_key(&pid) {
            return Ok(());
        }

        self.round_robin.push(psid);
        Ok(())
    }

    /// Registers a partition session stop.
    ///
    /// Returns `Ok(true)` when this stop unblocked a child session that already
    /// has buffered messages, so `pop_batch` waiters must be notified.
    pub(super) fn register_stopping(
        &mut self,
        psid: PartitionSessionId,
        pid: PartitionId,
    ) -> YdbResult<bool> {
        self.round_robin.remove(psid);
        if self.queues.remove(&psid).is_none() {
            return Err(YdbError::custom(format!(
                "topic reader stop partition session for unknown partition session {psid}"
            )));
        }

        match self.partition_to_session.remove(&pid) {
            Some(mapped_psid) if mapped_psid == psid => {}
            Some(mapped_psid) => {
                return Err(YdbError::custom(format!(
                    "topic reader stop partition session {psid} for partition {pid}, but partition belongs to session {mapped_psid}"
                )));
            }
            None => {
                return Err(YdbError::custom(format!(
                    "topic reader stop partition session {psid} for unknown partition {pid}"
                )));
            }
        }

        let Some(child_pids) = self.pending_children.remove(&psid) else {
            return Ok(false);
        };

        let mut messages_became_available = false;
        for pid in child_pids {
            let Some(count) = self.blocked_partition_ids.get_mut(&pid) else {
                return Err(YdbError::custom(format!(
                    "topic reader child partition {pid} from parent session {psid} is not blocked"
                )));
            };

            *count -= 1;
            if *count == 0 {
                self.blocked_partition_ids.remove(&pid);

                let Some(&child_psid) = self.partition_to_session.get(&pid) else {
                    continue;
                };

                messages_became_available |= self.queue_has_messages(child_psid);
                self.round_robin.push(child_psid);
            }
        }

        Ok(messages_became_available)
    }

    pub(super) fn register_ending(
        &mut self,
        parent_sid: PartitionSessionId,
        child_pids: Vec<PartitionId>,
    ) -> YdbResult<()> {
        if !self.queue_non_empty(parent_sid)? {
            return Ok(());
        }

        if self.pending_children.contains_key(&parent_sid) {
            return Err(YdbError::custom(format!(
                "topic reader duplicate end partition session {parent_sid}"
            )));
        }

        for &pid in &child_pids {
            *self.blocked_partition_ids.entry(pid).or_insert(0) += 1;
        }

        self.pending_children.insert(parent_sid, child_pids);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_topic::topicreader::messages::TopicReaderMessage;

    fn psid(value: i64) -> PartitionSessionId {
        PartitionSessionId::from_raw(value)
    }

    fn pid(value: i64) -> PartitionId {
        PartitionId::from_raw(value)
    }

    fn msg(session_id: i64, partition_id: i64, epoch: usize, bytes: i64) -> TopicReaderMessage {
        TopicReaderMessage::test_message_full(session_id, partition_id, epoch, bytes)
    }

    #[test]
    fn push_routes_by_session() {
        let mut buf = MessageBuffer::default();
        buf.register_starting(psid(1), pid(10)).unwrap();
        buf.register_starting(psid(2), pid(20)).unwrap();
        buf.push_batch(vec![msg(1, 10, 0, 0), msg(1, 10, 0, 0)])
            .unwrap();
        buf.push_batch(vec![msg(2, 20, 0, 0)]).unwrap();

        let b0 = buf.pop_batch(1).unwrap().unwrap();
        assert_eq!(b0.messages[0].commit_marker.partition_session_id, psid(1));

        let b1 = buf.pop_batch(1).unwrap().unwrap();
        assert_eq!(b1.messages[0].commit_marker.partition_session_id, psid(2));

        let b2 = buf.pop_batch(1).unwrap().unwrap();
        assert_eq!(b2.messages[0].commit_marker.partition_session_id, psid(1));
    }

    #[test]
    fn merge_child_blocked_until_both_parents_stop() {
        let mut buf = MessageBuffer::default();

        // Parent 1 (session 1, partition 10): 5 messages.
        // Parent 2 (session 2, partition 20): 1 message.
        // Both declare partition 30 as a child.
        buf.register_starting(psid(1), pid(10)).unwrap();
        buf.register_starting(psid(2), pid(20)).unwrap();
        buf.push_batch(vec![
            msg(1, 10, 0, 0),
            msg(1, 10, 0, 0),
            msg(1, 10, 0, 0),
            msg(1, 10, 0, 0),
            msg(1, 10, 0, 0),
        ])
        .unwrap();
        buf.push_batch(vec![msg(2, 20, 0, 0)]).unwrap();
        buf.register_ending(psid(1), vec![pid(30)]).unwrap();
        buf.register_ending(psid(2), vec![pid(30)]).unwrap();

        // Child (session 3, partition 30): 2 messages.
        buf.register_starting(psid(3), pid(30)).unwrap();
        buf.push_batch(vec![msg(3, 30, 0, 0), msg(3, 30, 0, 0)])
            .unwrap();

        assert!(
            !buf.round_robin.contains(psid(3)),
            "child must be blocked before either parent drains"
        );

        // Drain all 6 parent messages two-at-a-time; child must stay blocked throughout.
        let mut parent_msgs_seen = 0;
        let mut pops = 0;
        loop {
            assert!(
                !buf.round_robin.contains(psid(3)),
                "child must stay blocked while parents have messages"
            );
            let b = buf.pop_batch(2).unwrap().unwrap();
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
            !buf.round_robin.contains(psid(3)),
            "child must stay blocked after parents drain but before they stop"
        );

        buf.register_stopping(psid(1), pid(10)).unwrap();
        assert!(
            !buf.round_robin.contains(psid(3)),
            "child must stay blocked until every parent stops"
        );
        buf.register_stopping(psid(2), pid(20)).unwrap();
        assert!(
            buf.round_robin.contains(psid(3)),
            "child must unblock after stop"
        );

        // cap=2 matches child queue length: all 2 messages in one pop.
        let b = buf.pop_batch(2).unwrap().unwrap();
        assert_eq!(b.messages.len(), 2);
        assert_eq!(b.messages[0].commit_marker.partition_session_id, psid(3));
        assert!(
            buf.pop_batch(2).unwrap().is_none(),
            "buffer must be empty after child drains"
        );
    }

    #[test]
    fn bytes_to_release_accumulated() {
        let mut buf = MessageBuffer::default();
        buf.register_starting(psid(1), pid(10)).unwrap();
        buf.push_batch(vec![msg(1, 10, 0, 0), msg(1, 10, 0, 100)])
            .unwrap();
        let b = buf.pop_batch(10).unwrap().unwrap();
        assert_eq!(b.bytes_to_release, 100);
    }

    #[test]
    fn pop_returns_none_when_all_empty() {
        let mut buf = MessageBuffer::default();
        assert!(buf.pop_batch(10).unwrap().is_none());
        buf.register_starting(psid(1), pid(10)).unwrap();
        buf.push_batch(vec![msg(1, 10, 0, 0)]).unwrap();
        buf.pop_batch(10).unwrap();
        assert!(buf.pop_batch(10).unwrap().is_none());
    }

    #[test]
    fn pop_skips_started_sessions_without_messages() {
        let mut buf = MessageBuffer::default();
        buf.register_starting(psid(1), pid(10)).unwrap();
        buf.register_starting(psid(2), pid(20)).unwrap();
        buf.push_batch(vec![msg(2, 20, 0, 0)]).unwrap();

        let b = buf.pop_batch(10).unwrap().unwrap();
        assert_eq!(b.messages[0].commit_marker.partition_session_id, psid(2));
    }

    #[test]
    fn stopped_child_is_not_unblocked_later() {
        let mut buf = MessageBuffer::default();
        buf.register_starting(psid(1), pid(10)).unwrap();
        buf.push_batch(vec![msg(1, 10, 0, 0)]).unwrap();

        buf.register_ending(psid(1), vec![pid(20)]).unwrap();
        buf.register_starting(psid(2), pid(20)).unwrap();
        buf.push_batch(vec![msg(2, 20, 0, 0)]).unwrap();
        buf.register_stopping(psid(2), pid(20)).unwrap();
        buf.register_stopping(psid(1), pid(10)).unwrap();

        assert!(!buf.round_robin.contains(psid(2)));
    }

    #[test]
    fn push_after_stop_is_silent_drop() {
        let mut buf = MessageBuffer::default();
        buf.register_starting(psid(1), pid(10)).unwrap();
        buf.register_stopping(psid(1), pid(10)).unwrap();

        assert!(!buf.push_batch(vec![msg(1, 10, 0, 0)]).unwrap());
    }

    #[test]
    fn push_after_partition_reassignment_is_silent_drop() {
        let mut buf = MessageBuffer::default();
        buf.register_starting(psid(1), pid(10)).unwrap();
        buf.register_stopping(psid(1), pid(10)).unwrap();
        buf.register_starting(psid(2), pid(10)).unwrap();

        assert!(!buf.push_batch(vec![msg(1, 10, 0, 0)]).unwrap());
    }

    #[test]
    fn push_before_start_is_silent_drop() {
        let mut buf = MessageBuffer::default();

        assert!(!buf.push_batch(vec![msg(1, 10, 0, 0)]).unwrap());
    }
}
