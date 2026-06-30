use std::collections::{HashMap, VecDeque};

use tracing::warn;

use crate::client_topic::topicreader::messages::TopicReaderMessage;

use super::round_robin::RoundRobin;

pub(super) struct BufferedBatch {
    pub(super) messages: Vec<TopicReaderMessage>,
    pub(super) bytes_to_release: i64,
    pub(super) epoch: usize,
}

#[derive(Default)]
pub(super) struct MessageBuffer {
    /// Buffered messages per partition session, in arrival order.
    queues: HashMap<i64, VecDeque<TopicReaderMessage>>,

    /// Round-robin schedule over active partition session IDs that are not blocked.
    /// The queue may be empty because sessions enter the schedule on Start.
    round_robin: RoundRobin,

    /// Maps partition_id → session_id. Updated on Start and read responses so that
    /// Stop can find the session for a child partition when unblocking it.
    partition_to_session: HashMap<i64, i64>,

    /// Maps parent session_id → child partition_ids registered via `EndPartitionSession`.
    /// Only populated when the parent still has buffered messages; empty-parent cases
    /// skip registration entirely because there is nothing to order against.
    pending_children: HashMap<i64, Vec<i64>>,

    /// Maps child partition_id → number of parent sessions that must stop before
    /// the child may enter the round-robin. Decremented on Stop; the child becomes
    /// readable only when the count reaches zero.
    blocked_partition_ids: HashMap<i64, usize>,
}

impl MessageBuffer {
    pub(super) fn push_batch(&mut self, messages: Vec<TopicReaderMessage>) {
        let Some(first) = messages.first() else {
            return;
        };

        let session_id = first.commit_marker.partition_session_id;
        let partition_id = first.commit_marker.partition_id;

        self.partition_to_session.insert(partition_id, session_id);
        self.queues.entry(session_id).or_default().extend(messages);
    }

    pub(super) fn pop_batch(&mut self, cap: usize) -> Option<BufferedBatch> {
        for _ in 0..self.round_robin.len() {
            let sid = self.round_robin.next()?;
            let Some(queue) = self.queues.get_mut(&sid) else {
                warn!(
                    partition_session_id = sid,
                    "round robin contains partition not presented in active partitions"
                );
                continue;
            };

            if queue.is_empty() {
                continue;
            }

            let take = cap.min(queue.len());
            let out: Vec<_> = queue.drain(..take).collect();
            let epoch = out[0].commit_marker.epoch;
            let bytes: i64 = out.iter().map(|m| m.bytes_to_release).sum();

            return Some(BufferedBatch {
                messages: out,
                bytes_to_release: bytes,
                epoch,
            });
        }

        None
    }

    fn queue_non_empty(&self, session_id: i64) -> bool {
        self.queues
            .get(&session_id)
            .map(|q| !q.is_empty())
            .unwrap_or(false)
    }

    pub(super) fn register_starting(&mut self, psid: i64, pid: i64) {
        self.partition_to_session.insert(pid, psid);

        if self.blocked_partition_ids.contains_key(&pid) {
            return;
        }

        self.round_robin.push(psid);
    }

    pub(super) fn register_stopping(&mut self, psid: i64, pid: i64) {
        self.round_robin.remove(psid);
        self.queues.remove(&psid);
        self.partition_to_session.remove(&pid);

        let Some(child_pids) = self.pending_children.remove(&psid) else {
            return;
        };

        for pid in child_pids {
            let Some(count) = self.blocked_partition_ids.get_mut(&pid) else {
                warn!(psid, pid, "child partition not in blocked_partition_ids");
                continue;
            };

            *count -= 1;
            if *count == 0 {
                self.blocked_partition_ids.remove(&pid);

                let Some(&psid) = self.partition_to_session.get(&pid) else {
                    continue;
                };

                self.round_robin.push(psid);
            }
        }
    }

    pub(super) fn register_ending(&mut self, parent_sid: i64, child_pids: Vec<i64>) {
        if !self.queue_non_empty(parent_sid) {
            return;
        }

        if self.pending_children.contains_key(&parent_sid) {
            warn!(parent_sid, "duplicate end partition session");
            return;
        }

        for &pid in &child_pids {
            *self.blocked_partition_ids.entry(pid).or_insert(0) += 1;
        }

        self.pending_children.insert(parent_sid, child_pids);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_topic::topicreader::messages::TopicReaderMessage;

    fn msg(session_id: i64, partition_id: i64, epoch: usize, bytes: i64) -> TopicReaderMessage {
        TopicReaderMessage::test_message_full(session_id, partition_id, epoch, bytes)
    }

    #[test]
    fn push_routes_by_session() {
        let mut buf = MessageBuffer::default();
        buf.register_starting(1, 10);
        buf.register_starting(2, 20);
        buf.push_batch(vec![msg(1, 10, 0, 0), msg(1, 10, 0, 0)]);
        buf.push_batch(vec![msg(2, 20, 0, 0)]);

        let b0 = buf.pop_batch(1).unwrap();
        assert_eq!(b0.messages[0].commit_marker.partition_session_id, 1);

        let b1 = buf.pop_batch(1).unwrap();
        assert_eq!(b1.messages[0].commit_marker.partition_session_id, 2);

        let b2 = buf.pop_batch(1).unwrap();
        assert_eq!(b2.messages[0].commit_marker.partition_session_id, 1);
    }

    #[test]
    fn merge_child_blocked_until_both_parents_stop() {
        let mut buf = MessageBuffer::default();

        // Parent 1 (session 1, partition 10): 5 messages.
        // Parent 2 (session 2, partition 20): 1 message.
        // Both declare partition 30 as a child.
        buf.register_starting(1, 10);
        buf.register_starting(2, 20);
        buf.push_batch(vec![
            msg(1, 10, 0, 0),
            msg(1, 10, 0, 0),
            msg(1, 10, 0, 0),
            msg(1, 10, 0, 0),
            msg(1, 10, 0, 0),
        ]);
        buf.push_batch(vec![msg(2, 20, 0, 0)]);
        buf.register_ending(1, vec![30]);
        buf.register_ending(2, vec![30]);

        // Child (session 3, partition 30): 2 messages.
        buf.register_starting(3, 30);
        buf.push_batch(vec![msg(3, 30, 0, 0), msg(3, 30, 0, 0)]);

        assert!(
            !buf.round_robin.contains(3),
            "child must be blocked before either parent drains"
        );

        // Drain all 6 parent messages two-at-a-time; child must stay blocked throughout.
        let mut parent_msgs_seen = 0;
        let mut pops = 0;
        loop {
            assert!(
                !buf.round_robin.contains(3),
                "child must stay blocked while parents have messages"
            );
            let b = buf.pop_batch(2).unwrap();
            assert_ne!(
                b.messages[0].commit_marker.partition_session_id, 3,
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
            !buf.round_robin.contains(3),
            "child must stay blocked after parents drain but before they stop"
        );

        buf.register_stopping(1, 10);
        assert!(
            !buf.round_robin.contains(3),
            "child must stay blocked until every parent stops"
        );
        buf.register_stopping(2, 20);
        assert!(buf.round_robin.contains(3), "child must unblock after stop");

        // cap=2 matches child queue length: all 2 messages in one pop.
        let b = buf.pop_batch(2).unwrap();
        assert_eq!(b.messages.len(), 2);
        assert_eq!(b.messages[0].commit_marker.partition_session_id, 3);
        assert!(
            buf.pop_batch(2).is_none(),
            "buffer must be empty after child drains"
        );
    }

    #[test]
    fn bytes_to_release_accumulated() {
        let mut buf = MessageBuffer::default();
        buf.register_starting(1, 10);
        buf.push_batch(vec![msg(1, 10, 0, 0), msg(1, 10, 0, 100)]);
        let b = buf.pop_batch(10).unwrap();
        assert_eq!(b.bytes_to_release, 100);
    }

    #[test]
    fn pop_returns_none_when_all_empty() {
        let mut buf = MessageBuffer::default();
        assert!(buf.pop_batch(10).is_none());
        buf.register_starting(1, 10);
        buf.push_batch(vec![msg(1, 10, 0, 0)]);
        buf.pop_batch(10);
        assert!(buf.pop_batch(10).is_none());
    }

    #[test]
    fn pop_skips_started_sessions_without_messages() {
        let mut buf = MessageBuffer::default();
        buf.register_starting(1, 10);
        buf.register_starting(2, 20);
        buf.push_batch(vec![msg(2, 20, 0, 0)]);

        let b = buf.pop_batch(10).unwrap();
        assert_eq!(b.messages[0].commit_marker.partition_session_id, 2);
    }

    #[test]
    fn stopped_child_is_not_unblocked_later() {
        let mut buf = MessageBuffer::default();
        buf.register_starting(1, 10);
        buf.push_batch(vec![msg(1, 10, 0, 0)]);

        buf.register_ending(1, vec![20]);
        buf.register_starting(2, 20);
        buf.push_batch(vec![msg(2, 20, 0, 0)]);
        buf.register_stopping(2, 20);
        buf.register_stopping(1, 10);

        assert!(!buf.round_robin.contains(2));
    }
}
