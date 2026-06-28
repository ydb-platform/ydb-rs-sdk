use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};

use tokio::sync::oneshot;

use crate::{YdbError, YdbResult};

pub(super) type PartitionSessionId = i64;
type CommitAckSender = oneshot::Sender<YdbResult<()>>;
pub(super) type CommitAckReceiver = oneshot::Receiver<YdbResult<()>>;
type PartitionPendingCommits = BTreeMap<Reverse<i64>, CommitAckSender>;

#[derive(Default)]
pub(super) struct PendingCommits {
    sessions: HashMap<PartitionSessionId, PartitionPendingCommits>,
}

impl PendingCommits {
    pub(super) fn push(
        &mut self,
        partition_session_id: PartitionSessionId,
        committed_offset: i64,
    ) -> CommitAckReceiver {
        let (sender, receiver) = oneshot::channel();
        self.sessions
            .entry(partition_session_id)
            .or_default()
            .insert(Reverse(committed_offset), sender);
        receiver
    }

    pub(super) fn ack(
        &mut self,
        committed_offsets: impl IntoIterator<Item = (PartitionSessionId, i64)>,
    ) {
        for (psid, offset) in committed_offsets {
            self.ack_partition(psid, offset);
        }
    }

    pub(super) fn fail_all(&mut self, reason: &YdbError) {
        let sessions = std::mem::take(&mut self.sessions);
        for session in sessions.into_values() {
            Self::fail_commits(session, reason);
        }
    }

    fn fail_session(&mut self, psid: PartitionSessionId, reason: &YdbError) {
        if let Some(session) = self.sessions.remove(&psid) {
            Self::fail_commits(session, reason);
        }
    }

    pub(super) fn fail_one(
        &mut self,
        psid: PartitionSessionId,
        committed_offset: i64,
        reason: &YdbError,
    ) {
        let Some(session) = self.sessions.get_mut(&psid) else {
            return;
        };
        if let Some(sender) = session.remove(&Reverse(committed_offset)) {
            let _ = sender.send(Err(reason.clone()));
        }
        if session.is_empty() {
            self.sessions.remove(&psid);
        }
    }

    pub(super) fn stop(
        &mut self,
        psid: PartitionSessionId,
        committed_offset: Option<i64>,
        reason: &YdbError,
    ) {
        if let Some(offset) = committed_offset {
            self.ack_partition(psid, offset);
        }
        self.fail_session(psid, reason);
    }

    fn ack_partition(&mut self, psid: PartitionSessionId, committed_offset: i64) {
        let Some(session) = self.sessions.get_mut(&psid) else {
            return;
        };

        // NOTE: Reverse keeps offsets covered by the server ack on the right of
        // split_off(&Reverse(committed_offset)): real end_offset <= committed_offset.
        let acked = session.split_off(&Reverse(committed_offset));
        Self::ack_commits(acked);
        if session.is_empty() {
            self.sessions.remove(&psid);
        }
    }

    fn ack_commits(commits: PartitionPendingCommits) {
        for sender in commits.into_values() {
            let _ = sender.send(Ok(()));
        }
    }

    fn fail_commits(commits: PartitionPendingCommits, reason: &YdbError) {
        for sender in commits.into_values() {
            let _ = sender.send(Err(reason.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot::error::TryRecvError;

    use super::*;

    #[test]
    fn pending_commits_acks_up_to_committed_offset() {
        let mut pending = PendingCommits::default();

        let mut ack0_0 = pending.push(0, 0);
        let mut ack0_1 = pending.push(0, 1);
        let mut ack0_2 = pending.push(0, 2);
        let mut ack1_0 = pending.push(1, 0);

        pending.ack([(0, 1)]);

        assert!(ack0_0.try_recv().is_ok());
        assert!(ack0_1.try_recv().is_ok());
        assert!(matches!(ack0_2.try_recv(), Err(TryRecvError::Empty)));
        assert!(matches!(ack1_0.try_recv(), Err(TryRecvError::Empty)));

        pending.fail_session(1, &YdbError::custom("fail"));
        assert!(matches!(ack1_0.try_recv(), Ok(Err(_))));

        drop(pending);
        assert!(matches!(ack0_2.try_recv(), Err(TryRecvError::Closed)));
    }
}
