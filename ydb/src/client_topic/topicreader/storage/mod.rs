mod connection_state;
mod message_buffer;
mod pending_commits;

use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, Notify};

use crate::client_topic::topicreader::messages::{TopicReaderBatch, TopicReaderMessage};
use crate::client_topic::topicreader::reader::TopicReaderCommitMarker;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawFromClientOneOf;
use crate::{YdbError, YdbResult};

use self::connection_state::ConnectionState;
use self::message_buffer::MessageBuffer;
use self::pending_commits::{CommitAckReceiver, PartitionSessionId, PendingCommits};

const SHARED_STORAGE_POISONED: &str = "topic reader shared storage mutex poisoned";

struct State {
    buffer: MessageBuffer,
    pending_commits: PendingCommits,
    connection: ConnectionState,
}

impl State {
    fn new(
        outgoing_tx: mpsc::UnboundedSender<RawFromClientOneOf>,
        connection_epoch: usize,
    ) -> Self {
        Self {
            buffer: MessageBuffer::default(),
            pending_commits: PendingCommits::default(),
            connection: ConnectionState::new(outgoing_tx, connection_epoch),
        }
    }

    fn push_batch(&mut self, messages: Vec<TopicReaderMessage>) {
        self.buffer.push_batch(messages);
    }

    fn pop_batch(&mut self, cap: usize) -> Option<TopicReaderBatch> {
        let batch = self.buffer.pop_batch(cap)?;
        self.connection
            .request_bytes(batch.bytes_to_release, batch.epoch);
        Some(TopicReaderBatch::from_messages(batch.messages))
    }

    fn commit(&mut self, commit_marker: &TopicReaderCommitMarker) -> YdbResult<CommitAckReceiver> {
        self.connection
            .commit(commit_marker, &mut self.pending_commits)
    }
}

enum StorageState {
    Connecting,
    Running(State),
    Failed(YdbError),
}

struct Inner {
    state: Mutex<StorageState>,
    notify: Notify,
}

#[derive(Clone)]
pub(super) struct SharedStorage {
    inner: Arc<Inner>,
}

impl SharedStorage {
    pub(super) fn connecting() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(StorageState::Connecting),
                notify: Notify::new(),
            }),
        }
    }

    #[cfg(test)]
    pub(super) fn with_connection(
        outgoing_tx: mpsc::UnboundedSender<RawFromClientOneOf>,
        connection_epoch: usize,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(StorageState::Running(State::new(
                    outgoing_tx,
                    connection_epoch,
                ))),
                notify: Notify::new(),
            }),
        }
    }

    pub(super) fn push_batch(&self, messages: Vec<TopicReaderMessage>) -> YdbResult<()> {
        let pushed = {
            let mut state = self.lock_state()?;
            match &mut *state {
                StorageState::Connecting => false,
                StorageState::Running(state) => {
                    state.push_batch(messages);
                    true
                }
                StorageState::Failed(err) => return Err(err.clone()),
            }
        };

        if pushed {
            self.inner.notify.notify_one();
        }

        Ok(())
    }

    pub(super) async fn pop_batch(&self, cap: usize) -> YdbResult<TopicReaderBatch> {
        if cap == 0 {
            return Err(YdbError::Custom(
                "topic reader pop_batch called with cap=0".into(),
            ));
        }

        loop {
            // Register interest BEFORE checking the buffer; any notify_one()
            // between the check and notified.await leaves a permit, not a lost wake.
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let batch = {
                let mut guard = self.lock_state()?;
                match &mut *guard {
                    StorageState::Connecting => None,
                    StorageState::Running(state) => state.pop_batch(cap),
                    StorageState::Failed(err) => return Err(err.clone()),
                }
            };

            if let Some(batch) = batch {
                return Ok(batch);
            }

            notified.await;
        }
    }

    pub(super) fn commit(
        &self,
        commit_marker: TopicReaderCommitMarker,
    ) -> YdbResult<CommitAckReceiver> {
        let mut state = self.lock_state()?;
        match &mut *state {
            StorageState::Running(state) => state.commit(&commit_marker),
            StorageState::Connecting => Err(YdbError::custom(
                "topic reader commit requested while reconnecting",
            )),
            StorageState::Failed(err) => Err(err.clone()),
        }
    }

    pub(super) fn ack_commits(
        &self,
        committed_offsets: impl IntoIterator<Item = (PartitionSessionId, i64)>,
    ) -> YdbResult<()> {
        let mut state = self.lock_state()?;
        match &mut *state {
            StorageState::Running(state) => state.pending_commits.ack(committed_offsets),
            StorageState::Connecting => {}
            StorageState::Failed(err) => return Err(err.clone()),
        }
        Ok(())
    }

    pub(super) fn stop_partition(
        &self,
        partition_session_id: PartitionSessionId,
        committed_offset: Option<i64>,
        reason: &YdbError,
    ) -> YdbResult<()> {
        let mut state = self.lock_state()?;
        match &mut *state {
            StorageState::Running(state) => {
                state
                    .pending_commits
                    .stop(partition_session_id, committed_offset, reason);
            }
            StorageState::Connecting => {}
            StorageState::Failed(err) => return Err(err.clone()),
        }
        Ok(())
    }

    pub(super) fn begin_connecting(&self, err: YdbError) -> YdbResult<()> {
        let mut pending_commits = PendingCommits::default();
        {
            let mut state = self.lock_state()?;
            match &mut *state {
                StorageState::Running(running) => {
                    std::mem::swap(&mut pending_commits, &mut running.pending_commits);
                }
                StorageState::Connecting => {}
                StorageState::Failed(err) => return Err(err.clone()),
            }
            *state = StorageState::Connecting;
        }
        pending_commits.fail_all(&err);
        self.inner.notify.notify_waiters();
        Ok(())
    }

    pub(super) fn recreate(
        &self,
        outgoing_tx: mpsc::UnboundedSender<RawFromClientOneOf>,
        connection_epoch: usize,
        err: YdbError,
    ) -> YdbResult<()> {
        let mut pending_commits = PendingCommits::default();
        {
            let mut state = self.lock_state()?;
            match &mut *state {
                StorageState::Running(running_state) => {
                    std::mem::swap(&mut pending_commits, &mut running_state.pending_commits);
                }
                StorageState::Connecting => {}
                StorageState::Failed(err) => return Err(err.clone()),
            }
            *state = StorageState::Running(State::new(outgoing_tx, connection_epoch));
        }
        pending_commits.fail_all(&err);
        self.inner.notify.notify_waiters();
        Ok(())
    }

    pub(super) fn fail(&self, err: &YdbError) -> YdbResult<()> {
        let mut pending_commits = PendingCommits::default();
        {
            let mut state = self.lock_state()?;
            if let StorageState::Running(state) = &mut *state {
                std::mem::swap(&mut pending_commits, &mut state.pending_commits);
            }
            *state = StorageState::Failed(err.clone());
        }
        pending_commits.fail_all(err);
        self.inner.notify.notify_waiters();
        Ok(())
    }

    fn lock_state(&self) -> YdbResult<std::sync::MutexGuard<'_, StorageState>> {
        self.inner
            .state
            .lock()
            .map_err(|_| YdbError::custom(SHARED_STORAGE_POISONED))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawReadRequest;

    #[test]
    fn commit_rejects_stale_epoch() {
        let (storage, _outgoing_rx) = storage_with_epoch(1);

        assert!(storage.commit(commit_marker(0)).is_err());
        assert!(storage.commit(commit_marker(1)).is_ok());
    }

    #[test]
    fn recreate_fails_old_pending_commits() {
        let (storage, _outgoing_rx) = storage_with_epoch(0);

        let mut ack = storage
            .commit(commit_marker(0))
            .expect("commit should be registered");

        let (next_outgoing_tx, _next_outgoing_rx) = mpsc::unbounded_channel();
        storage
            .recreate(next_outgoing_tx, 1, YdbError::custom("test reconnect"))
            .expect("recreate should succeed");

        assert!(matches!(ack.try_recv(), Ok(Err(_))));
        assert!(storage.commit(commit_marker(0)).is_err());
        assert!(storage.commit(commit_marker(1)).is_ok());
    }

    #[test]
    fn recreate_routes_commits_to_new_channel() {
        let (storage, _old_rx) = storage_with_epoch(0);

        let (new_tx, mut new_rx) = mpsc::unbounded_channel();
        storage
            .recreate(new_tx, 1, YdbError::custom("reconnect"))
            .expect("recreate should succeed");

        storage
            .commit(commit_marker(1))
            .expect("commit should be registered");

        assert!(matches!(
            new_rx.try_recv(),
            Ok(RawFromClientOneOf::CommitOffsetRequest(_))
        ));
    }

    #[test]
    fn recreate_does_not_recover_failed_storage() {
        let (storage, _outgoing_rx) = storage_with_epoch(0);
        storage
            .fail(&YdbError::custom("terminal"))
            .expect("fail should succeed");

        let (next_outgoing_tx, _next_outgoing_rx) = mpsc::unbounded_channel();
        assert!(storage
            .recreate(next_outgoing_tx, 1, YdbError::custom("reconnect"))
            .is_err());
        assert!(storage.commit(commit_marker(1)).is_err());
    }

    #[test]
    fn connecting_storage_installs_first_connection() {
        let storage = SharedStorage::connecting();
        assert!(storage.commit(commit_marker(0)).is_err());

        let (outgoing_tx, _outgoing_rx) = mpsc::unbounded_channel();
        storage
            .recreate(outgoing_tx, 0, YdbError::custom("first connection"))
            .expect("recreate should install connection");

        assert!(storage.commit(commit_marker(0)).is_ok());
    }

    #[tokio::test]
    async fn begin_connecting_drops_buffered_messages() {
        use crate::client_topic::topicreader::messages::TopicReaderMessage;

        let (storage, _outgoing_rx) = storage_with_epoch(0);

        storage
            .push_batch(vec![TopicReaderMessage::test_message(0, 10)])
            .expect("push should succeed");
        storage
            .begin_connecting(YdbError::Transport("test reconnect".to_string()))
            .expect("storage should enter connecting state");

        let (next_outgoing_tx, _next_outgoing_rx) = mpsc::unbounded_channel();
        storage
            .recreate(next_outgoing_tx, 1, YdbError::custom("next connection"))
            .expect("recreate should install next connection");
        storage
            .push_batch(vec![TopicReaderMessage::test_message(1, 20)])
            .expect("push should succeed");

        let batch = storage.pop_batch(10).await.expect("pop should succeed");
        assert_eq!(batch.messages.len(), 1);
        assert_eq!(batch.messages[0].get_commit_marker().epoch, 1);
    }

    #[test]
    fn commit_registers_ack_and_sends_commit_request() {
        let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel();
        let storage = SharedStorage::with_connection(outgoing_tx, 1);

        let _ack = storage
            .commit(commit_marker(1))
            .expect("commit should be registered");

        let sent = outgoing_rx
            .try_recv()
            .expect("commit request should be sent");
        let RawFromClientOneOf::CommitOffsetRequest(request) = sent else {
            panic!("expected commit offset request");
        };

        assert_eq!(request.commit_offsets.len(), 1);
        assert_eq!(request.commit_offsets[0].partition_session_id, 10);
        assert_eq!(request.commit_offsets[0].offsets.len(), 1);
        assert_eq!(request.commit_offsets[0].offsets[0].start, 30);
        assert_eq!(request.commit_offsets[0].offsets[0].end, 40);
    }

    #[tokio::test]
    async fn pop_batch_sends_read_request_for_released_bytes() {
        use crate::client_topic::topicreader::messages::TopicReaderMessage;

        let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel();
        let storage = SharedStorage::with_connection(outgoing_tx, 1);

        storage
            .push_batch(vec![TopicReaderMessage::test_message(1, 15)])
            .expect("push should succeed");

        storage.pop_batch(10).await.expect("pop should succeed");

        assert!(matches!(
            outgoing_rx.try_recv(),
            Ok(RawFromClientOneOf::ReadRequest(RawReadRequest {
                bytes_size: 15
            }))
        ));
    }

    #[tokio::test]
    async fn pop_batch_returns_messages_when_read_request_channel_is_closed() {
        use crate::client_topic::topicreader::messages::TopicReaderMessage;

        let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel();
        let storage = SharedStorage::with_connection(outgoing_tx, 1);
        drop(outgoing_rx);

        storage
            .push_batch(vec![TopicReaderMessage::test_message(1, 15)])
            .expect("push should succeed");

        let batch = storage.pop_batch(10).await.expect("pop should succeed");
        assert_eq!(batch.messages.len(), 1);
    }

    fn storage_with_epoch(
        epoch: usize,
    ) -> (SharedStorage, mpsc::UnboundedReceiver<RawFromClientOneOf>) {
        let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel();
        (
            SharedStorage::with_connection(outgoing_tx, epoch),
            outgoing_rx,
        )
    }

    fn commit_marker(epoch: usize) -> TopicReaderCommitMarker {
        TopicReaderCommitMarker {
            partition_session_id: 10,
            partition_id: 20,
            start_offset: 30,
            end_offset: 40,
            topic: "test-topic".to_string(),
            epoch,
        }
    }
}
