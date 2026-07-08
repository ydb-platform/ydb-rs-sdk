use std::sync::{Arc, Mutex};

use tokio::sync::Notify;
use tokio::sync::futures::Notified;

use crate::client_topic::topicreader::messages::{TopicReaderBatch, TopicReaderMessage};
use crate::client_topic::topicreader::reader::TopicReaderCommitMarker;
use crate::grpc_wrapper::raw_topic_service::common::partition::RawOffsetsRange;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::{
    PartitionCommitOffset, RawCommitOffsetRequest, RawFromClientOneOf, RawReadRequest,
};
use crate::{YdbError, YdbResult};

use super::connection::Connection;
use super::message_buffer::{BufferedBatch, MessageBuffer};
use super::pending_commits::{CommitAckReceiver, PartitionSessionId, PendingCommits};

const RUNTIME_HANDLE_POISONED: &str = "topic reader runtime handle mutex poisoned";

struct Active {
    buffer: MessageBuffer,
    pending_commits: PendingCommits,
    connection: Connection,
}

impl Active {
    fn new(connection: Connection) -> Self {
        Self {
            buffer: MessageBuffer::default(),
            pending_commits: PendingCommits::default(),
            connection,
        }
    }

    fn push_batch(&mut self, messages: Vec<TopicReaderMessage>) {
        self.buffer.push_batch(messages);
    }

    fn pop_batch(&mut self, cap: usize) -> Option<BufferedBatch> {
        self.buffer.pop_batch(cap)
    }
}

enum State {
    Reconnecting,
    Active(Active),
    Failed(YdbError),
}

struct Inner {
    state: Mutex<State>,
    messages_available: Notify,
    reconnect_notify: Notify,
}

#[derive(Clone)]
pub(crate) struct RuntimeHandle {
    inner: Arc<Inner>,
}

impl RuntimeHandle {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State::Reconnecting),
                messages_available: Notify::new(),
                reconnect_notify: Notify::new(),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_connection(connection: Connection) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State::Active(Active::new(connection))),
                messages_available: Notify::new(),
                reconnect_notify: Notify::new(),
            }),
        }
    }

    pub(crate) fn push_batch(&self, messages: Vec<TopicReaderMessage>) -> YdbResult<()> {
        let pushed = {
            let mut state = self.lock_state()?;
            match &mut *state {
                State::Reconnecting => false,
                State::Active(active) => {
                    active.push_batch(messages);
                    true
                }
                State::Failed(err) => return Err(err.clone()),
            }
        };

        if pushed {
            self.inner.messages_available.notify_one();
        }

        Ok(())
    }

    pub(crate) async fn pop_batch(&self, cap: usize) -> YdbResult<TopicReaderBatch> {
        if cap == 0 {
            return Err(YdbError::Custom(
                "topic reader pop_batch called with cap=0".into(),
            ));
        }

        loop {
            // Register interest BEFORE checking the buffer; any notify_one()
            // between the check and notified.await leaves a permit, not a lost wake.
            let notified = self.inner.messages_available.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let batch = {
                let mut guard = self.lock_state()?;
                match &mut *guard {
                    State::Reconnecting => None,
                    State::Active(active) => active.pop_batch(cap),
                    State::Failed(err) => return Err(err.clone()),
                }
            };

            if let Some(batch) = batch {
                self.request_bytes(batch.bytes_to_release, batch.epoch)?;
                return Ok(TopicReaderBatch::from_messages(batch.messages));
            }

            notified.await;
        }
    }

    pub(crate) fn commit(
        &self,
        commit_marker: TopicReaderCommitMarker,
    ) -> YdbResult<CommitAckReceiver> {
        let mut state = self.lock_state()?;
        match &mut *state {
            State::Active(active) => {
                if commit_marker.epoch != active.connection.epoch() {
                    return Err(YdbError::custom(format!(
                        "topic reader commit for partition session {} belongs to connection epoch {}, current epoch {}",
                        commit_marker.partition_session_id,
                        commit_marker.epoch,
                        active.connection.epoch(),
                    )));
                }

                let receiver = active
                    .pending_commits
                    .push(commit_marker.partition_session_id, commit_marker.end_offset);
                let commit_message =
                    RawFromClientOneOf::CommitOffsetRequest(RawCommitOffsetRequest {
                        commit_offsets: vec![PartitionCommitOffset {
                            partition_session_id: commit_marker.partition_session_id,
                            offsets: vec![RawOffsetsRange {
                                start: commit_marker.start_offset,
                                end: commit_marker.end_offset,
                            }],
                        }],
                    });

                if let Err(err) = active.connection.send(commit_message) {
                    active.pending_commits.fail_one(
                        commit_marker.partition_session_id,
                        commit_marker.end_offset,
                        &err,
                    );
                    return Err(err);
                }

                Ok(receiver)
            }
            State::Reconnecting => Err(YdbError::custom(
                "topic reader commit requested while reconnecting",
            )),
            State::Failed(err) => Err(err.clone()),
        }
    }

    pub(crate) fn request_bytes(&self, bytes_to_release: i64, epoch: usize) -> YdbResult<()> {
        let state = self.lock_state()?;
        match &*state {
            State::Active(active) => {
                if bytes_to_release > 0 && active.connection.epoch() == epoch {
                    // Read credit belongs to the current grpc attempt. If its channel
                    // is already closed, the attempt is dying and GrpcStreamer will
                    // drive reconnect; buffered messages must still be returned.
                    let _ =
                        active
                            .connection
                            .send(RawFromClientOneOf::ReadRequest(RawReadRequest {
                                bytes_size: bytes_to_release,
                            }));
                }
                Ok(())
            }
            State::Reconnecting => Ok(()),
            State::Failed(err) => Err(err.clone()),
        }
    }

    pub(crate) fn ack_commits(
        &self,
        committed_offsets: impl IntoIterator<Item = (PartitionSessionId, i64)>,
    ) -> YdbResult<()> {
        let mut state = self.lock_state()?;
        match &mut *state {
            State::Active(active) => active.pending_commits.ack(committed_offsets),
            State::Reconnecting => {}
            State::Failed(err) => return Err(err.clone()),
        }
        Ok(())
    }

    pub(crate) fn stop_partition(
        &self,
        partition_session_id: PartitionSessionId,
        committed_offset: Option<i64>,
        reason: &YdbError,
    ) -> YdbResult<()> {
        let mut state = self.lock_state()?;
        match &mut *state {
            State::Active(active) => {
                active
                    .pending_commits
                    .stop(partition_session_id, committed_offset, reason);
            }
            State::Reconnecting => {}
            State::Failed(err) => return Err(err.clone()),
        }
        Ok(())
    }

    /// Switches the reader runtime to reconnecting state.
    ///
    /// This is used by the reconnector itself after the current stream fails. It fails all pending
    /// commits from the active connection and wakes readers waiting for buffered messages.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime has already failed permanently.
    pub(crate) fn enter_reconnecting(&self, err: YdbError) -> YdbResult<()> {
        self.enter_reconnecting_inner(err)?;
        Ok(())
    }

    /// Requests the reconnector to drop the current stream and establish a new one.
    ///
    /// The request is idempotent while a reconnect is already in progress. A notification is sent
    /// only when this call changes the runtime from active to reconnecting, so duplicate callers do
    /// not leave stale reconnect permits.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime has already failed permanently.
    pub(crate) fn force_reconnection(&self, err: YdbError) -> YdbResult<()> {
        let changed = self.enter_reconnecting_inner(err)?;
        if changed {
            self.inner.reconnect_notify.notify_one();
        }

        Ok(())
    }

    fn enter_reconnecting_inner(&self, err: YdbError) -> YdbResult<bool> {
        let mut pending_commits = PendingCommits::default();
        let changed = {
            let mut state = self.lock_state()?;
            match &mut *state {
                State::Active(active) => {
                    std::mem::swap(&mut pending_commits, &mut active.pending_commits);
                    *state = State::Reconnecting;
                    true
                }
                State::Reconnecting => false,
                State::Failed(err) => return Err(err.clone()),
            }
        };

        if changed {
            pending_commits.fail_all(&err);
        }
        self.inner.messages_available.notify_waiters();

        Ok(changed)
    }

    pub(crate) fn install_connection(
        &self,
        connection: Connection,
        err: YdbError,
    ) -> YdbResult<()> {
        let mut pending_commits = PendingCommits::default();
        {
            let mut state = self.lock_state()?;
            match &mut *state {
                State::Active(active) => {
                    std::mem::swap(&mut pending_commits, &mut active.pending_commits);
                }
                State::Reconnecting => {}
                State::Failed(err) => return Err(err.clone()),
            }
            *state = State::Active(Active::new(connection));
        }
        pending_commits.fail_all(&err);
        self.inner.messages_available.notify_waiters();
        Ok(())
    }

    pub(crate) fn fail(&self, err: &YdbError) -> YdbResult<()> {
        let mut pending_commits = PendingCommits::default();
        {
            let mut state = self.lock_state()?;
            if let State::Active(active) = &mut *state {
                std::mem::swap(&mut pending_commits, &mut active.pending_commits);
            }
            *state = State::Failed(err.clone());
        }
        pending_commits.fail_all(err);
        self.inner.messages_available.notify_waiters();
        Ok(())
    }

    pub(crate) fn reconnection_notifier<'a>(&'a self) -> Notified<'a> {
        self.inner.reconnect_notify.notified()
    }

    fn lock_state(&self) -> YdbResult<std::sync::MutexGuard<'_, State>> {
        self.inner
            .state
            .lock()
            .map_err(|_| YdbError::custom(RUNTIME_HANDLE_POISONED))
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use super::*;
    use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawReadRequest;

    #[test]
    fn commit_rejects_stale_epoch() {
        let (runtime, _outgoing_rx) = runtime_with_epoch(1);

        assert!(runtime.commit(commit_marker(0)).is_err());
        assert!(runtime.commit(commit_marker(1)).is_ok());
    }

    #[test]
    fn install_connection_fails_old_pending_commits() {
        let (runtime, _outgoing_rx) = runtime_with_epoch(0);

        let mut ack = runtime
            .commit(commit_marker(0))
            .expect("commit should be registered");

        let (next_outgoing_tx, _next_outgoing_rx) = mpsc::unbounded_channel();
        runtime
            .install_connection(
                Connection::new(next_outgoing_tx, 1),
                YdbError::custom("test reconnect"),
            )
            .expect("install_connection should succeed");

        assert!(matches!(ack.try_recv(), Ok(Err(_))));
        assert!(runtime.commit(commit_marker(0)).is_err());
        assert!(runtime.commit(commit_marker(1)).is_ok());
    }

    #[test]
    fn install_connection_routes_commits_to_new_channel() {
        let (runtime, _old_rx) = runtime_with_epoch(0);

        let (new_tx, mut new_rx) = mpsc::unbounded_channel();
        runtime
            .install_connection(Connection::new(new_tx, 1), YdbError::custom("reconnect"))
            .expect("install_connection should succeed");

        runtime
            .commit(commit_marker(1))
            .expect("commit should be registered");

        assert!(matches!(
            new_rx.try_recv(),
            Ok(RawFromClientOneOf::CommitOffsetRequest(_))
        ));
    }

    #[test]
    fn install_connection_does_not_recover_failed_runtime() {
        let (runtime, _outgoing_rx) = runtime_with_epoch(0);
        runtime
            .fail(&YdbError::custom("terminal"))
            .expect("fail should succeed");

        let (next_outgoing_tx, _next_outgoing_rx) = mpsc::unbounded_channel();
        assert!(
            runtime
                .install_connection(
                    Connection::new(next_outgoing_tx, 1),
                    YdbError::custom("reconnect")
                )
                .is_err()
        );
        assert!(runtime.commit(commit_marker(1)).is_err());
    }

    #[test]
    fn reconnecting_runtime_installs_first_connection() {
        let runtime = RuntimeHandle::new();
        assert!(runtime.commit(commit_marker(0)).is_err());

        let (outgoing_tx, _outgoing_rx) = mpsc::unbounded_channel();
        runtime
            .install_connection(
                Connection::new(outgoing_tx, 0),
                YdbError::custom("first connection"),
            )
            .expect("install_connection should install connection");

        assert!(runtime.commit(commit_marker(0)).is_ok());
    }

    #[tokio::test]
    async fn enter_reconnecting_drops_buffered_messages() {
        use crate::client_topic::topicreader::messages::TopicReaderMessage;

        let (runtime, _outgoing_rx) = runtime_with_epoch(0);

        runtime
            .push_batch(vec![TopicReaderMessage::test_message(0, 10)])
            .expect("push should succeed");
        runtime
            .enter_reconnecting(YdbError::Transport("test reconnect".to_string()))
            .expect("runtime should enter reconnecting state");

        let (next_outgoing_tx, _next_outgoing_rx) = mpsc::unbounded_channel();
        runtime
            .install_connection(
                Connection::new(next_outgoing_tx, 1),
                YdbError::custom("next connection"),
            )
            .expect("install_connection should install next connection");
        runtime
            .push_batch(vec![TopicReaderMessage::test_message(1, 20)])
            .expect("push should succeed");

        let batch = runtime.pop_batch(10).await.expect("pop should succeed");
        assert_eq!(batch.messages.len(), 1);
        assert_eq!(batch.messages[0].get_commit_marker().epoch, 1);
    }

    #[test]
    fn commit_registers_ack_and_sends_commit_request() {
        let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel();
        let runtime = RuntimeHandle::with_connection(Connection::new(outgoing_tx, 1));

        let _ack = runtime
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
        let runtime = RuntimeHandle::with_connection(Connection::new(outgoing_tx, 1));

        runtime
            .push_batch(vec![TopicReaderMessage::test_message(1, 15)])
            .expect("push should succeed");

        runtime.pop_batch(10).await.expect("pop should succeed");

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
        let runtime = RuntimeHandle::with_connection(Connection::new(outgoing_tx, 1));
        drop(outgoing_rx);

        runtime
            .push_batch(vec![TopicReaderMessage::test_message(1, 15)])
            .expect("push should succeed");

        let batch = runtime.pop_batch(10).await.expect("pop should succeed");
        assert_eq!(batch.messages.len(), 1);
    }

    fn runtime_with_epoch(
        epoch: usize,
    ) -> (RuntimeHandle, mpsc::UnboundedReceiver<RawFromClientOneOf>) {
        let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel();
        (
            RuntimeHandle::with_connection(Connection::new(outgoing_tx, epoch)),
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
