use std::sync::{Arc, Mutex};

use tokio::sync::Notify;
use tracing::{debug, warn};
use tokio::sync::futures::Notified;

use crate::client_topic::topicreader::ids::PartitionSessionId;
use crate::client_topic::topicreader::messages::TopicReaderBatch;
#[cfg(test)]
use crate::client_topic::topicreader::messages::TopicReaderMessage;
use crate::client_topic::topicreader::partition_state::PartitionSession;
use crate::client_topic::topicreader::reader::TopicReaderCommitMarker;
use crate::grpc_wrapper::raw_topic_service::common::partition::RawOffsetsRange;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::{
    PartitionCommitOffset, RawCommitOffsetRequest, RawCommitOffsetResponse, RawFromClientOneOf,
    RawFromServer, RawReadRequest, RawReadResponse, RawStartPartitionSessionRequest,
    RawStartPartitionSessionResponse, RawStopPartitionSessionRequest,
    RawStopPartitionSessionResponse,
};
use crate::{YdbError, YdbResult};

use super::connection::Connection;
use super::message_buffer::{BufferedBatch, MessageBuffer};
use super::pending_commits::{CommitAckReceiver, PendingCommits};

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

    #[cfg(test)]
    fn push_batch(&mut self, messages: Vec<TopicReaderMessage>) {
        self.buffer.push_batch(messages);
    }

    fn pop_batch(&mut self, cap: usize) -> YdbResult<Option<BufferedBatch>> {
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
    reader_id: usize,
    reconnect_notify: Notify,
}

#[derive(Clone)]
pub(crate) struct RuntimeHandle {
    inner: Arc<Inner>,
}

impl RuntimeHandle {
    pub(crate) fn new(reader_id: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State::Reconnecting),
                messages_available: Notify::new(),
                reader_id,
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
                reader_id: 0,
                reconnect_notify: Notify::new(),
            }),
        }
    }

    pub(crate) fn handle_from_server(&self, msg: RawFromServer) -> YdbResult<()> {
        match msg {
            RawFromServer::ReadResponse(resp) => self.handle_read_response(resp),
            RawFromServer::CommitOffsetResponse(resp) => self.handle_commit_offset_response(resp),
            RawFromServer::StartPartitionSessionRequest(req) => {
                self.handle_start_partition_session(req)
            }
            RawFromServer::StopPartitionSessionRequest(req) => {
                self.handle_stop_partition_session(req)
            }
            RawFromServer::InitResponse(_) => {
                debug!("topic reader initialized");
                Ok(())
            }
            RawFromServer::UpdateTokenResponse(_) => {
                debug!("topic reader received update token response");
                Ok(())
            }
            RawFromServer::UnsupportedMessage(message) => {
                debug!("topic reader received unsupported message: {message}");
                Ok(())
            }
        }
    }

    fn handle_read_response(&self, resp: RawReadResponse) -> YdbResult<()> {
        let mut pushed = false;
        {
            let mut state = self.lock_state()?;
            let State::Active(active) = &mut *state else {
                return Ok(());
            };
            let reader_id = self.inner.reader_id;
            let epoch = active.connection.epoch();

            for partition_data in resp.partition_data {
                let partition_session_id =
                    PartitionSessionId::from_raw(partition_data.partition_session_id);
                for batch in partition_data.batches {
                    let added = active.buffer.push_raw_batch(
                        batch,
                        partition_session_id,
                        reader_id,
                        epoch,
                    )?;
                    if !added {
                        warn!(
                            %partition_session_id,
                            "topic reader received read response for unknown partition session"
                        );
                    }
                    pushed |= added;
                }
            }
        }

        if pushed {
            self.inner.messages_available.notify_one();
        }
        Ok(())
    }

    fn handle_commit_offset_response(&self, resp: RawCommitOffsetResponse) -> YdbResult<()> {
        let committed_offsets = resp.partitions_committed_offsets.into_iter().map(|offset| {
            (
                PartitionSessionId::from_raw(offset.partition_session_id),
                offset.committed_offset,
            )
        });

        let mut state = self.lock_state()?;
        match &mut *state {
            State::Active(active) => active.pending_commits.ack(committed_offsets),
            State::Reconnecting => {}
            State::Failed(err) => return Err(err.clone()),
        }
        Ok(())
    }

    fn handle_start_partition_session(
        &self,
        req: RawStartPartitionSessionRequest,
    ) -> YdbResult<()> {
        let mut state = self.lock_state()?;
        let State::Active(active) = &mut *state else {
            return Ok(());
        };

        let session = PartitionSession::from(req);
        let partition_session_id = session.partition_session_id;
        active.buffer.start(session)?;
        active
            .connection
            .send(RawFromClientOneOf::StartPartitionSessionResponse(
                RawStartPartitionSessionResponse {
                    partition_session_id: partition_session_id.into_raw(),
                },
            ))?;
        Ok(())
    }

    fn handle_stop_partition_session(&self, req: RawStopPartitionSessionRequest) -> YdbResult<()> {
        let RawStopPartitionSessionRequest {
            partition_session_id,
            graceful,
            committed_offset,
        } = req;
        let partition_session_id = PartitionSessionId::from_raw(partition_session_id);

        debug!(
            %partition_session_id,
            graceful, committed_offset, "topic reader received stop partition session request"
        );

        let mut state = self.lock_state()?;
        let State::Active(active) = &mut *state else {
            return Ok(());
        };

        if !active.buffer.stop(partition_session_id) {
            warn!(
                %partition_session_id,
                "topic reader received stop for unknown partition session"
            );
        }

        active.pending_commits.stop(
            partition_session_id,
            Some(committed_offset),
            &YdbError::custom(format!(
                "partition session {partition_session_id} stopped by server"
            )),
        );
        active
            .connection
            .send(RawFromClientOneOf::StopPartitionSessionResponse(
                RawStopPartitionSessionResponse {
                    partition_session_id: partition_session_id.into_raw(),
                },
            ))?;
        Ok(())
    }

    #[cfg(test)]
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
                    State::Active(active) => active.pop_batch(cap)?,
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

                if !active
                    .buffer
                    .is_active_session(commit_marker.partition_session_id)
                {
                    return Err(YdbError::custom(format!(
                        "topic reader commit for stopped partition session {}",
                        commit_marker.partition_session_id,
                    )));
                }

                let receiver = active
                    .pending_commits
                    .push(commit_marker.partition_session_id, commit_marker.end_offset);
                let commit_message =
                    RawFromClientOneOf::CommitOffsetRequest(RawCommitOffsetRequest {
                        commit_offsets: vec![PartitionCommitOffset {
                            partition_session_id: commit_marker.partition_session_id.into_raw(),
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
    use crate::client_topic::topicreader::messages::TopicReaderMessage;
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
        activate_test_session(&runtime, 1);
        assert!(runtime.commit(commit_marker(1)).is_ok());
    }

    #[test]
    fn install_connection_routes_commits_to_new_channel() {
        let (runtime, _old_rx) = runtime_with_epoch(0);

        let (new_tx, mut new_rx) = mpsc::unbounded_channel();
        runtime
            .install_connection(Connection::new(new_tx, 1), YdbError::custom("reconnect"))
            .expect("install_connection should succeed");
        activate_test_session(&runtime, 1);

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
        let runtime = RuntimeHandle::new(0);
        assert!(runtime.commit(commit_marker(0)).is_err());

        let (outgoing_tx, _outgoing_rx) = mpsc::unbounded_channel();
        runtime
            .install_connection(
                Connection::new(outgoing_tx, 0),
                YdbError::custom("first connection"),
            )
            .expect("install_connection should install connection");
        activate_test_session(&runtime, 0);

        assert!(runtime.commit(commit_marker(0)).is_ok());
    }

    #[tokio::test]
    async fn enter_reconnecting_drops_buffered_messages() {
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
        activate_test_session(&runtime, 1);

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
        let runtime = RuntimeHandle::with_connection(Connection::new(outgoing_tx, epoch));
        activate_test_session(&runtime, epoch);
        (runtime, outgoing_rx)
    }

    fn activate_test_session(runtime: &RuntimeHandle, epoch: usize) {
        runtime
            .push_batch(vec![TopicReaderMessage::test_message(epoch, 0)])
            .expect("test session should be activated");
    }

    fn commit_marker(epoch: usize) -> TopicReaderCommitMarker {
        TopicReaderCommitMarker {
            partition_session_id: PartitionSessionId::from_raw(10),
            partition_id: crate::client_topic::topicreader::ids::PartitionId::from_raw(20),
            start_offset: 30,
            end_offset: 40,
            topic: "test-topic".to_string(),
            epoch,
        }
    }
}
