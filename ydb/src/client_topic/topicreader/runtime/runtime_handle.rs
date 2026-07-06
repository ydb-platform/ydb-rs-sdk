use std::sync::{Arc, Mutex};

use itertools::Itertools;
use tokio::sync::futures::Notified;
use tokio::sync::Notify;
use tracing::debug;

use crate::client_topic::topicreader::ids::{PartitionId, PartitionSessionId};
use crate::client_topic::topicreader::messages::TopicReaderBatch;
use crate::client_topic::topicreader::partition_state::PartitionSession;
use crate::client_topic::topicreader::reader::TopicReaderCommitMarker;
use crate::grpc_wrapper::raw_topic_service::common::partition::RawOffsetsRange;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::{
    PartitionCommitOffset, RawCommitOffsetRequest, RawCommitOffsetResponse, RawEndPartitionSession,
    RawFromClientOneOf, RawFromServer, RawReadRequest, RawReadResponse,
    RawStartPartitionSessionRequest, RawStartPartitionSessionResponse,
    RawStopPartitionSessionRequest, RawStopPartitionSessionResponse,
};
use crate::{YdbError, YdbResult};

use super::connection::Connection;
use super::message_buffer::{BufferedBatch, PartitionSessions, StopOutcome};
use super::pending_commits::{CommitAckReceiver, PendingCommits};

const RUNTIME_HANDLE_POISONED: &str = "topic reader runtime handle mutex poisoned";

struct Active {
    partitions: PartitionSessions,
    pending_commits: PendingCommits,
    connection: Connection,
}

impl Active {
    fn new(connection: Connection) -> Self {
        Self {
            partitions: PartitionSessions::default(),
            pending_commits: PendingCommits::default(),
            connection,
        }
    }

    fn pop_batch(&mut self, cap: usize) -> YdbResult<Option<BufferedBatch>> {
        self.partitions.pop_batch(cap)
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
            RawFromServer::StartPartitionSessionRequest(req) => {
                self.handle_start_partition_session(req)
            }
            RawFromServer::StopPartitionSessionRequest(req) => {
                self.handle_stop_partition_session(req)
            }
            RawFromServer::EndPartitionSession(end) => self.handle_end_partition_session(end),
            RawFromServer::CommitOffsetResponse(resp) => self.handle_commit_offset_response(resp),
            RawFromServer::InitResponse(_) => {
                debug!("topic reader initialized");
                Ok(())
            }
            RawFromServer::UpdateTokenResponse(_) => {
                debug!("topic reader received update token response");
                Ok(())
            }
            RawFromServer::UnsupportedMessage(m) => {
                debug!("topic reader received unsupported message: {m}");
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
                let psid = PartitionSessionId::from_raw(partition_data.partition_session_id);
                for batch in partition_data.batches {
                    pushed |= active
                        .partitions
                        .push_raw_batch(batch, psid, reader_id, epoch)?;
                }
            }
        }
        if pushed {
            self.inner.messages_available.notify_one();
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
        let psid = session.partition_session_id;
        active.partitions.start(session)?;
        active
            .connection
            .send(RawFromClientOneOf::StartPartitionSessionResponse(
                RawStartPartitionSessionResponse {
                    partition_session_id: psid.as_raw(),
                },
            ))?;
        Ok(())
    }

    fn handle_stop_partition_session(&self, req: RawStopPartitionSessionRequest) -> YdbResult<()> {
        let psid = PartitionSessionId::from_raw(req.partition_session_id);
        let outcome: StopOutcome = {
            let mut state = self.lock_state()?;
            let State::Active(active) = &mut *state else {
                return Ok(());
            };
            active.pending_commits.stop(
                psid,
                Some(req.committed_offset),
                &YdbError::custom(format!("partition session {psid} stopped by server")),
            );
            let outcome = active.partitions.stop(psid, req.committed_offset)?;
            active
                .connection
                .send(RawFromClientOneOf::StopPartitionSessionResponse(
                    RawStopPartitionSessionResponse {
                        partition_session_id: psid.as_raw(),
                    },
                ))?;
            outcome
        };
        if outcome.messages_became_available {
            self.inner.messages_available.notify_one();
        }
        if outcome.reconnect_required {
            return Err(YdbError::Transport(format!(
                "partition session {psid} stopped before terminal offset committed, reconnecting"
            )));
        }
        Ok(())
    }

    fn handle_end_partition_session(&self, end: RawEndPartitionSession) -> YdbResult<()> {
        let psid = PartitionSessionId::from_raw(end.partition_session_id);
        let child_ids = end
            .child_partition_ids
            .into_iter()
            .map(PartitionId::from_raw)
            .collect_vec();

        let mut state = self.lock_state()?;
        let State::Active(active) = &mut *state else {
            return Ok(());
        };
        active.partitions.end(psid, child_ids)?;
        Ok(())
    }

    fn handle_commit_offset_response(&self, resp: RawCommitOffsetResponse) -> YdbResult<()> {
        let committed_offsets: Vec<(PartitionSessionId, i64)> = resp
            .partitions_committed_offsets
            .into_iter()
            .map(|o| {
                (
                    PartitionSessionId::from_raw(o.partition_session_id),
                    o.committed_offset,
                )
            })
            .collect();
        let mut child_unblocked = false;
        {
            let mut state = self.lock_state()?;
            if let State::Active(active) = &mut *state {
                for &(psid, committed_offset) in &committed_offsets {
                    child_unblocked |= active
                        .partitions
                        .observe_commit_ack(psid, committed_offset)?;
                }
            }
        }
        self.ack_commits(committed_offsets)?;
        if child_unblocked {
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
                    .partitions
                    .has_session(commit_marker.partition_session_id)
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
                            partition_session_id: commit_marker.partition_session_id.as_raw(),
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
impl RuntimeHandle {
    pub(crate) fn push_test_messages(
        &self,
        messages: Vec<crate::client_topic::topicreader::messages::TopicReaderMessage>,
    ) -> YdbResult<()> {
        let Some(first) = messages.first() else {
            return Ok(());
        };
        let partition_session_id = first.commit_marker.partition_session_id;
        let epoch = first.commit_marker.epoch;
        let batch = crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawBatch {
            producer_id: String::new(),
            write_session_meta: std::collections::HashMap::new(),
            codec: crate::grpc_wrapper::raw_topic_service::common::codecs::RawCodec {
                code: i32::from(ydb_grpc::ydb_proto::topic::Codec::Raw),
            },
            written_at: crate::grpc_wrapper::raw_common_types::Timestamp::from(
                std::time::UNIX_EPOCH,
            ),
            message_data: messages
                .into_iter()
                .map(|message| {
                    crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawMessageData {
                        offset: message.offset,
                        seq_no: message.seq_no,
                        created_at: message.created_at.map(Into::into),
                        uncompressed_size: message.uncompressed_size,
                        data: Vec::new(),
                        read_session_size_bytes: message.bytes_to_release,
                    }
                })
                .collect(),
        };

        let pushed = {
            let mut state = self.lock_state()?;
            match &mut *state {
                State::Reconnecting => false,
                State::Active(active) => {
                    active
                        .partitions
                        .push_raw_batch(batch, partition_session_id, 0, epoch)?
                }
                State::Failed(err) => return Err(err.clone()),
            }
        };
        if pushed {
            self.inner.messages_available.notify_one();
        }
        Ok(())
    }

    pub(crate) fn register_starting_partition(
        &self,
        session_id: PartitionSessionId,
        partition_id: PartitionId,
    ) -> YdbResult<()> {
        let session = PartitionSession {
            partition_session_id: session_id,
            partition_id,
            topic: String::new(),
            next_commit_offset_start: 0,
        };
        let mut state = self.lock_state()?;
        match &mut *state {
            State::Active(active) => active.partitions.start(session)?,
            State::Reconnecting => {}
            State::Failed(err) => return Err(err.clone()),
        }
        Ok(())
    }

    pub(crate) fn register_ending_partition(
        &self,
        session_id: PartitionSessionId,
        child_partition_ids: Vec<PartitionId>,
    ) -> YdbResult<()> {
        let mut state = self.lock_state()?;
        match &mut *state {
            State::Active(active) => active.partitions.end(session_id, child_partition_ids)?,
            State::Reconnecting => {}
            State::Failed(err) => return Err(err.clone()),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use super::*;
    use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawReadRequest;

    fn psid(value: i64) -> PartitionSessionId {
        PartitionSessionId::from_raw(value)
    }

    fn pid(value: i64) -> PartitionId {
        PartitionId::from_raw(value)
    }

    #[test]
    fn commit_rejects_stale_epoch() {
        let (runtime, _outgoing_rx) = runtime_with_epoch(1);
        runtime
            .register_starting_partition(psid(10), pid(20))
            .expect("start should succeed");

        assert!(runtime.commit(commit_marker(psid(10), pid(20), 0)).is_err());
        assert!(runtime.commit(commit_marker(psid(10), pid(20), 1)).is_ok());
    }

    #[test]
    fn install_connection_fails_old_pending_commits() {
        let (runtime, _outgoing_rx) = runtime_with_epoch(0);
        runtime
            .register_starting_partition(psid(10), pid(20))
            .expect("start should succeed");

        let mut ack = runtime
            .commit(commit_marker(psid(10), pid(20), 0))
            .expect("commit should be registered");

        let (next_outgoing_tx, _next_outgoing_rx) = mpsc::unbounded_channel();
        runtime
            .install_connection(
                Connection::new(next_outgoing_tx, 1),
                YdbError::custom("test reconnect"),
            )
            .expect("install_connection should succeed");
        runtime
            .register_starting_partition(psid(10), pid(20))
            .expect("start should succeed after reconnect");

        assert!(matches!(ack.try_recv(), Ok(Err(_))));
        assert!(runtime.commit(commit_marker(psid(10), pid(20), 0)).is_err());
        assert!(runtime.commit(commit_marker(psid(10), pid(20), 1)).is_ok());
    }

    #[test]
    fn install_connection_routes_commits_to_new_channel() {
        let (runtime, _old_rx) = runtime_with_epoch(0);

        let (new_tx, mut new_rx) = mpsc::unbounded_channel();
        runtime
            .install_connection(Connection::new(new_tx, 1), YdbError::custom("reconnect"))
            .expect("install_connection should succeed");
        runtime
            .register_starting_partition(psid(10), pid(20))
            .expect("start should succeed after reconnect");

        runtime
            .commit(commit_marker(psid(10), pid(20), 1))
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
        assert!(runtime
            .install_connection(
                Connection::new(next_outgoing_tx, 1),
                YdbError::custom("reconnect")
            )
            .is_err());
        assert!(runtime.commit(commit_marker(psid(10), pid(20), 1)).is_err());
    }

    #[test]
    fn reconnecting_runtime_installs_first_connection() {
        let runtime = RuntimeHandle::new(0);
        assert!(runtime.commit(commit_marker(psid(10), pid(20), 0)).is_err());

        let (outgoing_tx, _outgoing_rx) = mpsc::unbounded_channel();
        runtime
            .install_connection(
                Connection::new(outgoing_tx, 0),
                YdbError::custom("first connection"),
            )
            .expect("install_connection should install connection");
        runtime
            .register_starting_partition(psid(10), pid(20))
            .expect("start should succeed");

        assert!(runtime.commit(commit_marker(psid(10), pid(20), 0)).is_ok());
    }

    #[tokio::test]
    async fn enter_reconnecting_drops_buffered_messages() {
        use crate::client_topic::topicreader::messages::TopicReaderMessage;

        let (runtime, _outgoing_rx) = runtime_with_epoch(0);

        runtime
            .register_starting_partition(psid(1), pid(10))
            .expect("start should succeed");
        runtime
            .push_test_messages(vec![TopicReaderMessage::test_message_full(1, 10, 0, 10)])
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
            .register_starting_partition(psid(1), pid(10))
            .expect("start should succeed");
        runtime
            .push_test_messages(vec![TopicReaderMessage::test_message_full(1, 10, 1, 20)])
            .expect("push should succeed");

        let batch = runtime.pop_batch(10).await.expect("pop should succeed");
        assert_eq!(batch.messages.len(), 1);
        assert_eq!(batch.messages[0].get_commit_marker().epoch, 1);
    }

    #[test]
    fn commit_registers_ack_and_sends_commit_request() {
        let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel();
        let runtime = RuntimeHandle::with_connection(Connection::new(outgoing_tx, 1));
        runtime
            .register_starting_partition(psid(10), pid(20))
            .expect("start should succeed");

        let _ack = runtime
            .commit(commit_marker(psid(10), pid(20), 1))
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
            .register_starting_partition(psid(1), pid(10))
            .expect("start should succeed");
        runtime
            .push_test_messages(vec![TopicReaderMessage::test_message_full(1, 10, 1, 15)])
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
            .register_starting_partition(psid(1), pid(10))
            .expect("start should succeed");
        runtime
            .push_test_messages(vec![TopicReaderMessage::test_message_full(1, 10, 1, 15)])
            .expect("push should succeed");

        let batch = runtime.pop_batch(10).await.expect("pop should succeed");
        assert_eq!(batch.messages.len(), 1);
    }

    #[tokio::test]
    async fn commit_ack_makes_buffered_child_readable() -> YdbResult<()> {
        use crate::client_topic::topicreader::messages::TopicReaderMessage;
        use crate::grpc_wrapper::raw_topic_service::stream_read::messages::{
            RawCommitOffsetResponse, RawPartitionCommittedOffset,
        };

        let (outgoing_tx, _outgoing_rx) = mpsc::unbounded_channel();
        let runtime = RuntimeHandle::with_connection(Connection::new(outgoing_tx, 1));

        runtime.register_starting_partition(psid(1), pid(10))?;
        // test_message_full produces a message at offset 0 → terminal = 1.
        runtime.push_test_messages(vec![TopicReaderMessage::test_message_full(1, 10, 1, 0)])?;
        runtime.register_ending_partition(psid(1), vec![pid(20)])?;
        runtime.register_starting_partition(psid(2), pid(20))?;
        runtime.push_test_messages(vec![TopicReaderMessage::test_message_full(2, 20, 1, 0)])?;

        let parent = runtime.pop_batch(10).await?;
        assert_eq!(
            parent.messages[0].get_commit_marker().partition_session_id,
            psid(1)
        );

        // Parent drained → pending_ending_sessions. Child is blocked until commit ack.
        // Inject the commit ack to release the child.
        runtime.handle_from_server(RawFromServer::CommitOffsetResponse(
            RawCommitOffsetResponse {
                partitions_committed_offsets: vec![RawPartitionCommittedOffset {
                    partition_session_id: 1,
                    committed_offset: 1,
                }],
            },
        ))?;

        let child = runtime.pop_batch(10).await?;
        assert_eq!(
            child.messages[0].get_commit_marker().partition_session_id,
            psid(2)
        );
        Ok(())
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

    fn commit_marker(
        session_id: PartitionSessionId,
        partition_id: PartitionId,
        epoch: usize,
    ) -> TopicReaderCommitMarker {
        TopicReaderCommitMarker {
            partition_session_id: session_id,
            partition_id,
            start_offset: 30,
            end_offset: 40,
            topic: "test-topic".to_string(),
            epoch,
        }
    }
}
