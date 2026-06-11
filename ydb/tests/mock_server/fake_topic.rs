use super::{
    commit_offset_response, create_mock_topic_reader, init_response, read_response,
    start_partition_session_request, stop_partition_session_request, MockServer, TopicClientEvent,
};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::debug;
use ydb::{TopicReader, YdbResult};
use ydb_grpc::ydb_proto::topic::stream_read_message;
use ydb_grpc::ydb_proto::topic::stream_read_message::from_client::ClientMessage;

pub struct FakeTopic {
    commands: mpsc::UnboundedSender<FakeTopicCommand>,
    task: tokio::task::JoinHandle<MockServer>,
}

enum FakeTopicCommand {
    Deliver {
        offset: i64,
        payload: Vec<u8>,
    },
    RedeliverUncommitted,
    ExpectCommit {
        start: i64,
        end: i64,
        ack: bool,
        done: oneshot::Sender<()>,
    },
    AckCommittedOffset {
        end: i64,
        done: oneshot::Sender<()>,
    },
    StopPartitionWithoutCommit {
        done: oneshot::Sender<()>,
    },
    Fail {
        status: tonic::Status,
        done: oneshot::Sender<()>,
    },
    AssertNoReconnect {
        quiet_period: Duration,
        done: oneshot::Sender<()>,
    },
    RestartServer {
        done: oneshot::Sender<()>,
    },
}

struct FakeTopicActor {
    server: MockServer,
    state: TopicState,
    current_stream_id: Option<u64>,
    current_partition_session_id: Option<i64>,
    closed_streams: Vec<u64>,
    next_partition_session_id: i64,
    topic_path: String,
    consumer: String,
}

#[derive(Default)]
struct TopicState {
    messages: BTreeMap<i64, Vec<u8>>,
    committed_offset: i64,
}

impl FakeTopic {
    pub async fn new(
        database: impl AsRef<str>,
        topic_path: impl Into<String>,
        consumer: impl Into<String>,
    ) -> YdbResult<(TopicReader, Self)> {
        let topic_path = topic_path.into();
        let consumer = consumer.into();
        let (reader, server) = tokio::time::timeout(
            Duration::from_secs(5),
            create_mock_topic_reader(database, topic_path.clone(), consumer.clone()),
        )
        .await
        .expect("timed out creating TopicReader test context")?;

        let (commands, commands_rx) = mpsc::unbounded_channel();
        let actor = FakeTopicActor {
            server,
            state: TopicState::default(),
            current_stream_id: None,
            current_partition_session_id: None,
            closed_streams: Vec::new(),
            next_partition_session_id: 10,
            topic_path,
            consumer,
        };
        let task = tokio::spawn(actor.run(commands_rx));

        Ok((reader, Self { commands, task }))
    }

    pub async fn deliver(&mut self, offset: i64, payload: &[u8]) {
        self.commands
            .send(FakeTopicCommand::Deliver {
                offset,
                payload: payload.to_vec(),
            })
            .expect("fake topic actor stopped");
    }

    pub async fn redeliver_uncommitted(&mut self) {
        self.commands
            .send(FakeTopicCommand::RedeliverUncommitted)
            .expect("fake topic actor stopped");
    }

    pub async fn expect_next_commit(&mut self, start: i64, end: i64) {
        self.expect_or_ack_next_commit(start, end, false).await;
    }

    pub async fn ack_next_commit(&mut self, start: i64, end: i64) {
        self.expect_or_ack_next_commit(start, end, true).await;
    }

    pub async fn ack_committed_offset(&mut self, end: i64) {
        let (done, wait_done) = oneshot::channel();
        self.commands
            .send(FakeTopicCommand::AckCommittedOffset { end, done })
            .expect("fake topic actor stopped");
        wait_done.await.expect("fake topic actor stopped");
    }

    pub async fn stop_partition_without_commit(&mut self) {
        let (done, wait_done) = oneshot::channel();
        self.commands
            .send(FakeTopicCommand::StopPartitionWithoutCommit { done })
            .expect("fake topic actor stopped");
        wait_done.await.expect("fake topic actor stopped");
    }

    pub async fn fail_retriable(&mut self) {
        self.fail(tonic::Status::aborted("retriable topic stream failure"))
            .await;
    }

    pub async fn fail_idempotent(&mut self) {
        self.fail(tonic::Status::unavailable(
            "idempotent-only topic stream failure",
        ))
        .await;
    }

    pub async fn fail_non_retriable(&mut self) {
        self.fail(tonic::Status::invalid_argument(
            "non-retriable topic stream failure",
        ))
        .await;
    }

    pub async fn assert_no_reconnect(&mut self, quiet_period: Duration) {
        let (done, wait_done) = oneshot::channel();
        self.commands
            .send(FakeTopicCommand::AssertNoReconnect { quiet_period, done })
            .expect("fake topic actor stopped");
        wait_done.await.expect("fake topic actor stopped");
    }

    pub async fn restart_server(&mut self) {
        let (done, wait_done) = oneshot::channel();
        self.commands
            .send(FakeTopicCommand::RestartServer { done })
            .expect("fake topic actor stopped");
        wait_done.await.expect("fake topic actor stopped");
    }

    async fn expect_or_ack_next_commit(&mut self, start: i64, end: i64, ack: bool) {
        let (done, wait_done) = oneshot::channel();
        self.commands
            .send(FakeTopicCommand::ExpectCommit {
                start,
                end,
                ack,
                done,
            })
            .expect("fake topic actor stopped");
        wait_done.await.expect("fake topic actor stopped");
    }

    async fn fail(&mut self, status: tonic::Status) {
        let (done, wait_done) = oneshot::channel();
        self.commands
            .send(FakeTopicCommand::Fail { status, done })
            .expect("fake topic actor stopped");
        wait_done.await.expect("fake topic actor stopped");
    }
}

impl Drop for FakeTopic {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeTopicActor {
    async fn run(mut self, mut commands: mpsc::UnboundedReceiver<FakeTopicCommand>) -> MockServer {
        while let Some(command) = commands.recv().await {
            match command {
                FakeTopicCommand::Deliver { offset, payload } => {
                    self.deliver(offset, payload).await;
                }
                FakeTopicCommand::RedeliverUncommitted => {
                    self.redeliver_uncommitted().await;
                }
                FakeTopicCommand::ExpectCommit {
                    start,
                    end,
                    ack,
                    done,
                } => {
                    self.expect_next_commit(start, end, ack).await;
                    let _ = done.send(());
                }
                FakeTopicCommand::AckCommittedOffset { end, done } => {
                    self.ack_committed_offset(end).await;
                    let _ = done.send(());
                }
                FakeTopicCommand::StopPartitionWithoutCommit { done } => {
                    self.stop_partition_without_commit().await;
                    let _ = done.send(());
                }
                FakeTopicCommand::Fail { status, done } => {
                    self.fail_current_stream(status).await;
                    let _ = done.send(());
                }
                FakeTopicCommand::AssertNoReconnect { quiet_period, done } => {
                    assert_no_stream_read_opened(&mut self.server, quiet_period).await;
                    let _ = done.send(());
                }
                FakeTopicCommand::RestartServer { done } => {
                    self.restart_server().await;
                    let _ = done.send(());
                }
            }
        }

        self.server
    }

    async fn deliver(&mut self, offset: i64, payload: Vec<u8>) {
        self.state.messages.insert(offset, payload.clone());
        self.ensure_reader_ready().await;

        let partition_session_id = self
            .current_partition_session_id
            .expect("reader must have an active partition session");

        debug!(offset, "fake topic delivering message");
        self.server
            .send(read_response(partition_session_id, offset, payload));
    }

    async fn redeliver_uncommitted(&mut self) {
        self.ensure_reader_ready().await;
        let partition_session_id = self
            .current_partition_session_id
            .expect("reader must have an active partition session");

        let uncommitted = self
            .state
            .messages
            .range(self.state.committed_offset..)
            .map(|(offset, payload)| (*offset, payload.clone()))
            .collect::<Vec<_>>();

        assert!(
            !uncommitted.is_empty(),
            "fake topic has no uncommitted messages to redeliver"
        );

        debug!(
            from_offset = self.state.committed_offset,
            count = uncommitted.len(),
            "fake topic redelivering uncommitted messages"
        );
        for (offset, payload) in uncommitted {
            self.server
                .send(read_response(partition_session_id, offset, payload));
        }
    }

    async fn expect_next_commit(&mut self, start: i64, end: i64, ack: bool) {
        self.ensure_reader_ready().await;
        let stream_id = self
            .current_stream_id
            .expect("reader must have an active stream");
        let partition_session_id = self
            .current_partition_session_id
            .expect("reader must have an active partition session");

        loop {
            match expect_client_message_on_stream(&mut self.server, stream_id, &self.closed_streams)
                .await
            {
                ClientMessage::ReadRequest(read) => {
                    debug!(
                        bytes_size = read.bytes_size,
                        "fake topic observed flow-control read request"
                    );
                }
                ClientMessage::CommitOffsetRequest(commit_request) => {
                    assert_commit_request(commit_request, partition_session_id, start, end);
                    if ack {
                        self.ack_committed_offset(end).await;
                    }
                    return;
                }
                other => panic!("expected commit request from topic reader, got {other:?}"),
            }
        }
    }

    async fn ack_committed_offset(&mut self, end: i64) {
        self.ensure_reader_ready().await;
        let partition_session_id = self
            .current_partition_session_id
            .expect("reader must have an active partition session");

        self.state.committed_offset = self.state.committed_offset.max(end);
        debug!(committed_offset = end, "fake topic acknowledging commit");
        self.server
            .send(commit_offset_response(partition_session_id, end));
    }

    async fn stop_partition_without_commit(&mut self) {
        self.ensure_reader_ready().await;
        let stream_id = self
            .current_stream_id
            .expect("reader must have an active stream");
        let partition_session_id = self
            .current_partition_session_id
            .take()
            .expect("reader must have an active partition session");

        debug!(
            partition_session_id,
            "fake topic stopping partition without committed offset"
        );
        self.server.send(stop_partition_session_request(
            partition_session_id,
            false,
            0,
        ));

        let response = expect_stop_partition_session_response_on_stream(
            &mut self.server,
            stream_id,
            &self.closed_streams,
        )
        .await;
        assert_eq!(response.partition_session_id, partition_session_id);
    }

    async fn fail_current_stream(&mut self, status: tonic::Status) {
        self.ensure_reader_ready().await;
        let stream_id = self
            .current_stream_id
            .take()
            .expect("reader must have an active stream");

        debug!(stream_id, ?status, "fake topic failing active stream");
        self.server.fail_stream(status);
        self.closed_streams.push(stream_id);
        self.current_partition_session_id = None;
    }

    async fn restart_server(&mut self) {
        if self.current_stream_id.is_some() {
            self.fail_current_stream(tonic::Status::unavailable("topic server restarting"))
                .await;
        }

        debug!("fake topic restarting mock server");
        self.server.restart().await;
        self.current_stream_id = None;
        self.current_partition_session_id = None;
        self.closed_streams.clear();
        self.next_partition_session_id = 10;
    }

    async fn ensure_reader_ready(&mut self) {
        if self.current_stream_id.is_some() {
            return;
        }

        let partition_session_id = self.next_partition_session_id;
        self.next_partition_session_id += 10;

        debug!(
            partition_session_id,
            committed_offset = self.state.committed_offset,
            "fake topic accepting reader stream"
        );
        let stream_id = expect_reader_started_on_stream(
            &mut self.server,
            &self.closed_streams,
            "read-session",
            partition_session_id,
            self.state.committed_offset,
            &self.topic_path,
            &self.consumer,
        )
        .await;

        self.current_stream_id = Some(stream_id);
        self.current_partition_session_id = Some(partition_session_id);
    }
}

async fn expect_reader_started_on_stream(
    server: &mut MockServer,
    allowed_closed_streams: &[u64],
    session_id: &str,
    partition_session_id: i64,
    committed_offset: i64,
    topic_path: &str,
    consumer: &str,
) -> u64 {
    let stream_id = expect_stream_read_opened_on_stream(server, allowed_closed_streams).await;

    let init = expect_init_request_on_stream(server, stream_id, allowed_closed_streams).await;
    assert_eq!(init.consumer, consumer);
    assert_eq!(init.topics_read_settings[0].path, topic_path);

    let _read = expect_read_request_on_stream(server, stream_id, allowed_closed_streams).await;

    server.send(init_response(session_id));
    server.send(start_partition_session_request(
        partition_session_id,
        topic_path,
        0,
        committed_offset,
    ));

    let start_response = expect_start_partition_session_response_on_stream(
        server,
        stream_id,
        allowed_closed_streams,
    )
    .await;
    assert_eq!(start_response.partition_session_id, partition_session_id);

    stream_id
}

fn assert_commit_request(
    commit_request: stream_read_message::CommitOffsetRequest,
    partition_session_id: i64,
    start: i64,
    end: i64,
) {
    assert_eq!(commit_request.commit_offsets.len(), 1);
    assert_eq!(
        commit_request.commit_offsets[0].partition_session_id,
        partition_session_id
    );
    assert_eq!(commit_request.commit_offsets[0].offsets[0].start, start);
    assert_eq!(commit_request.commit_offsets[0].offsets[0].end, end);
}

async fn expect_stream_read_opened_on_stream(
    server: &mut MockServer,
    allowed_closed_streams: &[u64],
) -> u64 {
    loop {
        match server.next_event().await {
            TopicClientEvent::StreamReadOpened { stream_id } => return stream_id,
            TopicClientEvent::StreamReadClosed { stream_id }
                if allowed_closed_streams.contains(&stream_id) => {}
            TopicClientEvent::StreamReadMessage { stream_id, .. }
                if allowed_closed_streams.contains(&stream_id) => {}
            TopicClientEvent::StreamReadError {
                stream_id,
                status: _,
            } if allowed_closed_streams.contains(&stream_id) => {}
            event => panic!("expected StreamReadOpened, got {event:?}"),
        }
    }
}

async fn expect_init_request_on_stream(
    server: &mut MockServer,
    stream_id: u64,
    allowed_closed_streams: &[u64],
) -> stream_read_message::InitRequest {
    match expect_client_message_on_stream(server, stream_id, allowed_closed_streams).await {
        ClientMessage::InitRequest(value) => value,
        other => panic!("expected InitRequest on stream {stream_id}, got {other:?}"),
    }
}

async fn expect_read_request_on_stream(
    server: &mut MockServer,
    stream_id: u64,
    allowed_closed_streams: &[u64],
) -> stream_read_message::ReadRequest {
    match expect_client_message_on_stream(server, stream_id, allowed_closed_streams).await {
        ClientMessage::ReadRequest(value) => value,
        other => panic!("expected ReadRequest on stream {stream_id}, got {other:?}"),
    }
}

async fn expect_start_partition_session_response_on_stream(
    server: &mut MockServer,
    stream_id: u64,
    allowed_closed_streams: &[u64],
) -> stream_read_message::StartPartitionSessionResponse {
    match expect_client_message_on_stream(server, stream_id, allowed_closed_streams).await {
        ClientMessage::StartPartitionSessionResponse(value) => value,
        other => {
            panic!("expected StartPartitionSessionResponse on stream {stream_id}, got {other:?}")
        }
    }
}

async fn expect_stop_partition_session_response_on_stream(
    server: &mut MockServer,
    stream_id: u64,
    allowed_closed_streams: &[u64],
) -> stream_read_message::StopPartitionSessionResponse {
    match expect_client_message_on_stream(server, stream_id, allowed_closed_streams).await {
        ClientMessage::StopPartitionSessionResponse(value) => value,
        other => {
            panic!("expected StopPartitionSessionResponse on stream {stream_id}, got {other:?}")
        }
    }
}

async fn expect_client_message_on_stream(
    server: &mut MockServer,
    target_stream_id: u64,
    allowed_closed_streams: &[u64],
) -> ClientMessage {
    loop {
        match server.next_event().await {
            TopicClientEvent::StreamReadMessage { stream_id, message }
                if stream_id == target_stream_id =>
            {
                return message
                    .client_message
                    .expect("StreamRead client message must be present");
            }
            TopicClientEvent::StreamReadClosed { stream_id }
                if allowed_closed_streams.contains(&stream_id) => {}
            TopicClientEvent::StreamReadMessage { stream_id, .. }
                if allowed_closed_streams.contains(&stream_id) => {}
            TopicClientEvent::StreamReadError {
                stream_id,
                status: _,
            } if allowed_closed_streams.contains(&stream_id) => {}
            event => {
                panic!("expected client message on stream {target_stream_id}, got {event:?}")
            }
        }
    }
}

async fn assert_no_stream_read_opened(server: &mut MockServer, quiet_period: Duration) {
    loop {
        match tokio::time::timeout(quiet_period, server.next_event()).await {
            Ok(TopicClientEvent::StreamReadOpened { stream_id }) => {
                panic!("unexpected reconnect opened stream {stream_id}")
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
}
