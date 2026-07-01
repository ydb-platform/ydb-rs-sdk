mod mock_server;

use flate2::{write::GzEncoder, Compression};
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;
use tokio::sync::Notify;
use ydb::{
    ClientBuilder, Codec, Executor, TopicReader, TopicReaderBatch, TopicReaderCommitMarker,
    TopicReaderOptionsBuilder, YdbResult,
};
use ydb_grpc::ydb_proto::topic::stream_read_message::from_client::ClientMessage as ReadFromClient;

// Runs compression tasks inline on the calling thread.
// available_parallelism() == 1 keeps the decompressor's chunk_size at the full batch
// size, so a multi-message ReadResponse is forwarded as a single push_batch call.
struct InplaceExecutor;

impl Executor for InplaceExecutor {
    fn available_parallelism(&self) -> std::num::NonZeroUsize {
        const { std::num::NonZeroUsize::new(1).unwrap() }
    }

    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        task();
    }
}

use crate::mock_server::handler::{FromHandlerToService, Handler, Incoming, Reply};
use crate::mock_server::server::MockServer;
use crate::mock_server::topic::{builders, TopicIncoming};

macro_rules! topic_test {
    ($name:ident, timeout_secs = $secs:literal, $body:block) => {
        #[tokio::test]
        #[tracing_test::traced_test]
        async fn $name() -> YdbResult<()> {
            tokio::time::timeout(std::time::Duration::from_secs($secs), async move { $body })
                .await
                .unwrap_or_else(|_| panic!("test {} timed out after {}s", stringify!($name), $secs))
        }
    };
}

const DATABASE: &str = "/local";
const TOPIC_PATH: &str = "/local/topic";
const CONSUMER: &str = "consumer";
const PARTITION_SESSION_ID: i64 = 1;
const PARTITION_SESSION_ID_2: i64 = 2;
const PARTITION_ID_2: i64 = 1;
const UNKNOWN_CODEC: Codec = Codec { code: 10001 };

#[derive(Default)]
struct ServerState {
    partition_ready: Notify,
    partitions_ready: AtomicUsize,
    auto_partitioning_seen: AtomicBool,
    commits_seen: AtomicUsize,
    commits_changed: Notify,
    stream_id: Arc<std::sync::Mutex<u64>>,
}

impl ServerState {
    async fn wait_commits(&self, target: usize) {
        loop {
            let notified = self.commits_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.commits_seen.load(Ordering::SeqCst) >= target {
                return;
            }
            notified.await;
        }
    }

    async fn wait_partitions(&self, target: usize) {
        loop {
            let notified = self.partition_ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.partitions_ready.load(Ordering::SeqCst) >= target {
                return;
            }
            notified.await;
        }
    }

    fn current_stream_id(&self) -> u64 {
        *self.stream_id.lock().unwrap()
    }
}

struct Counter {
    state: Arc<ServerState>,
}

impl Handler for Counter {
    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        if let Incoming::Topic(TopicIncoming::StreamRead { stream_id, msg }) = &incoming {
            match msg {
                ReadFromClient::InitRequest(req) => {
                    self.state
                        .auto_partitioning_seen
                        .store(req.auto_partitioning_support, Ordering::SeqCst);
                    *self.state.stream_id.lock().unwrap() = *stream_id;
                }

                ReadFromClient::StartPartitionSessionResponse(_) => {
                    self.state.partitions_ready.fetch_add(1, Ordering::SeqCst);
                    self.state.partition_ready.notify_waiters();
                }
                ReadFromClient::CommitOffsetRequest(_) => {
                    self.state.commits_seen.fetch_add(1, Ordering::SeqCst);
                    self.state.commits_changed.notify_waiters();
                }
                _ => {}
            }
        }
        Some(incoming)
    }
}

struct Driver {
    server: MockServer,
    reply_tx: FromHandlerToService,
    state: Arc<ServerState>,
}

impl Driver {
    async fn start() -> Self {
        let state = Arc::new(ServerState::default());
        let handler = Counter {
            state: state.clone(),
        };
        let (server, reply_tx) = MockServer::start(handler).await;
        Self {
            server,
            reply_tx,
            state,
        }
    }

    fn send(&self, reply: Reply) {
        self.reply_tx.send(reply).expect("mock server dropped");
    }

    fn send_read_response(&self, offset: i64, payload: impl Into<Vec<u8>>) {
        self.send(Reply::Topic(builders::read_response(
            self.state.current_stream_id(),
            PARTITION_SESSION_ID,
            offset,
            payload,
        )))
    }

    fn send_read_response_with_codec(
        &self,
        offset: i64,
        uncompressed_size: i64,
        payload: impl Into<Vec<u8>>,
        codec: Codec,
    ) {
        self.send(Reply::Topic(builders::read_response_with_codec(
            self.state.current_stream_id(),
            PARTITION_SESSION_ID,
            offset,
            uncompressed_size,
            payload,
            codec,
        )))
    }

    fn send_commit_offset_response(&self, committed_offset: i64) {
        self.send(Reply::Topic(builders::commit_offset_response(
            self.state.current_stream_id(),
            PARTITION_SESSION_ID,
            committed_offset,
        )))
    }

    async fn start_session(&self, partition_session_id: i64, partition_id: i64) {
        let target = self.state.partitions_ready.load(Ordering::SeqCst) + 1;
        self.send(Reply::Topic(builders::start_partition_session_request(
            self.state.current_stream_id(),
            partition_session_id,
            TOPIC_PATH,
            partition_id,
            0,
        )));
        self.state.wait_partitions(target).await;
    }

    fn send_end_partition_session(&self, partition_session_id: i64, child_partition_ids: Vec<i64>) {
        self.send(Reply::Topic(builders::end_partition_session(
            self.state.current_stream_id(),
            partition_session_id,
            child_partition_ids,
        )))
    }
}

async fn make_reader_with_batch_size(
    server: &MockServer,
    batch_size: usize,
) -> YdbResult<TopicReader> {
    let client = ClientBuilder::new_from_connection_string(format!(
        "{}{DATABASE}?use_discovery=false",
        server.endpoint()
    ))?
    .with_executor(Arc::new(InplaceExecutor))
    .client()?;
    let options = TopicReaderOptionsBuilder::default()
        .consumer(CONSUMER.to_string())
        .topic(TOPIC_PATH.into())
        .batch_size(batch_size)
        .build()?;
    client
        .topic_client()
        .create_reader_with_params(options)
        .await
}

async fn make_reader(server: &MockServer) -> YdbResult<TopicReader> {
    let client = ClientBuilder::new_from_connection_string(format!(
        "{}{DATABASE}?use_discovery=false",
        server.endpoint()
    ))?
    .client()?;

    client
        .topic_client()
        .create_reader(CONSUMER.to_string(), TOPIC_PATH.to_string())
        .await
}

async fn assert_single_message_batch(
    mut batch: TopicReaderBatch,
    offset: i64,
    payload: &[u8],
) -> YdbResult<TopicReaderBatch> {
    assert_eq!(batch.messages.len(), 1);
    assert_eq!(batch.messages[0].offset, offset);
    assert_eq!(batch.messages[0].get_topic(), TOPIC_PATH);
    assert_eq!(batch.messages[0].get_partition_id(), 0);
    assert_eq!(
        batch.messages[0].read_and_take().await?.as_deref(),
        Some(payload),
    );
    Ok(batch)
}

async fn deliver_and_read(
    driver: &Driver,
    reader: &mut TopicReader,
    offset: i64,
    payload: &[u8],
) -> YdbResult<TopicReaderCommitMarker> {
    driver.send_read_response(offset, payload);
    let batch = reader.read_batch().await?;
    Ok(assert_single_message_batch(batch, offset, payload)
        .await?
        .get_commit_marker())
}

topic_test!(reads_message, timeout_secs = 1, {
    let driver = Driver::start().await;
    let mut reader = make_reader(&driver.server).await?;
    driver.state.partition_ready.notified().await;

    deliver_and_read(&driver, &mut reader, 0, b"hello").await?;
    Ok(())
});

topic_test!(reads_gzip_message, timeout_secs = 1, {
    let driver = Driver::start().await;
    let mut reader = make_reader(&driver.server).await?;
    driver.state.partition_ready.notified().await;

    let payload = b"hello gzip";
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(payload)?;
    let compressed = encoder.finish()?;
    driver.send_read_response_with_codec(0, payload.len() as i64, compressed, Codec::GZIP);

    let batch = reader.read_batch().await?;
    assert_single_message_batch(batch, 0, payload).await?;

    Ok(())
});

topic_test!(unknown_codec_fails_reader, timeout_secs = 1, {
    let driver = Driver::start().await;
    let mut reader = make_reader(&driver.server).await?;
    driver.state.partition_ready.notified().await;

    driver.send_read_response_with_codec(0, 5, b"hello", UNKNOWN_CODEC);

    assert!(reader.read_batch().await.is_err());

    Ok(())
});

topic_test!(commits_message_after_server_ack, timeout_secs = 2, {
    let driver = Driver::start().await;
    let mut reader = make_reader(&driver.server).await?;
    driver.state.partition_ready.notified().await;

    let m0 = deliver_and_read(&driver, &mut reader, 0, b"first").await?;
    let m1 = deliver_and_read(&driver, &mut reader, 1, b"second").await?;
    let m2 = deliver_and_read(&driver, &mut reader, 2, b"third").await?;

    let c0 = reader.commit_with_ack(m0);
    let c1 = reader.commit_with_ack(m1);
    let mut c2 = Box::pin(reader.commit_with_ack(m2));

    let ack_first_two = async {
        driver.state.wait_commits(2).await;
        driver.send_commit_offset_response(2);
    };

    let (_, r0, r1) = tokio::join!(ack_first_two, c0, c1);
    r0.expect("first commit must resolve");
    r1.expect("second commit must resolve");
    assert!(
        matches!(futures_util::poll!(&mut c2), Poll::Pending),
        "c2 must not have been resolved yet"
    );

    driver.send_commit_offset_response(3);
    c2.await
        .expect("third commit must resolve after second ack");

    Ok(())
});

topic_test!(retryable_fail, timeout_secs = 20, {
    let driver = Driver::start().await;
    let mut reader = make_reader(&driver.server).await?;
    driver.state.partition_ready.notified().await;

    let m0 = deliver_and_read(&driver, &mut reader, 0, b"first").await?;
    let m1 = deliver_and_read(&driver, &mut reader, 1, b"second").await?;

    let c0 = reader.commit_with_ack(m0);
    let mut c1 = Box::pin(reader.commit_with_ack(m1));

    let ack_first = async {
        driver.state.wait_commits(1).await;
        driver.send_commit_offset_response(1);
    };

    let (_, r0) = tokio::join!(ack_first, c0);
    r0.expect("first commit must resolve");
    assert!(
        matches!(futures_util::poll!(&mut c1), Poll::Pending),
        "c1 must not have resolved yet"
    );

    let stream_id = driver.state.current_stream_id();

    let fail_msg = builders::empty_with_status(
        stream_id,
        ydb_grpc::ydb_proto::status_ids::StatusCode::Unavailable,
    );

    driver.send(Reply::Topic(fail_msg));

    let new_stream = async {
        driver.state.partition_ready.notified().await;
        driver.send_read_response(1, b"second");
    };

    let (batch, _, r1) = tokio::join!(reader.read_batch(), new_stream, c1);

    let batch = batch.expect("Topic Reader should not fail on retryable error");

    let _ = assert_single_message_batch(batch, 1, b"second").await;

    assert!(r1.is_err());

    Ok(())
});

topic_test!(
    commit_after_partition_stop_must_resolve,
    timeout_secs = 5,
    {
        let driver = Driver::start().await;
        let mut reader = make_reader(&driver.server).await?;
        driver.state.partition_ready.notified().await;

        let m0 = deliver_and_read(&driver, &mut reader, 0, b"first").await?;

        let stream_id = driver.state.current_stream_id();
        driver.send(Reply::Topic(builders::stop_partition_session_request(
            stream_id,
            PARTITION_SESSION_ID,
            /* graceful */ false,
            /* committed_offset */ 0,
        )));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert!(
            reader.commit_with_ack(m0).await.is_err(),
            "commit on stopped partition session must return Err"
        );

        Ok(())
    }
);

topic_test!(
    read_batch_after_partition_stop_skips_stopped_session,
    timeout_secs = 5,
    {
        let driver = Driver::start().await;
        let mut reader = make_reader(&driver.server).await?;
        driver.state.partition_ready.notified().await;

        driver.send_read_response(0, b"buffered");
        // Let the message reach the runtime buffer before the stop arrives.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let stream_id = driver.state.current_stream_id();
        driver.send(Reply::Topic(builders::stop_partition_session_request(
            stream_id,
            PARTITION_SESSION_ID,
            /* graceful */ false,
            /* committed_offset */ 0,
        )));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let result =
            tokio::time::timeout(std::time::Duration::from_millis(300), reader.read_batch()).await;

        if let Ok(Ok(batch)) = result {
            panic!(
                "read_batch returned {} message(s) from a stopped partition session",
                batch.messages.len()
            );
        }

        Ok(())
    }
);

topic_test!(non_retryable_fail, timeout_secs = 20, {
    let driver = Driver::start().await;
    let mut reader = make_reader(&driver.server).await?;
    driver.state.partition_ready.notified().await;

    let m0 = deliver_and_read(&driver, &mut reader, 0, b"first").await?;
    let c0 = reader.commit_with_ack(m0);

    let stream_id = driver.state.current_stream_id();

    let fail_msg = builders::empty_with_status(
        stream_id,
        ydb_grpc::ydb_proto::status_ids::StatusCode::InternalError,
    );

    driver.send(Reply::Topic(fail_msg));

    assert!(reader.read_batch().await.is_err());
    assert!(c0.await.is_err());

    Ok(())
});

// TODO: Test TopicReader Token recieving before restart, Token recieving after restart.

topic_test!(
    auto_partitioning_support_is_set_in_init_request,
    timeout_secs = 1,
    {
        let driver = Driver::start().await;
        let _reader = make_reader(&driver.server).await?;
        driver.state.wait_partitions(1).await;
        assert!(
            driver.state.auto_partitioning_seen.load(Ordering::SeqCst),
            "reader must set auto_partitioning_support=true in InitRequest"
        );
        Ok(())
    }
);

topic_test!(round_robin_interleaves_two_partitions, timeout_secs = 2, {
    let driver = Driver::start().await;
    let mut reader = make_reader_with_batch_size(&driver.server, 1).await?;
    driver.state.wait_partitions(1).await;
    driver
        .start_session(PARTITION_SESSION_ID_2, PARTITION_ID_2)
        .await;

    let sid = driver.state.current_stream_id();
    // Send both p0 messages atomically so they land in session 1's buffer before the
    // first notify fires. Separate sends would race: only p0-first buffered when
    // read_batch(1) first runs, leaving session 1 with a queued p0-second that RR
    // would serve before switching — making RR indistinguishable from FIFO.
    driver.send(Reply::Topic(builders::read_response_batch_with_codec(
        sid,
        PARTITION_SESSION_ID,
        vec![(0, 8, b"p0-first".to_vec()), (1, 9, b"p0-second".to_vec())],
        Codec::RAW,
    )));
    driver.send(Reply::Topic(builders::read_response(
        sid,
        PARTITION_SESSION_ID_2,
        0,
        b"p1-only",
    )));

    let b1 = reader.read_batch().await?;
    let b2 = reader.read_batch().await?;
    let b3 = reader.read_batch().await?;
    assert_eq!(b1.messages[0].get_partition_id(), 0);
    assert_eq!(
        b2.messages[0].get_partition_id(),
        1,
        "RR must switch to p1 before returning to p0-second"
    );
    assert_eq!(b3.messages[0].get_partition_id(), 0);
    Ok(())
});

topic_test!(
    end_partition_session_child_blocked_until_parent_drained,
    timeout_secs = 2,
    {
        let driver = Driver::start().await;
        let mut reader = make_reader(&driver.server).await?;
        driver.state.wait_partitions(1).await;

        driver.send_read_response(0, b"parent-message");
        driver.send_end_partition_session(PARTITION_SESSION_ID, vec![PARTITION_ID_2]);
        driver
            .start_session(PARTITION_SESSION_ID_2, PARTITION_ID_2)
            .await;

        // Parent message must come first.
        let b1 = reader.read_batch().await?;
        assert_eq!(
            b1.messages[0].get_partition_id(),
            0,
            "parent must come first"
        );

        // After draining parent, drain_ending removes the block on child_pid. Child session
        // is not yet known to the buffer (no push_batch for session 2 yet), so child is not
        // added to RR here. When child data arrives, push_batch adds it to RR and notifies.
        let sid = driver.state.current_stream_id();
        let (b2_result, ()) = tokio::join!(reader.read_batch(), async {
            tokio::task::yield_now().await;
            driver.send(Reply::Topic(builders::read_response(
                sid,
                PARTITION_SESSION_ID_2,
                0,
                b"child-message",
            )));
        });
        assert_eq!(b2_result?.messages[0].get_partition_id(), 1);
        Ok(())
    }
);

topic_test!(
    end_partition_session_no_children_reader_stays_healthy,
    timeout_secs = 2,
    {
        let driver = Driver::start().await;
        let mut reader = make_reader(&driver.server).await?;
        driver.state.wait_partitions(1).await;

        deliver_and_read(&driver, &mut reader, 0, b"before-end").await?;
        driver.send_end_partition_session(PARTITION_SESSION_ID, vec![]);
        driver
            .start_session(PARTITION_SESSION_ID_2, PARTITION_ID_2)
            .await;

        let sid = driver.state.current_stream_id();
        driver.send(Reply::Topic(builders::read_response(
            sid,
            PARTITION_SESSION_ID_2,
            0,
            b"after-end",
        )));

        let batch = reader.read_batch().await?;
        assert_eq!(batch.messages.len(), 1);
        assert_eq!(batch.messages[0].get_partition_id(), 1);
        Ok(())
    }
);
