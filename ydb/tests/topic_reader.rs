mod mock_server;

use prost::bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;
use tokio::sync::Notify;
use ydb::{
    ClientBuilder, Codec, CodecRegistry, TopicReader, TopicReaderBatch, TopicReaderCommitMarker,
    YdbResult,
};
use ydb_grpc::ydb_proto::topic::stream_read_message::from_client::ClientMessage as ReadFromClient;

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
const UNKNOWN_CODEC: Codec = Codec { code: 10001 };

#[derive(Default)]
struct ServerState {
    partition_ready: Notify,
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
                ReadFromClient::InitRequest(_) => {
                    *self.state.stream_id.lock().unwrap() = *stream_id;
                }

                ReadFromClient::StartPartitionSessionResponse(_) => {
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
}

async fn make_reader(server: &MockServer) -> YdbResult<TopicReader> {
    let client = ClientBuilder::new_from_connection_string(format!(
        "{}?database={DATABASE}&use_discovery=false",
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
    let compressed = CodecRegistry::default()
        .compress(&Bytes::copy_from_slice(payload), &Codec::GZIP)?
        .to_vec();
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
