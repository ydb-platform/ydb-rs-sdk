mod mock_server;

use std::sync::{Arc, Mutex};
use ydb::{Client, ClientBuilder, TopicWriterMessage, TopicWriterTxOptionsBuilder, YdbResult};
use ydb_grpc::ydb_proto::topic::stream_write_message::from_client::ClientMessage as WriteFromClient;
use ydb_grpc::ydb_proto::topic::stream_write_message::InitRequest;
use ydb_grpc::ydb_proto::topic::TransactionIdentity;

use crate::mock_server::handler::{FromHandlerToService, Handler, Incoming, Reply};
use crate::mock_server::query::QueryIncoming;
use crate::mock_server::server::MockServer;
use crate::mock_server::topic::{builders, TopicIncoming};

const DATABASE: &str = "/local";
const TOPIC_PATH: &str = "/local/topic";
const TX_ID: &str = "tx-id-abc";
const SESSION_ID: &str = "session-id-xyz";
const WRITE_SESSION_ID: &str = "write-session-id";
const PARTITION_ID: i64 = 0;
const WRONG_ACK_OFFSET: i64 = 42;
const REGULAR_WRITER_OFFSET: i64 = 0;
const TEST_MESSAGE_DATA: &[u8] = b"hello tx";

type CapturedTxIdentity = Arc<Mutex<Option<TransactionIdentity>>>;
type CapturedInitRequest = Arc<Mutex<Option<InitRequest>>>;
type CapturedTxVec = Arc<Mutex<Vec<TransactionIdentity>>>;
type CapturedStreamId = Arc<Mutex<Option<u64>>>;
type CapturedTxLifecycle = Arc<Mutex<TxLifecycle>>;

#[derive(Default)]
struct TxLifecycle {
    begin_count: usize,
    commit_count: usize,
    rollback_count: usize,
}

enum AckMode {
    WrittenInTx,
    Written { offset: i64 },
    SkippedAlreadyWritten,
}

#[derive(Default)]
struct ReplySink {
    tx: Mutex<Option<FromHandlerToService>>,
}

impl ReplySink {
    fn set_channel(&self, tx: FromHandlerToService) {
        *self.tx.lock().unwrap() = Some(tx);
    }

    fn send(&self, reply: Reply) {
        if let Some(tx) = self.tx.lock().unwrap().as_ref() {
            tx.send(reply).expect("mock server failed to send reply");
        }
    }
}

struct AutoReplyHandler {
    replies: ReplySink,
    ack_mode: AckMode,
    captured_tx_identity: CapturedTxIdentity,
    captured_init_request: CapturedInitRequest,
    tx_lifecycle: CapturedTxLifecycle,
}

impl AutoReplyHandler {
    fn new(
        ack_mode: AckMode,
    ) -> (
        Self,
        CapturedTxIdentity,
        CapturedInitRequest,
        CapturedTxLifecycle,
    ) {
        let captured_tx = Arc::new(Mutex::new(None));
        let captured_init = Arc::new(Mutex::new(None));
        let tx_lifecycle = Arc::new(Mutex::new(TxLifecycle::default()));
        let handler = Self {
            replies: ReplySink::default(),
            ack_mode,
            captured_tx_identity: captured_tx.clone(),
            captured_init_request: captured_init.clone(),
            tx_lifecycle: tx_lifecycle.clone(),
        };
        (handler, captured_tx, captured_init, tx_lifecycle)
    }
}

impl Handler for AutoReplyHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies.set_channel(tx);
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        record_tx_lifecycle(&incoming, &self.tx_lifecycle);

        if let Incoming::Topic(TopicIncoming::StreamWrite { stream_id, msg }) = &incoming {
            let stream_id = *stream_id;
            match msg {
                WriteFromClient::InitRequest(req) => {
                    *self.captured_init_request.lock().unwrap() = Some(req.clone());
                    self.replies
                        .send(Reply::Topic(builders::write_init_response(
                            stream_id,
                            WRITE_SESSION_ID,
                            PARTITION_ID,
                        )));
                }
                WriteFromClient::WriteRequest(req) => {
                    *self.captured_tx_identity.lock().unwrap() = req.tx.clone();
                    let seq_no = req.messages.first().map(|m| m.seq_no).unwrap_or(1);
                    let reply = match self.ack_mode {
                        AckMode::WrittenInTx => {
                            builders::write_ack_written_in_tx(stream_id, seq_no)
                        }
                        AckMode::Written { offset } => {
                            builders::write_ack_written(stream_id, seq_no, offset)
                        }
                        AckMode::SkippedAlreadyWritten => {
                            builders::write_ack_skipped_already_written(stream_id, seq_no)
                        }
                    };
                    self.replies.send(Reply::Topic(reply));
                }
                _ => {}
            }
        }
        Some(incoming)
    }
}

fn record_tx_lifecycle(incoming: &Incoming, tx_lifecycle: &CapturedTxLifecycle) {
    match incoming {
        Incoming::Query(QueryIncoming::BeginTransaction(_, _)) => {
            tx_lifecycle.lock().unwrap().begin_count += 1;
        }
        Incoming::Query(QueryIncoming::CommitTransaction(_, _)) => {
            tx_lifecycle.lock().unwrap().commit_count += 1;
        }
        Incoming::Query(QueryIncoming::RollbackTransaction(_, _)) => {
            tx_lifecycle.lock().unwrap().rollback_count += 1;
        }
        _ => {}
    }
}

fn make_client(server: &MockServer) -> YdbResult<Client> {
    ClientBuilder::new_from_connection_string(format!(
        "{}{DATABASE}?use_discovery=false",
        server.endpoint()
    ))?
    .client()
}

fn test_message() -> TopicWriterMessage {
    TopicWriterMessage::from_data(TEST_MESSAGE_DATA)
}

#[tokio::test]
#[tracing_test::traced_test]
async fn write_single_message_written_in_tx() -> YdbResult<()> {
    let (handler, _, _, _) = AutoReplyHandler::new(AckMode::WrittenInTx);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server)?;

    client
        .query_client()
        .retry_tx(async |tx| {
            let mut writer = client
                .topic_client()
                .create_writer_tx(TOPIC_PATH.to_string(), tx)
                .await?;
            writer.write(test_message()).await?;
            writer.stop().await?;
            Ok(())
        })
        .await?;

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn write_wrong_ack_status_returns_error() -> YdbResult<()> {
    let (handler, _, _, tx_lifecycle) = AutoReplyHandler::new(AckMode::Written {
        offset: WRONG_ACK_OFFSET,
    });
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server)?;

    let result = client
        .query_client()
        .retry_tx(async |tx| {
            let mut writer = client
                .topic_client()
                .create_writer_tx(TOPIC_PATH.to_string(), tx)
                .await?;

            let result = writer.write(test_message()).await;
            writer.stop().await?;
            result?;
            Ok(())
        })
        .await;

    assert!(result.is_err(), "expected error for non-WrittenInTx ack");

    let tx_lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(tx_lifecycle.begin_count, 1);
    assert_eq!(
        tx_lifecycle.rollback_count, 1,
        "write error must roll back the query transaction"
    );
    assert_eq!(
        tx_lifecycle.commit_count, 0,
        "failed write must not commit the query transaction"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn tx_identity_present_in_write_request() -> YdbResult<()> {
    let (handler, captured_tx, _, _) = AutoReplyHandler::new(AckMode::WrittenInTx);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server)?;

    client
        .query_client()
        .retry_tx(async |tx| {
            let mut writer = client
                .topic_client()
                .create_writer_tx(TOPIC_PATH.to_string(), tx)
                .await?;
            writer.write(test_message()).await?;
            writer.stop().await?;
            Ok(())
        })
        .await?;

    let identity = captured_tx.lock().unwrap().clone();
    let identity = identity.expect("WriteRequest.tx must be set for tx writer");
    assert_eq!(identity.id, TX_ID);
    assert_eq!(identity.session, SESSION_ID);

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn regular_writer_sends_no_tx_identity() -> YdbResult<()> {
    let (handler, captured_tx, _, _) = AutoReplyHandler::new(AckMode::Written {
        offset: REGULAR_WRITER_OFFSET,
    });
    let (server, _reply_tx) = MockServer::start(handler).await;

    let client = make_client(&server)?;
    let writer = client
        .topic_client()
        .create_writer(TOPIC_PATH.to_string())
        .await?;

    writer.write_with_ack(test_message()).await?;
    writer.stop().await?;

    let identity = captured_tx.lock().unwrap().clone();
    assert!(
        identity.is_none(),
        "regular writer must not set WriteRequest.tx"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn tx_writer_options_propagated_to_init_request() -> YdbResult<()> {
    let (handler, _, captured_init, _) = AutoReplyHandler::new(AckMode::WrittenInTx);
    let (server, _reply_tx) = MockServer::start(handler).await;

    let options = TopicWriterTxOptionsBuilder::default()
        .topic_path(TOPIC_PATH.to_string())
        .build()?;

    let client = make_client(&server)?;
    client
        .query_client()
        .retry_tx(async |tx| {
            let mut writer = client
                .topic_client()
                .create_writer_tx_with_params(options.clone(), tx)
                .await?;
            writer.write(test_message()).await?;
            writer.stop().await?;
            Ok(())
        })
        .await?;

    let init = captured_init.lock().unwrap().clone();
    let init = init.expect("InitRequest must be captured");
    assert_eq!(init.path, TOPIC_PATH);
    assert_eq!(
        init.producer_id, "",
        "tx writer must always use empty producer_id"
    );

    Ok(())
}

struct ReconnectHandler {
    replies: ReplySink,
    captured_txs: CapturedTxVec,
    captured_stream_id: CapturedStreamId,
    tx_lifecycle: CapturedTxLifecycle,
}

impl ReconnectHandler {
    fn new() -> (Self, CapturedTxVec, CapturedStreamId, CapturedTxLifecycle) {
        let txs = Arc::new(Mutex::new(Vec::new()));
        let stream_id = Arc::new(Mutex::new(None));
        let tx_lifecycle = Arc::new(Mutex::new(TxLifecycle::default()));
        let handler = Self {
            replies: ReplySink::default(),
            captured_txs: txs.clone(),
            captured_stream_id: stream_id.clone(),
            tx_lifecycle: tx_lifecycle.clone(),
        };
        (handler, txs, stream_id, tx_lifecycle)
    }
}

impl Handler for ReconnectHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies.set_channel(tx);
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        record_tx_lifecycle(&incoming, &self.tx_lifecycle);

        if let Incoming::Topic(TopicIncoming::StreamWrite { stream_id, msg }) = &incoming {
            let stream_id = *stream_id;
            match msg {
                WriteFromClient::InitRequest(_) => {
                    *self.captured_stream_id.lock().unwrap() = Some(stream_id);
                    self.replies
                        .send(Reply::Topic(builders::write_init_response(
                            stream_id,
                            WRITE_SESSION_ID,
                            PARTITION_ID,
                        )));
                }
                WriteFromClient::WriteRequest(req) => {
                    if let Some(tx) = req.tx.clone() {
                        self.captured_txs.lock().unwrap().push(tx);
                    }
                    let seq_no = req.messages.first().map(|m| m.seq_no).unwrap_or(1);
                    self.replies
                        .send(Reply::Topic(builders::write_ack_written_in_tx(
                            stream_id, seq_no,
                        )));
                }
                _ => {}
            }
        }
        Some(incoming)
    }
}

#[tokio::test]
#[tracing_test::traced_test]
async fn write_skipped_already_written_treated_as_success() -> YdbResult<()> {
    let (handler, _, _, _) = AutoReplyHandler::new(AckMode::SkippedAlreadyWritten);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server)?;

    client
        .query_client()
        .retry_tx(async |tx| {
            let mut writer = client
                .topic_client()
                .create_writer_tx(TOPIC_PATH.to_string(), tx)
                .await?;
            writer.write(test_message()).await?;
            writer.stop().await?;
            Ok(())
        })
        .await?;

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn write_returns_error_after_stream_close_and_rolls_back() -> YdbResult<()> {
    let (handler, _, captured_stream_id, tx_lifecycle) = ReconnectHandler::new();
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server)?;

    let result = client
        .query_client()
        .retry_tx(async |tx| {
            let mut writer = client
                .topic_client()
                .create_writer_tx(TOPIC_PATH.to_string(), tx)
                .await?;

            let stream_id = captured_stream_id
                .lock()
                .unwrap()
                .expect("stream_id must be set after writer init");
            server
                .write_sender()
                .close(stream_id)
                .expect("mock server failed to fail write stream");

            let result = writer.write(test_message()).await;
            let _ = writer.stop().await;
            result?;
            Ok(())
        })
        .await;

    assert!(result.is_err(), "expected error after stream failure");

    let tx_lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(tx_lifecycle.begin_count, 1);
    assert_eq!(
        tx_lifecycle.rollback_count, 1,
        "write error must roll back the query transaction"
    );
    assert_eq!(
        tx_lifecycle.commit_count, 0,
        "failed write must not commit the query transaction"
    );

    Ok(())
}

#[derive(Default)]
struct CommitFailureState {
    begin_count: usize,
    commit_requests: Vec<(String, String)>,
    rollback_count: usize,
    write_txs: Vec<TransactionIdentity>,
}

type SharedCommitFailureState = Arc<Mutex<CommitFailureState>>;

struct CommitFailsHandler {
    replies: ReplySink,
    state: SharedCommitFailureState,
}

impl CommitFailsHandler {
    fn new() -> (Self, SharedCommitFailureState) {
        let state = Arc::new(Mutex::new(CommitFailureState::default()));
        let handler = Self {
            replies: ReplySink::default(),
            state: state.clone(),
        };
        (handler, state)
    }
}

impl Handler for CommitFailsHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies.set_channel(tx);
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        match &incoming {
            Incoming::Query(QueryIncoming::BeginTransaction(_, _)) => {
                self.state.lock().unwrap().begin_count += 1;
            }
            Incoming::Query(QueryIncoming::RollbackTransaction(_, _)) => {
                self.state.lock().unwrap().rollback_count += 1;
            }
            Incoming::Topic(TopicIncoming::StreamWrite { stream_id, msg }) => {
                let stream_id = *stream_id;
                match msg {
                    WriteFromClient::InitRequest(_) => {
                        self.replies
                            .send(Reply::Topic(builders::write_init_response(
                                stream_id,
                                WRITE_SESSION_ID,
                                PARTITION_ID,
                            )));
                    }
                    WriteFromClient::WriteRequest(req) => {
                        if let Some(tx) = req.tx.clone() {
                            self.state.lock().unwrap().write_txs.push(tx);
                        }
                        let seq_no = req.messages.first().map(|m| m.seq_no).unwrap_or(1);
                        self.replies
                            .send(Reply::Topic(builders::write_ack_written_in_tx(
                                stream_id, seq_no,
                            )));
                    }
                    _ => {}
                }
            }
            _ => {}
        }

        match incoming {
            Incoming::Query(QueryIncoming::CommitTransaction(req, reply_tx)) => {
                self.state
                    .lock()
                    .unwrap()
                    .commit_requests
                    .push((req.session_id, req.tx_id));
                let _ = reply_tx.send(Err(tonic::Status::unavailable(
                    "mock commit transaction failed",
                )));
                None
            }
            incoming => Some(incoming),
        }
    }
}

#[tokio::test]
#[tracing_test::traced_test]
async fn commit_failure_after_successful_write_is_not_retried() -> YdbResult<()> {
    let (handler, state) = CommitFailsHandler::new();
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server)?;

    let result = client
        .query_client()
        .retry_tx(async |tx| {
            let mut writer = client
                .topic_client()
                .create_writer_tx(TOPIC_PATH.to_string(), tx)
                .await?;
            writer.write(test_message()).await?;
            writer.stop().await?;
            Ok(())
        })
        .await;

    assert!(result.is_err(), "commit failure must be returned");

    let state = state.lock().unwrap();
    assert_eq!(state.begin_count, 1, "commit failure must not retry tx");
    assert_eq!(
        state.commit_requests,
        vec![(SESSION_ID.to_string(), TX_ID.to_string())]
    );
    assert_eq!(
        state.rollback_count, 0,
        "commit failure outcome is ambiguous and must not be rolled back"
    );
    assert_eq!(state.write_txs.len(), 1);
    assert_eq!(state.write_txs[0].id, TX_ID);
    assert_eq!(state.write_txs[0].session, SESSION_ID);

    Ok(())
}
