mod mock_server;

use std::future::{IntoFuture, pending};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::timeout;
use ydb::{
    Client, ClientBuilder, PartitioningStrategy, TopicWriterMessage, TopicWriterOptions,
    Transaction, YdbError, YdbResult, YdbResultWithCustomerErr, closure,
};
use ydb_grpc::ydb_proto::topic::TransactionIdentity;
use ydb_grpc::ydb_proto::topic::stream_write_message::InitRequest;
use ydb_grpc::ydb_proto::topic::stream_write_message::from_client::ClientMessage as WriteFromClient;
use ydb_grpc::ydb_proto::topic::stream_write_message::init_request::Partitioning;
use ydb_grpc::ydb_proto::{
    query::{ExecuteQueryResponsePart, TransactionMeta},
    status_ids::StatusCode,
};

use crate::mock_server::handler::{FromHandlerToService, Handler, Incoming, Reply, ReplySink};
use crate::mock_server::query::{QueryIncoming, QueryReply};
use crate::mock_server::server::MockServer;
use crate::mock_server::topic::{TopicIncoming, builders};

const DATABASE: &str = "/local";
const TOPIC_PATH: &str = "/local/topic";
const TX_ID: &str = "tx-id-abc";
const SESSION_ID: &str = "session-id-xyz";
const WRITE_SESSION_ID: &str = "write-session-id";
const PARTITION_ID: i64 = 0;
const WRONG_ACK_OFFSET: i64 = 42;
const REGULAR_WRITER_OFFSET: i64 = 0;
const TEST_MESSAGE_DATA: &[u8] = b"hello tx";
const PRODUCER_ID: &str = "tx-producer";

type CapturedTxIdentity = Arc<Mutex<Option<TransactionIdentity>>>;
type CapturedInitRequest = Arc<Mutex<Option<InitRequest>>>;
type CapturedTxVec = Arc<Mutex<Vec<TransactionIdentity>>>;
type CapturedOptionalTxVec = Arc<Mutex<Vec<Option<TransactionIdentity>>>>;
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
    Withheld,
}

struct AutoReplyHandler {
    replies: ReplySink,
    ack_mode: AckMode,
    captured_tx_identity: CapturedTxIdentity,
    captured_init_request: CapturedInitRequest,
    tx_lifecycle: CapturedTxLifecycle,
}

struct ReusableWriterHandler {
    replies: ReplySink,
    captured_transactions: CapturedOptionalTxVec,
    transaction_captured: Arc<Notify>,
}

impl ReusableWriterHandler {
    fn new() -> (Self, CapturedOptionalTxVec, Arc<Notify>) {
        let captured_transactions = Arc::new(Mutex::new(Vec::new()));
        let transaction_captured = Arc::new(Notify::new());
        (
            Self {
                replies: ReplySink::default(),
                captured_transactions: captured_transactions.clone(),
                transaction_captured: transaction_captured.clone(),
            },
            captured_transactions,
            transaction_captured,
        )
    }
}

impl Handler for ReusableWriterHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies.set_channel(tx);
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        if let Incoming::Topic(TopicIncoming::StreamWrite { stream_id, msg }) = &incoming {
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
                WriteFromClient::WriteRequest(request) => {
                    self.captured_transactions
                        .lock()
                        .unwrap()
                        .push(request.tx.clone());
                    if request.tx.is_some() {
                        self.transaction_captured.notify_one();
                    }
                    let seq_no = request.messages.first().map_or(1, |message| message.seq_no);
                    let reply = if request.tx.is_some() {
                        builders::write_ack_written_in_tx(stream_id, seq_no)
                    } else {
                        builders::write_ack_written(stream_id, seq_no, REGULAR_WRITER_OFFSET)
                    };
                    self.replies.send(Reply::Topic(reply));
                }
                _ => {}
            }
        }
        Some(incoming)
    }
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

        if let Incoming::Query(QueryIncoming::ExecuteQuery(_, stream_id)) = &incoming {
            self.replies.send(Reply::Query(QueryReply::ExecuteQuery {
                stream_id: *stream_id,
                part: ExecuteQueryResponsePart {
                    status: StatusCode::Success as i32,
                    issues: Vec::new(),
                    result_set_index: 0,
                    result_set: None,
                    exec_stats: None,
                    tx_meta: Some(TransactionMeta {
                        id: TX_ID.to_string(),
                    }),
                },
            }));
            self.replies
                .send(Reply::Query(QueryReply::ExecuteQueryClose {
                    stream_id: *stream_id,
                }));
            return None;
        }

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
                    for message in &req.messages {
                        let reply = match self.ack_mode {
                            AckMode::WrittenInTx => {
                                builders::write_ack_written_in_tx(stream_id, message.seq_no)
                            }
                            AckMode::Written { offset } => {
                                builders::write_ack_written(stream_id, message.seq_no, offset)
                            }
                            AckMode::SkippedAlreadyWritten => {
                                builders::write_ack_skipped_already_written(
                                    stream_id,
                                    message.seq_no,
                                )
                            }
                            AckMode::Withheld => continue,
                        };
                        self.replies.send(Reply::Topic(reply));
                    }
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

async fn make_client(server: &MockServer) -> YdbResult<Client> {
    ClientBuilder::new_from_connection_string(format!(
        "{}{DATABASE}?use_discovery=false",
        server.endpoint()
    ))?
    .build()
    .await
}

fn test_message() -> TopicWriterMessage {
    TopicWriterMessage::builder()
        .data(TEST_MESSAGE_DATA.to_vec())
        .build()
}

#[tokio::test]
#[tracing_test::traced_test]
async fn write_single_message_written_in_tx() -> YdbResult<()> {
    let (handler, _, _, _) = AutoReplyHandler::new(AckMode::WrittenInTx);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;
    let mut writer = client.topic_client().create_writer(TOPIC_PATH).await?;

    client
        .query_client()
        .retry_tx(closure!([&mut writer], async |tx: &mut Transaction| {
            let writer_tx = writer.transactional(tx).await?;
            writer_tx.write(test_message()).await?;
            Ok(())
        }))
        .await?;

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn dropping_bound_writer_fails_commit_instead_of_hanging() -> YdbResult<()> {
    let (handler, _, _, tx_lifecycle) = AutoReplyHandler::new(AckMode::Withheld);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;
    let query_client = client.query_client();

    let result = timeout(
        Duration::from_secs(5),
        query_client
            .retry_tx(closure!([&client], async |tx: &mut Transaction| {
                let mut writer = client.topic_client().create_writer(TOPIC_PATH).await?;
                let writer_tx = writer.transactional(tx).await?;
                writer_tx.write(test_message()).await?;
                Ok(())
            }))
            .into_future(),
    )
    .await
    .expect("transaction commit hung after its topic writer was dropped");

    let error = result.expect_err("transaction commit must fail after its writer is dropped");
    assert!(
        error
            .to_string()
            .contains("topic writer was dropped before pending operations completed")
    );

    let tx_lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(tx_lifecycle.commit_count, 0);
    assert_eq!(tx_lifecycle.rollback_count, 1);

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn transaction_can_execute_query_while_writer_view_exists() -> YdbResult<()> {
    let (handler, _, _, _) = AutoReplyHandler::new(AckMode::WrittenInTx);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;
    let mut writer = client.topic_client().create_writer(TOPIC_PATH).await?;

    client
        .query_client()
        .retry_tx(closure!([&mut writer], async |tx: &mut Transaction| {
            let writer_tx = writer.transactional(tx).await?;
            writer_tx.write(test_message()).await?;
            tx.exec("SELECT 1").await?;
            writer_tx
                .write(TopicWriterMessage::new(b"after query".to_vec()))
                .await?;
            Ok(())
        }))
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
    let client = make_client(&server).await?;
    let mut writer = client.topic_client().create_writer(TOPIC_PATH).await?;

    let result = client
        .query_client()
        .retry_tx(closure!([&mut writer], async |tx: &mut Transaction| {
            let writer_tx = writer.transactional(tx).await?;
            let result = writer_tx.write(test_message()).await;
            result?;
            Ok(())
        }))
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
    let client = make_client(&server).await?;
    let mut writer = client.topic_client().create_writer(TOPIC_PATH).await?;

    client
        .query_client()
        .retry_tx(closure!([&mut writer], async |tx: &mut Transaction| {
            let writer_tx = writer.transactional(tx).await?;
            writer_tx.write(test_message()).await?;
            Ok(())
        }))
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

    let client = make_client(&server).await?;
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
async fn cancelled_transaction_does_not_poison_or_replay_into_regular_writer() -> YdbResult<()> {
    let (handler, captured_transactions, transaction_captured) = ReusableWriterHandler::new();
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;
    let mut writer = client.topic_client().create_writer(TOPIC_PATH).await?;

    let query_client = client.query_client();
    let cancelled_attempt = query_client.retry_tx(closure!(
        [&mut writer, &transaction_captured],
        async |tx: &mut Transaction| {
            let writer_tx = writer.transactional(tx).await?;
            writer_tx.write(test_message()).await?;
            timeout(Duration::from_secs(1), transaction_captured.notified())
                .await
                .map_err(|_| YdbError::Custom("transactional write was not observed".into()))?;
            pending::<YdbResultWithCustomerErr<()>>().await
        }
    ));
    assert!(
        timeout(Duration::from_millis(100), cancelled_attempt.into_future(),)
            .await
            .is_err(),
        "transaction attempt must be cancelled by the test deadline",
    );

    writer
        .write_with_ack(TopicWriterMessage::new(
            b"ordinary after cancellation".to_vec(),
        ))
        .await?;
    writer.stop().await?;

    let captured_transactions = captured_transactions.lock().unwrap();
    assert_eq!(captured_transactions.len(), 2);
    assert!(captured_transactions[0].is_some());
    assert!(captured_transactions[1].is_none());

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn transaction_binding_flushes_ordinary_messages_first() -> YdbResult<()> {
    let (handler, captured_transactions, _transaction_captured) = ReusableWriterHandler::new();
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;
    let mut writer = client.topic_client().create_writer(TOPIC_PATH).await?;

    writer
        .write(TopicWriterMessage::new(
            b"ordinary before transaction".to_vec(),
        ))
        .await?;
    client
        .query_client()
        .retry_tx(closure!([&mut writer], async |tx: &mut Transaction| {
            let writer_tx = writer.transactional(tx).await?;
            writer_tx.write(test_message()).await?;
            Ok(())
        }))
        .await?;
    writer.stop().await?;

    let captured_transactions = captured_transactions.lock().unwrap();
    assert_eq!(captured_transactions.len(), 2);
    assert!(captured_transactions[0].is_none());
    assert!(captured_transactions[1].is_some());

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn tx_writer_with_producer_sends_producer_id() -> YdbResult<()> {
    let (handler, _, captured_init, _) = AutoReplyHandler::new(AckMode::WrittenInTx);
    let (server, _reply_tx) = MockServer::start(handler).await;

    let options = TopicWriterOptions::builder()
        .topic_path(TOPIC_PATH)
        .producer_id(PRODUCER_ID.to_string())
        .partitioning(PartitioningStrategy::PartitionId(PARTITION_ID))
        .build();

    let client = make_client(&server).await?;
    let mut writer = client
        .topic_client()
        .create_writer_with_params(options)
        .await?;
    client
        .query_client()
        .retry_tx(closure!([&mut writer], async |tx: &mut Transaction| {
            let writer_tx = writer.transactional(tx).await?;
            writer_tx.write(test_message()).await?;
            Ok(())
        }))
        .await?;

    let init = captured_init.lock().unwrap().clone();
    let init = init.expect("InitRequest must be captured");
    assert_eq!(init.path, TOPIC_PATH);
    assert_eq!(init.producer_id, PRODUCER_ID);
    assert_eq!(
        init.partitioning,
        Some(Partitioning::PartitionId(PARTITION_ID))
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
async fn write_skipped_already_written_returns_error_and_rolls_back() -> YdbResult<()> {
    let (handler, _, _, tx_lifecycle) = AutoReplyHandler::new(AckMode::SkippedAlreadyWritten);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;
    let mut writer = client.topic_client().create_writer(TOPIC_PATH).await?;

    let result = client
        .query_client()
        .retry_tx(closure!([&mut writer], async |tx: &mut Transaction| {
            let writer_tx = writer.transactional(tx).await?;
            writer_tx.write(test_message()).await?;
            Ok(())
        }))
        .await;

    assert!(result.is_err(), "expected error for AlreadyWritten ack");

    let tx_lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(tx_lifecycle.begin_count, 1);
    assert_eq!(
        tx_lifecycle.rollback_count, 1,
        "AlreadyWritten ack must roll back the query transaction"
    );
    assert_eq!(
        tx_lifecycle.commit_count, 0,
        "AlreadyWritten ack must not commit the query transaction"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn ignored_write_error_rolls_back_and_rebuilds_writer() -> YdbResult<()> {
    let (handler, _, captured_stream_id, tx_lifecycle) = ReconnectHandler::new();
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;
    let mut writer = client.topic_client().create_writer(TOPIC_PATH).await?;

    let result = client
        .query_client()
        .retry_tx(closure!(
            [&mut writer, &captured_stream_id, &server],
            async |tx: &mut Transaction| {
                let writer_tx = writer.transactional(tx).await?;

                let stream_id = captured_stream_id
                    .lock()
                    .unwrap()
                    .expect("stream_id must be set after writer init");
                server
                    .write_sender()
                    .close(stream_id)
                    .expect("mock server failed to fail write stream");

                let _write_result = writer_tx.write(test_message()).await;
                Ok(())
            }
        ))
        .await;

    assert!(result.is_err(), "expected error after stream failure");

    client
        .query_client()
        .retry_tx(closure!([&mut writer], async |tx: &mut Transaction| {
            let writer_tx = writer.transactional(tx).await?;
            writer_tx.write(test_message()).await?;
            Ok(())
        }))
        .await?;

    let tx_lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(tx_lifecycle.begin_count, 2);
    assert_eq!(
        tx_lifecycle.rollback_count, 1,
        "write error must roll back the query transaction"
    );
    assert_eq!(
        tx_lifecycle.commit_count, 1,
        "replacement writer must commit only the second transaction"
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
    let client = make_client(&server).await?;
    let mut writer = client.topic_client().create_writer(TOPIC_PATH).await?;

    let result = client
        .query_client()
        .retry_tx(closure!([&mut writer], async |tx: &mut Transaction| {
            let writer_tx = writer.transactional(tx).await?;
            writer_tx.write(test_message()).await?;
            Ok(())
        }))
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
