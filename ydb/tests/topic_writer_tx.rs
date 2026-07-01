mod mock_server;

use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use ydb::{
    Client, ClientBuilder, Query, QueryResult, TopicWriterMessage, TopicWriterMessageBuilder,
    TopicWriterTxOptionsBuilder, Transaction, TransactionInfo, YdbResult,
};
use ydb_grpc::ydb_proto::topic::stream_write_message::from_client::ClientMessage as WriteFromClient;
use ydb_grpc::ydb_proto::topic::stream_write_message::InitRequest;
use ydb_grpc::ydb_proto::topic::TransactionIdentity;

use crate::mock_server::handler::{FromHandlerToService, Handler, Incoming, Reply};
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

struct MockTransaction {
    tx_id: String,
    session_id: String,
}

impl MockTransaction {
    fn new(tx_id: impl Into<String>, session_id: impl Into<String>) -> Self {
        Self {
            tx_id: tx_id.into(),
            session_id: session_id.into(),
        }
    }
}

#[async_trait]
impl Transaction for MockTransaction {
    async fn query(&mut self, _: Query) -> YdbResult<QueryResult> {
        unimplemented!()
    }
    async fn commit(&mut self) -> YdbResult<()> {
        Ok(())
    }
    async fn rollback(&mut self) -> YdbResult<()> {
        Ok(())
    }
    async fn transaction_info(&mut self) -> YdbResult<TransactionInfo> {
        Ok(TransactionInfo::new(
            self.tx_id.clone(),
            self.session_id.clone(),
        ))
    }
}

type CapturedTxIdentity = Arc<Mutex<Option<TransactionIdentity>>>;
type CapturedInitRequest = Arc<Mutex<Option<InitRequest>>>;
type CapturedTxVec = Arc<Mutex<Vec<TransactionIdentity>>>;
type CapturedStreamId = Arc<Mutex<Option<u64>>>;

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
}

impl AutoReplyHandler {
    fn new(ack_mode: AckMode) -> (Self, CapturedTxIdentity, CapturedInitRequest) {
        let captured_tx = Arc::new(Mutex::new(None));
        let captured_init = Arc::new(Mutex::new(None));
        let handler = Self {
            replies: ReplySink::default(),
            ack_mode,
            captured_tx_identity: captured_tx.clone(),
            captured_init_request: captured_init.clone(),
        };
        (handler, captured_tx, captured_init)
    }
}

impl Handler for AutoReplyHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies.set_channel(tx);
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
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

fn make_client(server: &MockServer) -> YdbResult<Client> {
    ClientBuilder::new_from_connection_string(format!(
        "{}{DATABASE}?use_discovery=false",
        server.endpoint()
    ))?
    .client()
}

async fn make_writer_tx<'a>(
    server: &MockServer,
    tx: &'a mut dyn Transaction,
) -> YdbResult<ydb::TopicWriterTx> {
    let client = make_client(server)?;
    client
        .topic_client()
        .create_writer_tx(TOPIC_PATH.to_string(), tx)
        .await
}

fn test_message() -> TopicWriterMessage {
    TopicWriterMessageBuilder::default()
        .data(TEST_MESSAGE_DATA.to_vec())
        .build()
        .unwrap()
}

#[tokio::test]
#[tracing_test::traced_test]
async fn write_single_message_written_in_tx() -> YdbResult<()> {
    let (handler, _, _) = AutoReplyHandler::new(AckMode::WrittenInTx);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let mut tx = MockTransaction::new(TX_ID, SESSION_ID);
    let mut writer = make_writer_tx(&server, &mut tx).await?;

    writer.write(test_message()).await?;
    writer.stop().await?;

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn write_wrong_ack_status_returns_error() -> YdbResult<()> {
    let (handler, _, _) = AutoReplyHandler::new(AckMode::Written {
        offset: WRONG_ACK_OFFSET,
    });
    let (server, _reply_tx) = MockServer::start(handler).await;
    let mut tx = MockTransaction::new(TX_ID, SESSION_ID);
    let mut writer = make_writer_tx(&server, &mut tx).await?;

    let result = writer.write(test_message()).await;
    assert!(result.is_err(), "expected error for non-WrittenInTx ack");
    writer.stop().await?;

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn tx_identity_present_in_write_request() -> YdbResult<()> {
    let (handler, captured_tx, _) = AutoReplyHandler::new(AckMode::WrittenInTx);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let mut tx = MockTransaction::new(TX_ID, SESSION_ID);
    let mut writer = make_writer_tx(&server, &mut tx).await?;

    writer.write(test_message()).await?;
    writer.stop().await?;

    let identity = captured_tx.lock().unwrap().clone();
    let identity = identity.expect("WriteRequest.tx must be set for tx writer");
    assert_eq!(identity.id, TX_ID);
    assert_eq!(identity.session, SESSION_ID);

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn regular_writer_sends_no_tx_identity() -> YdbResult<()> {
    let (handler, captured_tx, _) = AutoReplyHandler::new(AckMode::Written {
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
    let (handler, _, captured_init) = AutoReplyHandler::new(AckMode::WrittenInTx);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let mut tx = MockTransaction::new(TX_ID, SESSION_ID);

    let options = TopicWriterTxOptionsBuilder::default()
        .topic_path(TOPIC_PATH.to_string())
        .build()?;

    let client = make_client(&server)?;
    let mut writer = client
        .topic_client()
        .create_writer_tx_with_params(options, &mut tx)
        .await?;

    writer.write(test_message()).await?;
    writer.stop().await?;

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
}

impl ReconnectHandler {
    fn new() -> (Self, CapturedTxVec, CapturedStreamId) {
        let txs = Arc::new(Mutex::new(Vec::new()));
        let stream_id = Arc::new(Mutex::new(None));
        let handler = Self {
            replies: ReplySink::default(),
            captured_txs: txs.clone(),
            captured_stream_id: stream_id.clone(),
        };
        (handler, txs, stream_id)
    }
}

impl Handler for ReconnectHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies.set_channel(tx);
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
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
    let (handler, _, _) = AutoReplyHandler::new(AckMode::SkippedAlreadyWritten);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let mut tx = MockTransaction::new(TX_ID, SESSION_ID);
    let mut writer = make_writer_tx(&server, &mut tx).await?;

    writer.write(test_message()).await?;
    writer.stop().await?;

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn write_errors_in_retriable_err() -> YdbResult<()> {
    let (handler, _, captured_stream_id) = ReconnectHandler::new();
    let (server, _reply_tx) = MockServer::start(handler).await;
    let mut tx = MockTransaction::new(TX_ID, SESSION_ID);
    let mut writer = make_writer_tx(&server, &mut tx).await?;

    let stream_id = captured_stream_id
        .lock()
        .unwrap()
        .expect("stream_id must be set after writer init");
    server
        .write_sender()
        .close(stream_id)
        .expect("mock server failed to fail write stream");

    let result = writer.write(test_message()).await;
    assert!(result.is_err(), "expected error after stream failure");

    let _ = writer.stop().await;

    Ok(())
}
