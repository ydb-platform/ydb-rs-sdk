mod mock_server;

use std::cmp::Ordering;
use std::sync::{Arc, Mutex};

use ydb::{Client, ClientBuilder, RetrySettings, Transaction, TxMode, YdbResult, closure};
use ydb_grpc::ydb_proto::VirtualTimestamp as RawVirtualTimestamp;
use ydb_grpc::ydb_proto::query::{
    CommitTransactionResponse, ExecuteQueryResponsePart, TransactionControl, TransactionMeta,
    transaction_control, transaction_settings,
};
use ydb_grpc::ydb_proto::status_ids::StatusCode;

use crate::mock_server::handler::{FromHandlerToService, Handler, Incoming, ReplySink};
use crate::mock_server::query::{QUERY_TX_ID, QueryIncoming, QueryReply};
use crate::mock_server::server::MockServer;

const DATABASE: &str = "/local";
const WRITE: &str = "UPSERT INTO test (id) VALUES (1)";

async fn make_client(server: &MockServer) -> YdbResult<Client> {
    ClientBuilder::new_from_connection_string(format!(
        "{}{DATABASE}?use_discovery=false",
        server.endpoint()
    ))?
    .build()
    .await
}

fn timestamp() -> RawVirtualTimestamp {
    RawVirtualTimestamp {
        plan_step: u64::MAX,
        tx_id: u64::MAX - 1,
    }
}

fn part(
    commit_timestamp: Option<RawVirtualTimestamp>,
    tx_id: Option<&str>,
) -> ExecuteQueryResponsePart {
    ExecuteQueryResponsePart {
        status: StatusCode::Success as i32,
        commit_timestamp,
        tx_meta: tx_id.map(|id| TransactionMeta { id: id.to_string() }),
        ..Default::default()
    }
}

struct TimestampHandler {
    replies: ReplySink,
    parts: Vec<ExecuteQueryResponsePart>,
    commit_timestamp: Option<RawVirtualTimestamp>,
    controls: Arc<Mutex<Vec<TransactionControl>>>,
}

impl TimestampHandler {
    fn new(
        parts: Vec<ExecuteQueryResponsePart>,
        commit_timestamp: Option<RawVirtualTimestamp>,
    ) -> (Self, Arc<Mutex<Vec<TransactionControl>>>) {
        let controls = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                replies: ReplySink::default(),
                parts,
                commit_timestamp,
                controls: controls.clone(),
            },
            controls,
        )
    }
}

impl Handler for TimestampHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies.set_channel(tx);
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        match incoming {
            Incoming::Query(QueryIncoming::ExecuteQuery(request, stream_id)) => {
                if let Some(control) = request.tx_control {
                    self.controls.lock().unwrap().push(control);
                }
                for part in &self.parts {
                    self.replies.send(QueryReply::ExecuteQuery {
                        stream_id,
                        part: part.clone(),
                    });
                }
                self.replies
                    .send(QueryReply::ExecuteQueryClose { stream_id });
                None
            }
            Incoming::Query(QueryIncoming::CommitTransaction(_, reply_tx)) => {
                let _ = reply_tx.send(Ok(tonic::Response::new(CommitTransactionResponse {
                    status: StatusCode::Success as i32,
                    commit_timestamp: self.commit_timestamp,
                    ..Default::default()
                })));
                None
            }
            other => Some(other),
        }
    }
}

fn assert_strict_mode(control: &TransactionControl, commit: bool) {
    assert_eq!(control.commit_tx, commit);
    assert!(matches!(
        control.tx_selector.as_ref(),
        Some(transaction_control::TxSelector::BeginTx(settings))
            if matches!(settings.tx_mode.as_ref(), Some(transaction_settings::TxMode::StrictSerializableReadWrite(_)))
    ));
}

#[tokio::test]
async fn one_shot_returns_unsigned_trailing_timestamp() -> YdbResult<()> {
    let (handler, controls) =
        TimestampHandler::new(vec![part(None, None), part(Some(timestamp()), None)], None);
    let (server, _) = MockServer::start(handler).await;
    let client = make_client(&server).await?;
    let mut query = client.query_client();
    let value = query
        .exec(WRITE)
        .with_tx_mode(TxMode::StrictSerializableRW)
        .execute_with_commit_timestamp()
        .await?
        .expect("trailing timestamp");
    assert_eq!(value.plan_step(), u64::MAX);
    assert_eq!(value.tx_id(), u64::MAX - 1);
    assert_eq!(value.database(), DATABASE);
    assert_strict_mode(&controls.lock().unwrap()[0], true);

    let mut cloned_query = client
        .clone_with_retry_settings(RetrySettings::dont_retry())
        .query_client();
    let same_scope = cloned_query
        .exec(WRITE)
        .with_tx_mode(TxMode::StrictSerializableRW)
        .execute_with_commit_timestamp()
        .await?
        .expect("trailing timestamp");
    assert_eq!(value.compare(&same_scope)?, Ordering::Equal);
    Ok(())
}

#[tokio::test]
async fn stream_uses_only_final_trailing_timestamp() -> YdbResult<()> {
    let (handler, _) =
        TimestampHandler::new(vec![part(Some(timestamp()), None), part(None, None)], None);
    let (server, _) = MockServer::start(handler).await;
    let client = make_client(&server).await?;
    let mut query = client.query_client();
    let mut stream = query
        .query(WRITE)
        .with_tx_mode(TxMode::StrictSerializableRW)
        .await?;
    assert!(stream.next_result_set().await?.is_none());
    assert!(stream.close_with_commit_timestamp().await?.is_none());

    let (handler, _) =
        TimestampHandler::new(vec![part(None, None), part(Some(timestamp()), None)], None);
    let (server, _) = MockServer::start(handler).await;
    let client = make_client(&server).await?;
    let mut query = client.query_client();
    let mut stream = query
        .query(WRITE)
        .with_tx_mode(TxMode::StrictSerializableRW)
        .await?;
    assert!(stream.next_result_set().await?.is_none());
    assert_eq!(
        stream.close_with_commit_timestamp().await?.unwrap().tx_id(),
        u64::MAX - 1
    );
    Ok(())
}

#[tokio::test]
async fn interactive_query_commit_returns_trailing_timestamp() -> YdbResult<()> {
    let (handler, controls) =
        TimestampHandler::new(vec![part(None, None), part(Some(timestamp()), None)], None);
    let (server, _) = MockServer::start(handler).await;
    let client = make_client(&server).await?;
    let query = client.query_client();
    let observed = query
        .retry_tx(closure!(async |tx: &mut Transaction| {
            Ok(tx
                .exec(WRITE)
                .with_commit(true)
                .execute_with_commit_timestamp()
                .await?)
        }))
        .isolation(TxMode::StrictSerializableRW)
        .await
        .expect("query commit")
        .expect("trailing timestamp");
    assert_eq!(observed.plan_step(), u64::MAX);
    assert_strict_mode(&controls.lock().unwrap()[0], true);
    Ok(())
}

#[tokio::test]
async fn explicit_commit_preserves_optional_timestamp() -> YdbResult<()> {
    for commit_timestamp in [Some(timestamp()), None] {
        let (handler, controls) =
            TimestampHandler::new(vec![part(None, Some(QUERY_TX_ID))], commit_timestamp);
        let (server, _) = MockServer::start(handler).await;
        let client = make_client(&server).await?;
        let query = client.query_client();
        let observed = query
            .retry_tx(closure!(async |tx: &mut Transaction| {
                tx.exec(WRITE).await?;
                let timestamp = tx.commit_with_timestamp().await?;
                assert!(tx.commit_with_timestamp().await?.is_none());
                Ok(timestamp)
            }))
            .isolation(TxMode::StrictSerializableRW)
            .await
            .expect("explicit commit");
        assert_eq!(
            observed.map(|value| (value.plan_step(), value.tx_id())),
            commit_timestamp.map(|value| (value.plan_step, value.tx_id))
        );
        assert_strict_mode(&controls.lock().unwrap()[0], false);
    }
    Ok(())
}
