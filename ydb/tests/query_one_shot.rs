mod mock_server;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ydb::{Client, ClientBuilder, QueryExecutor, Transaction, YdbResult, closure};
use ydb_grpc::ydb_proto::query::{ExecuteQueryResponsePart, TransactionMeta};
use ydb_grpc::ydb_proto::status_ids::StatusCode;

use crate::mock_server::handler::{FromHandlerToService, Handler, Incoming, ReplySink};
use crate::mock_server::query::parts::{empty_row_part, row_with_value};
use crate::mock_server::query::{QUERY_TX_ID, QueryIncoming, QueryReply};
use crate::mock_server::server::MockServer;

const DATABASE: &str = "/local";
const BASIC_UPSERT: &str = "UPSERT INTO test (id, val) VALUES (1, 'x')";

async fn make_client(server: &MockServer) -> YdbResult<Client> {
    ClientBuilder::new_from_connection_string(format!(
        "{}{DATABASE}?use_discovery=false",
        server.endpoint()
    ))?
    .build()
    .await
}

fn success_part(tx_id: Option<&str>) -> ExecuteQueryResponsePart {
    ExecuteQueryResponsePart {
        status: StatusCode::Success as i32,
        issues: vec![],
        result_set_index: 0,
        result_set: None,
        exec_stats: None,
        tx_meta: tx_id.map(|id| TransactionMeta { id: id.to_owned() }),
    }
}

struct FailThenSucceedHandlerMock {
    replies: ReplySink,
    execute_count: Arc<AtomicUsize>,
    failure_code: tonic::Code,
}

impl FailThenSucceedHandlerMock {
    fn new(failure_code: tonic::Code) -> Self {
        Self {
            replies: ReplySink::default(),
            execute_count: Arc::new(AtomicUsize::new(0)),
            failure_code,
        }
    }

    fn execute_count_ref(&self) -> &Arc<AtomicUsize> {
        &self.execute_count
    }
}

impl Handler for FailThenSucceedHandlerMock {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies.set_channel(tx);
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        let Incoming::Query(QueryIncoming::ExecuteQuery(_, stream_id)) = incoming else {
            return Some(incoming);
        };

        let call = self.execute_count.fetch_add(1, Ordering::Relaxed);
        if call == 0 {
            self.replies.send(QueryReply::ExecuteQueryFail {
                stream_id,
                status: tonic::Status::new(self.failure_code, "mock execute transport failure"),
            });
        } else {
            self.replies.send(QueryReply::ExecuteQuery {
                stream_id,
                part: success_part(Some(QUERY_TX_ID)),
            });
            self.replies
                .send(QueryReply::ExecuteQueryClose { stream_id });
        }
        None
    }
}

const ALWAYS_RETRYABLE_TRANSPORT_CODES: &[tonic::Code] =
    &[tonic::Code::ResourceExhausted, tonic::Code::Aborted];

/// Transport codes that are retryable only when `.idempotent(true)` is set.
const RETRYABLE_ONLY_IF_IDEMPOTENT: &[tonic::Code] = &[
    tonic::Code::Internal,
    tonic::Code::Cancelled,
    tonic::Code::Unavailable,
    tonic::Code::Unknown,
];

fn retryable_transport_codes() -> impl Iterator<Item = tonic::Code> {
    ALWAYS_RETRYABLE_TRANSPORT_CODES
        .iter()
        .chain(RETRYABLE_ONLY_IF_IDEMPOTENT)
        .copied()
}

async fn transport_mock(
    failure_code: tonic::Code,
) -> YdbResult<(Client, Arc<AtomicUsize>, MockServer)> {
    let handler = FailThenSucceedHandlerMock::new(failure_code);
    let execute_count = handler.execute_count_ref().clone();
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;
    Ok((client, execute_count, server))
}

#[tokio::test]
#[tracing_test::traced_test]
async fn idempotent_one_shot_retries() -> YdbResult<()> {
    for failure_code in retryable_transport_codes() {
        let (client, execute_count, _server) = transport_mock(failure_code).await?;

        let result = client
            .query_client()
            .exec(BASIC_UPSERT)
            .idempotent(true)
            .await;

        assert!(
            result.is_ok(),
            "expected success after retry for {failure_code:?}, got {result:?}"
        );
        assert_eq!(
            execute_count.load(Ordering::Relaxed),
            2,
            "expected a second ExecuteQuery after {failure_code:?}"
        );
    }
    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn non_idempotent_one_shot_no_retries() -> YdbResult<()> {
    // These codes retry only with `.idempotent(true)`; default one-shot must not retry them.
    for failure_code in RETRYABLE_ONLY_IF_IDEMPOTENT.iter() {
        let (client, execute_count, _server) = transport_mock(*failure_code).await?;

        let result = client.query_client().exec(BASIC_UPSERT).await;

        assert!(
            result.is_err(),
            "expected error for {failure_code:?}, got {result:?}"
        );
        assert_eq!(
            execute_count.load(Ordering::Relaxed),
            1,
            "expected a single ExecuteQuery for {failure_code:?}"
        );
    }
    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn no_retries_in_place_inside_tx() -> YdbResult<()> {
    for failure_code in RETRYABLE_ONLY_IF_IDEMPOTENT.iter().copied() {
        let (client, execute_count, _server) = transport_mock(failure_code).await?;

        let result = client
            .query_client()
            .retry_tx(closure!(async |tx: &mut Transaction| {
                tx.exec(BASIC_UPSERT).idempotent(true).await?;
                Ok(())
            }))
            .await;

        assert!(
            result.is_err(),
            "expected error for {failure_code:?}, got {result:?}"
        );
        assert_eq!(
            execute_count.load(Ordering::Relaxed),
            1,
            "expected a single ExecuteQuery for {failure_code:?}"
        );
    }
    Ok(())
}

/// Replies to `ExecuteQuery` with one row (`val=42`).
struct OptionalRowHandler {
    replies: ReplySink,
}

impl Handler for OptionalRowHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies.set_channel(tx);
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        let Incoming::Query(QueryIncoming::ExecuteQuery(_, stream_id)) = incoming else {
            return Some(incoming);
        };
        self.replies.send(QueryReply::ExecuteQuery {
            stream_id,
            part: row_with_value(42),
        });
        self.replies
            .send(QueryReply::ExecuteQueryClose { stream_id });
        None
    }
}

/// Replies to `ExecuteQuery` with one empty result set.
struct EmptyRowHandler {
    replies: ReplySink,
}

impl Handler for EmptyRowHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies.set_channel(tx);
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        let Incoming::Query(QueryIncoming::ExecuteQuery(_, stream_id)) = incoming else {
            return Some(incoming);
        };
        self.replies.send(QueryReply::ExecuteQuery {
            stream_id,
            part: empty_row_part(),
        });
        self.replies
            .send(QueryReply::ExecuteQueryClose { stream_id });
        None
    }
}

#[tokio::test]
#[tracing_test::traced_test]
async fn optional_row_into_future_maps_to_option() -> YdbResult<()> {
    let handler = OptionalRowHandler {
        replies: ReplySink::default(),
    };
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let mut qc = client.query_client();
    let row = QueryExecutor::query_row(&mut qc, "SELECT 42 AS val")
        .optional()
        .await?;

    assert!(row.is_some(), "optional() must map a present row to Some");
    let mut row = row.expect("checked Some");
    let val: i64 = row.remove_field_by_name("val")?.try_into().unwrap();
    assert_eq!(val, 42);

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn optional_row_into_future_returns_none_when_empty() -> YdbResult<()> {
    let handler = EmptyRowHandler {
        replies: ReplySink::default(),
    };
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let mut qc = client.query_client();
    let row = QueryExecutor::query_row(&mut qc, "SELECT 42 AS val")
        .optional()
        .await?;

    assert!(
        row.is_none(),
        "optional() must map an empty result set to None"
    );

    Ok(())
}
