#![recursion_limit = "256"]
mod mock_server;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ydb::{Client, ClientBuilder, Transaction, YdbResult, closure};
use ydb_grpc::ydb_proto::query::{ExecuteQueryResponsePart, TransactionMeta};
use ydb_grpc::ydb_proto::status_ids::StatusCode;

use crate::mock_server::handler::{FromHandlerToService, Handler, Incoming, ReplySink};
use crate::mock_server::query::{QUERY_TX_ID, QueryIncoming, QueryReply};
use crate::mock_server::server::MockServer;

const DATABASE: &str = "/local";
const BASIC_UPSERT: &str = "UPSERT INTO test (id, val) VALUES (1, 'x')";

fn make_client(server: &MockServer) -> YdbResult<Client> {
    ClientBuilder::new_from_connection_string(format!(
        "{}{DATABASE}?use_discovery=false",
        server.endpoint()
    ))?
    .client()
}

fn success_part(tx_id: Option<&str>) -> ExecuteQueryResponsePart {
    ExecuteQueryResponsePart {
        status: StatusCode::Success as i32,
        issues: vec![],
        result_set_index: 0,
        result_set: None,
        exec_stats: None,
        tx_meta: tx_id.map(|id| TransactionMeta { id: id.to_string() }),
    }
}

struct FailThenSucceedHandlerMock {
    replies: ReplySink,
    execute_count: Arc<AtomicUsize>,
    failure_code: tonic::Code,
}

impl FailThenSucceedHandlerMock {
    fn new(failure_code: tonic::Code) -> (Self, Arc<AtomicUsize>) {
        let execute_count = Arc::new(AtomicUsize::new(0));
        (
            Self {
                replies: ReplySink::default(),
                execute_count: execute_count.clone(),
                failure_code,
            },
            execute_count,
        )
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

        let call = self.execute_count.fetch_add(1, Ordering::SeqCst);
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

const IDEMPOTENT_ONLY_TRANSPORT_CODES: &[tonic::Code] = &[
    tonic::Code::Internal,
    tonic::Code::Cancelled,
    tonic::Code::Unavailable,
    tonic::Code::Unknown,
];

fn retryable_transport_codes() -> impl Iterator<Item = tonic::Code> {
    ALWAYS_RETRYABLE_TRANSPORT_CODES
        .iter()
        .chain(IDEMPOTENT_ONLY_TRANSPORT_CODES)
        .copied()
}

async fn transport_mock(
    failure_code: tonic::Code,
) -> YdbResult<(Client, Arc<AtomicUsize>, MockServer)> {
    let (handler, execute_count) = FailThenSucceedHandlerMock::new(failure_code);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server)?;
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
            execute_count.load(Ordering::SeqCst),
            2,
            "expected a second ExecuteQuery after {failure_code:?}"
        );
    }
    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn non_idempotent_one_shot_no_retries() -> YdbResult<()> {
    for failure_code in IDEMPOTENT_ONLY_TRANSPORT_CODES.iter() {
        let (client, execute_count, _server) = transport_mock(*failure_code).await?;

        let result = client.query_client().exec(BASIC_UPSERT).await;

        assert!(
            result.is_err(),
            "expected error for {failure_code:?}, got {result:?}"
        );
        assert_eq!(
            execute_count.load(Ordering::SeqCst),
            1,
            "expected a single ExecuteQuery for {failure_code:?}"
        );
    }
    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn no_retries_in_place_inside_tx() -> YdbResult<()> {
    for failure_code in IDEMPOTENT_ONLY_TRANSPORT_CODES.iter().copied() {
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
            execute_count.load(Ordering::SeqCst),
            1,
            "expected a single ExecuteQuery for {failure_code:?}"
        );
    }
    Ok(())
}
