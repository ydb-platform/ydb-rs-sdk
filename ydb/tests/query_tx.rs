//! Mock-server regression and characterization tests for `QueryClient::retry_tx`
//! behavior that is not specific to topic writers.
//!
//! These tests cover the ways a transaction attempt can finish:
//!
//! - no query fails: `retry_tx` sends `CommitTransaction`;
//! - the last query uses `.with_commit(true)`: the query commits the transaction;
//! - a query returns a concrete failure: the whole transaction attempt is retried;
//! - a query returns an ambiguous status: the transaction outcome is unknown;
//! - the caller explicitly rolls back;
//! - rollback or commit RPC outcome is unknown;
//!
//! The regression cases for #521 are the swallowed-error paths: if the callback
//! returns `Ok` after the server invalidated the transaction, or after rollback
//! failed, `retry_tx` must not report a successful commit. A concrete transient query
//! failure ends the current attempt and retries the whole transaction.
#![recursion_limit = "256"]
mod mock_server;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ydb::{Client, ClientBuilder, Transaction, YdbError, YdbResult, YdbStatusError, closure};
use ydb_grpc::ydb_proto::query::{
    CommitTransactionResponse, ExecuteQueryResponsePart, RollbackTransactionResponse,
    TransactionMeta,
};
use ydb_grpc::ydb_proto::status_ids::StatusCode;

use crate::mock_server::handler::{FromHandlerToService, Handler, Incoming, ReplySink};
use crate::mock_server::query::{QUERY_TX_ID, QueryIncoming, QueryReply};
use crate::mock_server::server::MockServer;

const DATABASE: &str = "/local";

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
        tx_meta: tx_id.map(|id| TransactionMeta { id: id.to_string() }),
    }
}

fn failing_part(status: StatusCode) -> ExecuteQueryResponsePart {
    ExecuteQueryResponsePart {
        status: status as i32,
        issues: vec![],
        result_set_index: 0,
        result_set: None,
        exec_stats: None,
        tx_meta: None,
    }
}

fn status_error(status: StatusCode) -> YdbError {
    let mut error = YdbStatusError::default();
    error.message = "test callback error".to_string();
    error.operation_status = status as i32;
    YdbError::YdbStatusError(error)
}

/// Returns `script[call]`, or the script's last entry once `call` runs past the end.
/// An empty script always answers `Success`.
fn scripted_status(script: &[StatusCode], call: usize) -> StatusCode {
    script
        .get(call)
        .copied()
        .unwrap_or_else(|| script.last().copied().unwrap_or(StatusCode::Success))
}

#[derive(Default)]
struct TxLifecycle {
    commit_count: usize,
    rollback_count: usize,
}

type SharedTxLifecycle = Arc<Mutex<TxLifecycle>>;

/// Every `ExecuteQuery` succeeds. A lazy transaction ID arrives after the first response part;
/// a query that commits the transaction omits it. `CommitTransaction` and `RollbackTransaction`
/// are counted and then passed through to the mock's default handler, which replies success for
/// both. Covers T0 (happy path), T1 (commit-via-query), T4 (explicit rollback succeeds), and T6
/// (panic, before/after a real terminal event) — the mock behavior needed is identical across
/// those; only the callback differs.
#[derive(Default)]
struct CountingHandler {
    replies: ReplySink,
    tx_lifecycle: SharedTxLifecycle,
    empty_execute_response: bool,
}

impl CountingHandler {
    fn new() -> (Self, SharedTxLifecycle) {
        Self::with_empty_execute_response(false)
    }

    fn with_empty_execute_response(empty_execute_response: bool) -> (Self, SharedTxLifecycle) {
        let tx_lifecycle = Arc::new(Mutex::new(TxLifecycle::default()));
        let handler = Self {
            replies: ReplySink::default(),
            tx_lifecycle: tx_lifecycle.clone(),
            empty_execute_response,
        };
        (handler, tx_lifecycle)
    }
}

impl Handler for CountingHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies.set_channel(tx);
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        match &incoming {
            Incoming::Query(QueryIncoming::CommitTransaction(_, _)) => {
                self.tx_lifecycle.lock().unwrap().commit_count += 1;
            }
            Incoming::Query(QueryIncoming::RollbackTransaction(_, _)) => {
                self.tx_lifecycle.lock().unwrap().rollback_count += 1;
            }
            _ => {}
        }

        let Incoming::Query(QueryIncoming::ExecuteQuery(request, stream_id)) = incoming else {
            return Some(incoming);
        };
        if !self.empty_execute_response {
            self.replies.send(QueryReply::ExecuteQuery {
                stream_id,
                part: success_part(None),
            });
            if request
                .tx_control
                .as_ref()
                .is_none_or(|control| !control.commit_tx)
            {
                self.replies.send(QueryReply::ExecuteQuery {
                    stream_id,
                    part: success_part(Some(QUERY_TX_ID)),
                });
            }
        }
        self.replies
            .send(QueryReply::ExecuteQueryClose { stream_id });
        None
    }
}

/// `ExecuteQuery` and `RollbackTransaction` each follow a per-call status script (the last
/// entry repeats once exhausted); `CommitTransaction` is counted and passed through to the
/// default handler (always succeeds). Covers every test that needs a specific call in a
/// specific attempt to fail with a specific status: T2/T3 propagate, T3 swallow, T5 propagate.
struct ScriptedQueryHandler {
    replies: ReplySink,
    tx_lifecycle: SharedTxLifecycle,
    execute_call: AtomicUsize,
    execute_statuses: Vec<StatusCode>,
    rollback_call: AtomicUsize,
    rollback_statuses: Vec<StatusCode>,
}

impl ScriptedQueryHandler {
    fn new(
        execute_statuses: Vec<StatusCode>,
        rollback_statuses: Vec<StatusCode>,
    ) -> (Self, SharedTxLifecycle) {
        let tx_lifecycle = Arc::new(Mutex::new(TxLifecycle::default()));
        let handler = Self {
            replies: ReplySink::default(),
            tx_lifecycle: tx_lifecycle.clone(),
            execute_call: AtomicUsize::new(0),
            execute_statuses,
            rollback_call: AtomicUsize::new(0),
            rollback_statuses,
        };
        (handler, tx_lifecycle)
    }
}

impl Handler for ScriptedQueryHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies.set_channel(tx);
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        if let Incoming::Query(QueryIncoming::CommitTransaction(_, _)) = &incoming {
            self.tx_lifecycle.lock().unwrap().commit_count += 1;
        }

        match incoming {
            Incoming::Query(QueryIncoming::ExecuteQuery(_, stream_id)) => {
                let call = self.execute_call.fetch_add(1, Ordering::SeqCst);
                let status = scripted_status(&self.execute_statuses, call);
                let part = if status == StatusCode::Success {
                    success_part(Some(QUERY_TX_ID))
                } else {
                    failing_part(status)
                };
                self.replies
                    .send(QueryReply::ExecuteQuery { stream_id, part });
                self.replies
                    .send(QueryReply::ExecuteQueryClose { stream_id });
                None
            }
            Incoming::Query(QueryIncoming::RollbackTransaction(_, reply_tx)) => {
                self.tx_lifecycle.lock().unwrap().rollback_count += 1;
                let call = self.rollback_call.fetch_add(1, Ordering::SeqCst);
                let status = scripted_status(&self.rollback_statuses, call);
                let _ = reply_tx.send(Ok(tonic::Response::new(RollbackTransactionResponse {
                    status: status as i32,
                    issues: vec![],
                })));
                None
            }
            other => Some(other),
        }
    }
}

/// Every `ExecuteQuery` succeeds; `CommitTransaction` always fails at the transport level
/// (mirrors `topic_writer_tx.rs`'s `CommitFailsHandler`: a raw RPC failure, not a status-coded
/// response, so `need_retry` resolves to `IdempotentOnly` and the default `idempotent(false)`
/// blocks a blind whole-transaction retry after an ambiguous commit).
#[derive(Default)]
struct CommitTransportFailsHandler {
    replies: ReplySink,
    tx_lifecycle: SharedTxLifecycle,
}

impl CommitTransportFailsHandler {
    fn new() -> (Self, SharedTxLifecycle) {
        let tx_lifecycle = Arc::new(Mutex::new(TxLifecycle::default()));
        let handler = Self {
            replies: ReplySink::default(),
            tx_lifecycle: tx_lifecycle.clone(),
        };
        (handler, tx_lifecycle)
    }
}

impl Handler for CommitTransportFailsHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies.set_channel(tx);
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        if let Incoming::Query(QueryIncoming::RollbackTransaction(_, _)) = &incoming {
            self.tx_lifecycle.lock().unwrap().rollback_count += 1;
        }

        match incoming {
            Incoming::Query(QueryIncoming::ExecuteQuery(_, stream_id)) => {
                self.replies.send(QueryReply::ExecuteQuery {
                    stream_id,
                    part: success_part(Some(QUERY_TX_ID)),
                });
                self.replies
                    .send(QueryReply::ExecuteQueryClose { stream_id });
                None
            }
            Incoming::Query(QueryIncoming::CommitTransaction(_, reply_tx)) => {
                self.tx_lifecycle.lock().unwrap().commit_count += 1;
                let _ = reply_tx.send(Err(tonic::Status::unavailable(
                    "mock commit transport failure",
                )));
                None
            }
            other => Some(other),
        }
    }
}

/// Every `ExecuteQuery` succeeds; `CommitTransaction` follows a per-call status script.
struct ScriptedCommitHandler {
    replies: ReplySink,
    tx_lifecycle: SharedTxLifecycle,
    commit_call: AtomicUsize,
    commit_statuses: Vec<StatusCode>,
}

impl ScriptedCommitHandler {
    fn new(commit_statuses: Vec<StatusCode>) -> (Self, SharedTxLifecycle) {
        let tx_lifecycle = Arc::new(Mutex::new(TxLifecycle::default()));
        let handler = Self {
            replies: ReplySink::default(),
            tx_lifecycle: tx_lifecycle.clone(),
            commit_call: AtomicUsize::new(0),
            commit_statuses,
        };
        (handler, tx_lifecycle)
    }
}

impl Handler for ScriptedCommitHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies.set_channel(tx);
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        match incoming {
            Incoming::Query(QueryIncoming::ExecuteQuery(_, stream_id)) => {
                self.replies.send(QueryReply::ExecuteQuery {
                    stream_id,
                    part: success_part(Some(QUERY_TX_ID)),
                });
                self.replies
                    .send(QueryReply::ExecuteQueryClose { stream_id });
                None
            }
            Incoming::Query(QueryIncoming::CommitTransaction(_, reply_tx)) => {
                self.tx_lifecycle.lock().unwrap().commit_count += 1;
                let call = self.commit_call.fetch_add(1, Ordering::SeqCst);
                let status = scripted_status(&self.commit_statuses, call);
                let _ = reply_tx.send(Ok(tonic::Response::new(CommitTransactionResponse {
                    status: status as i32,
                    issues: vec![],
                })));
                None
            }
            other => Some(other),
        }
    }
}

#[tokio::test]
#[tracing_test::traced_test]
async fn happy_path_reports_committed() -> YdbResult<()> {
    let (handler, tx_lifecycle) = CountingHandler::new();
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let result = client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            Ok(())
        }))
        .await;

    assert!(result.is_ok(), "expected success, got {result:?}");
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(lifecycle.commit_count, 1, "a real commit must be sent");
    assert_eq!(lifecycle.rollback_count, 0);
    Ok(())
}

/// A failed `CommitTransaction` has an ambiguous server-side outcome, so `retry_tx`
/// must report the error instead of retrying the whole transaction blindly.
#[tokio::test]
#[tracing_test::traced_test]
async fn commit_rpc_failure_is_reported_and_not_retried() -> YdbResult<()> {
    let (handler, tx_lifecycle) = CommitTransportFailsHandler::new();
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let result = client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            Ok(())
        }))
        .await;

    assert!(
        result.is_err(),
        "a failed commit is ambiguous and must be reported as failure, got {result:?}"
    );
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(
        lifecycle.commit_count, 1,
        "commit outcome is ambiguous, so the whole tx must not be retried"
    );
    assert_eq!(lifecycle.rollback_count, 0);
    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn definitive_commit_failure_retries_whole_transaction() -> YdbResult<()> {
    let (handler, tx_lifecycle) =
        ScriptedCommitHandler::new(vec![StatusCode::Aborted, StatusCode::Success]);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let result = client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            Ok(())
        }))
        .await;

    assert!(result.is_ok(), "expected successful retry, got {result:?}");
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(
        lifecycle.commit_count, 2,
        "the definitive commit failure must retry the whole transaction"
    );
    assert_eq!(lifecycle.rollback_count, 0);
    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn idempotent_transaction_retries_ambiguous_commit_outcome() -> YdbResult<()> {
    let (handler, tx_lifecycle) =
        ScriptedCommitHandler::new(vec![StatusCode::Undetermined, StatusCode::Success]);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let result = client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            Ok(())
        }))
        .idempotent(true)
        .await;

    assert!(result.is_ok(), "expected successful retry, got {result:?}");
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(lifecycle.commit_count, 2);
    assert_eq!(lifecycle.rollback_count, 0);
    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn commit_via_query_reports_committed() -> YdbResult<()> {
    let (handler, tx_lifecycle) = CountingHandler::new();
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let result = client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')")
                .with_commit(true)
                .await?;
            Ok(())
        }))
        .await;

    assert!(result.is_ok(), "expected success, got {result:?}");
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(
        lifecycle.commit_count, 0,
        "commit already happened via the query; no separate RPC expected"
    );
    assert_eq!(lifecycle.rollback_count, 0);
    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn callback_error_after_commit_does_not_retry_transaction() -> YdbResult<()> {
    let (handler, _tx_lifecycle) = CountingHandler::new();
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;
    let attempts = Arc::new(AtomicUsize::new(0));

    let result = client
        .query_client()
        .retry_tx(closure!([&attempts], async |tx: &mut Transaction| {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')")
                .with_commit(true)
                .await?;
            if attempt == 0 {
                return Err(status_error(StatusCode::Unavailable).into());
            }
            Ok(())
        }))
        .await;

    assert!(result.is_err(), "the callback error must be returned");
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "a confirmed commit must never be retried"
    );
    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn empty_commit_via_query_response_is_not_committed() -> YdbResult<()> {
    let (handler, tx_lifecycle) = CountingHandler::with_empty_execute_response(true);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let result = client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')")
                .with_commit(true)
                .await?;
            Ok(())
        }))
        .await;

    assert!(
        result.is_err(),
        "an empty ExecuteQuery response must not confirm commit"
    );
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(lifecycle.commit_count, 0);
    assert_eq!(lifecycle.rollback_count, 0);
    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn swallowed_empty_commit_via_query_response_is_not_committed() -> YdbResult<()> {
    let (handler, tx_lifecycle) = CountingHandler::with_empty_execute_response(true);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let result = client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            let commit = tx
                .exec("UPSERT INTO t (id, val) VALUES (1, 'x')")
                .with_commit(true)
                .await;
            assert!(commit.is_err(), "empty response must fail the query");
            Ok(())
        }))
        .await;

    assert!(
        result.is_err(),
        "swallowing an ambiguous commit error must not report success"
    );
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(lifecycle.commit_count, 0);
    assert_eq!(lifecycle.rollback_count, 0);
    Ok(())
}

/// Once the first `ExecuteQuery` has been dispatched, a failed response can leave a server-side
/// transaction whose ID the SDK never received. Swallowing that error must not turn the local
/// transaction back into an unstarted transaction that can be "committed" without an RPC.
#[tokio::test]
#[tracing_test::traced_test]
async fn swallowed_first_query_failure_is_not_committed_locally() -> YdbResult<()> {
    let (handler, tx_lifecycle) = ScriptedQueryHandler::new(vec![StatusCode::Undetermined], vec![]);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let result = client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            let query = tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await;
            assert!(query.is_err(), "the first query must fail");
            Ok(())
        }))
        .await;

    assert!(
        result.is_err(),
        "an unconfirmed first query must leave the transaction ambiguous"
    );
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(lifecycle.commit_count, 0);
    assert_eq!(lifecycle.rollback_count, 0);
    Ok(())
}

/// If an invalidating query error is propagated, `retry_tx` can retry the whole
/// transaction because the server has already ended the failed attempt.
#[tokio::test]
#[tracing_test::traced_test]
async fn invalidating_error_propagated_is_retried_until_success() -> YdbResult<()> {
    let (handler, tx_lifecycle) =
        ScriptedQueryHandler::new(vec![StatusCode::BadSession, StatusCode::Success], vec![]);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let result = client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            Ok(())
        }))
        .await;

    assert!(result.is_ok(), "expected eventual success, got {result:?}");
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(
        lifecycle.commit_count, 1,
        "only the successful retry attempt should commit"
    );
    assert_eq!(lifecycle.rollback_count, 0);
    Ok(())
}

/// Regression test for https://github.com/ydb-platform/ydb-rs-sdk/issues/521:
/// `retry_tx` must not report a transaction as committed when the server has
/// already invalidated it, even if the user callback swallows the invalidating
/// query error and returns `Ok`.
#[tokio::test]
#[tracing_test::traced_test]
async fn swallowed_invalidating_error_must_not_report_committed() -> YdbResult<()> {
    let (handler, tx_lifecycle) = ScriptedQueryHandler::new(
        vec![StatusCode::Success, StatusCode::PreconditionFailed],
        vec![],
    );
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let result = client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (2, 'x')").await?;

            // Duplicate-key-style conflict: server aborts the transaction.
            let conflict = tx.exec("INSERT INTO t (id, val) VALUES (1, 'dup')").await;

            // The application "handles" the error itself and continues — this is the
            // exact swallow-and-continue path from the issue's repro.
            let _ = conflict;

            Ok(())
        }))
        .await;

    {
        let lifecycle = tx_lifecycle.lock().unwrap();
        assert_eq!(
            lifecycle.commit_count, 0,
            "the server already invalidated the tx; the SDK must not send CommitTransaction"
        );
        assert_eq!(
            lifecycle.rollback_count, 0,
            "the server already invalidated the tx; the SDK must not send RollbackTransaction"
        );
    }

    assert!(
        result.is_err(),
        "retry_tx reported success ({result:?}) for a transaction the server had already \
         aborted, just because the callback swallowed the invalidating query error \
         (https://github.com/ydb-platform/ydb-rs-sdk/issues/521)"
    );

    Ok(())
}

/// A concrete transient query failure ends the current attempt and retries the whole
/// transaction rather than continuing or rolling back that attempt.
#[tokio::test]
#[tracing_test::traced_test]
async fn transient_error_propagated_retries_whole_transaction() -> YdbResult<()> {
    let (handler, tx_lifecycle) = ScriptedQueryHandler::new(
        vec![
            StatusCode::Success,     // attempt 0, 1st query: establishes tx_id
            StatusCode::Unavailable, // attempt 0, 2nd query: transient failure, propagated
            StatusCode::Success,     // attempt 1, 1st query
            StatusCode::Success,     // attempt 1, 2nd query
        ],
        vec![],
    );
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let result = client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            tx.exec("UPSERT INTO t (id, val) VALUES (2, 'y')").await?;
            Ok(())
        }))
        .await;

    assert!(result.is_ok(), "expected eventual success, got {result:?}");
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(
        lifecycle.rollback_count, 0,
        "the failed query already ended the local transaction attempt"
    );
    assert_eq!(
        lifecycle.commit_count, 1,
        "the successful retry attempt must commit"
    );
    Ok(())
}

/// Swallowing a concrete transient query error still retries the whole transaction attempt.
#[tokio::test]
#[tracing_test::traced_test]
async fn transient_error_swallowed_retries_whole_transaction() -> YdbResult<()> {
    let (handler, tx_lifecycle) = ScriptedQueryHandler::new(
        vec![
            StatusCode::Success,
            StatusCode::Unavailable,
            StatusCode::Success,
            StatusCode::Success,
        ],
        vec![],
    );
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let result = client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;

            let transient = tx.exec("UPSERT INTO t (id, val) VALUES (2, 'y')").await;
            let _ = transient; // swallowed

            Ok(())
        }))
        .await;

    assert!(
        result.is_ok(),
        "the retried transaction should succeed: {result:?}"
    );
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(
        lifecycle.commit_count, 1,
        "only the successful retry attempt should commit"
    );
    assert_eq!(
        lifecycle.rollback_count, 0,
        "the concrete query failure ends its transaction attempt"
    );
    Ok(())
}

/// `UNDETERMINED` does not establish whether the dispatched query took effect. Even if the
/// callback swallows it, the SDK must not continue or commit that transaction.
#[tokio::test]
#[tracing_test::traced_test]
async fn swallowed_undetermined_query_error_is_ambiguous() -> YdbResult<()> {
    let (handler, tx_lifecycle) =
        ScriptedQueryHandler::new(vec![StatusCode::Success, StatusCode::Undetermined], vec![]);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let result = client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            let _ = tx.exec("UPSERT INTO t (id, val) VALUES (2, 'y')").await;
            Ok(())
        }))
        .await;

    assert!(result.is_err(), "an unknown transaction outcome must fail");
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(lifecycle.commit_count, 0);
    assert_eq!(lifecycle.rollback_count, 0);
    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn idempotent_transaction_retries_ambiguous_query_outcome() -> YdbResult<()> {
    let (handler, tx_lifecycle) =
        ScriptedQueryHandler::new(vec![StatusCode::Undetermined, StatusCode::Success], vec![]);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;
    let attempts = Arc::new(AtomicUsize::new(0));

    let result = client
        .query_client()
        .retry_tx(closure!([&attempts], async |tx: &mut Transaction| {
            attempts.fetch_add(1, Ordering::SeqCst);
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            Ok(())
        }))
        .idempotent(true)
        .await;

    assert!(result.is_ok(), "expected successful retry, got {result:?}");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(lifecycle.commit_count, 1);
    assert_eq!(lifecycle.rollback_count, 0);
    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn callback_error_does_not_override_ambiguous_transaction() -> YdbResult<()> {
    let (handler, tx_lifecycle) =
        ScriptedQueryHandler::new(vec![StatusCode::Undetermined, StatusCode::Success], vec![]);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;
    let attempts = Arc::new(AtomicUsize::new(0));

    let result = client
        .query_client()
        .retry_tx(closure!([&attempts], async |tx: &mut Transaction| {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            let query = tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await;
            if attempt == 0 {
                assert!(query.is_err(), "the first query must be ambiguous");
                return Err(status_error(StatusCode::Unavailable).into());
            }
            query?;
            Ok(())
        }))
        .await;

    assert!(result.is_err(), "the ambiguous outcome must be returned");
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "a callback error must not make an ambiguous transaction retryable"
    );
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(lifecycle.commit_count, 0);
    assert_eq!(lifecycle.rollback_count, 0);
    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn explicit_rollback_reports_ok_with_real_rollback_rpc() -> YdbResult<()> {
    let (handler, tx_lifecycle) = CountingHandler::new();
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let result = client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            tx.rollback().await?;
            Ok(())
        }))
        .await;

    assert!(
        result.is_ok(),
        "expected Ok(value) per the caller's own rollback: {result:?}"
    );
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(lifecycle.rollback_count, 1);
    assert_eq!(lifecycle.commit_count, 0);
    Ok(())
}

/// If `tx.rollback()` fails and the callback propagates that error, `retry_tx`
/// applies the normal retry policy; here the retried rollback succeeds.
#[tokio::test]
#[tracing_test::traced_test]
async fn rollback_rpc_failure_propagated_is_retried_until_rollback_succeeds() -> YdbResult<()> {
    let (handler, tx_lifecycle) = ScriptedQueryHandler::new(
        vec![StatusCode::Success],
        vec![StatusCode::BadSession, StatusCode::Success],
    );
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let result = client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            tx.rollback().await?;
            Ok(())
        }))
        .await;

    assert!(
        result.is_ok(),
        "the retried attempt's rollback succeeds, so this reflects the caller's own \
         rollback decision: {result:?}"
    );
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(
        lifecycle.rollback_count, 2,
        "first attempt's rollback fails, second attempt's rollback succeeds"
    );
    assert_eq!(lifecycle.commit_count, 0);
    Ok(())
}

/// Regression test, same root cause as https://github.com/ydb-platform/ydb-rs-sdk/issues/521
/// via a different path: once an explicit `RollbackTransaction` RPC fails, the transaction
/// has a terminal but unconfirmed outcome. If the callback swallows that rollback error and
/// returns `Ok`, `retry_tx` must report failure rather than treating the terminal state as a
/// successful commit.
#[tokio::test]
#[tracing_test::traced_test]
async fn swallowed_rollback_failure_must_not_report_committed() -> YdbResult<()> {
    let (handler, tx_lifecycle) =
        ScriptedQueryHandler::new(vec![StatusCode::Success], vec![StatusCode::Undetermined]);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let result = client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            let _ = tx.rollback().await;
            let _ = tx.rollback().await;

            Ok(())
        }))
        .await;

    {
        let lifecycle = tx_lifecycle.lock().unwrap();
        assert_eq!(lifecycle.rollback_count, 1);
        assert_eq!(
            lifecycle.commit_count, 0,
            "commit must never be attempted after rollback reached a terminal state"
        );
    }

    assert!(
        result.is_err(),
        "retry_tx reported success ({result:?}) even though the explicit RollbackTransaction \
         RPC failed and the server-side transaction outcome is unknown"
    );

    Ok(())
}
