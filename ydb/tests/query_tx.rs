//! Mock-server regression and characterization tests for `QueryClient::retry_tx`
//! behavior that is not specific to topic writers.
//!
//! These tests cover the ways a transaction attempt can finish:
//!
//! - no query fails: `retry_tx` sends `CommitTransaction`;
//! - the last query uses `.with_commit(true)`: the query commits the transaction;
//! - a query returns an invalidating status: the server has already ended the transaction;
//! - a query returns a transient/ambiguous status: the transaction may still be active;
//! - the caller explicitly rolls back;
//! - rollback or commit RPC outcome is unknown;
//! - the callback panics.
//!
//! The regression cases for #521 are the swallowed-error paths: if the callback
//! returns `Ok` after the server invalidated the transaction, or after rollback
//! failed, `retry_tx` must not report a successful commit. Swallowing a transient
//! query error is different: the transaction is not known to be invalidated, so a
//! real commit attempt decides the outcome.
mod mock_server;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ydb::{Client, ClientBuilder, Transaction, YdbOrCustomerError, YdbResult};
use ydb_grpc::ydb_proto::query::{
    ExecuteQueryResponsePart, RollbackTransactionResponse, TransactionMeta,
};
use ydb_grpc::ydb_proto::status_ids::StatusCode;

use crate::mock_server::handler::{FromHandlerToService, Handler, Incoming, Reply};
use crate::mock_server::query::{QUERY_TX_ID, QueryIncoming, QueryReply};
use crate::mock_server::server::MockServer;

const DATABASE: &str = "/local";

fn make_client(server: &MockServer) -> YdbResult<Client> {
    ClientBuilder::new_from_connection_string(format!(
        "{}{DATABASE}?use_discovery=false",
        server.endpoint()
    ))?
    .client()
}

fn panic_callback<T>(message: &'static str) -> Result<T, YdbOrCustomerError> {
    panic!("{message}");
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

#[derive(Default)]
struct ReplySink {
    tx: Mutex<Option<FromHandlerToService>>,
}

impl ReplySink {
    fn set_channel(&self, tx: FromHandlerToService) {
        *self.tx.lock().unwrap() = Some(tx);
    }

    fn send(&self, reply: QueryReply) {
        self.tx
            .lock()
            .unwrap()
            .as_ref()
            .expect("mock query reply channel must be set before replies are sent")
            .send(Reply::Query(reply))
            .expect("mock server failed to send query reply");
    }
}

/// Every `ExecuteQuery` succeeds (handing back `QUERY_TX_ID`); `CommitTransaction` and
/// `RollbackTransaction` are counted and then passed through to the mock's default handler,
/// which replies success for both. Covers T0 (happy path), T1 (commit-via-query), T4
/// (explicit rollback succeeds), and T6 (panic, before/after a real terminal event) — the
/// mock behavior needed is identical across those; only the callback differs.
#[derive(Default)]
struct CountingHandler {
    replies: ReplySink,
    tx_lifecycle: SharedTxLifecycle,
}

impl CountingHandler {
    fn new() -> (Self, SharedTxLifecycle) {
        let tx_lifecycle = Arc::new(Mutex::new(TxLifecycle::default()));
        let handler = Self {
            replies: ReplySink::default(),
            tx_lifecycle: tx_lifecycle.clone(),
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

        let Incoming::Query(QueryIncoming::ExecuteQuery(_, stream_id)) = incoming else {
            return Some(incoming);
        };
        self.replies.send(QueryReply::ExecuteQuery {
            stream_id,
            part: success_part(Some(QUERY_TX_ID)),
        });
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

#[tokio::test]
#[tracing_test::traced_test]
async fn happy_path_reports_committed() -> YdbResult<()> {
    let (handler, tx_lifecycle) = CountingHandler::new();
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server)?;

    let result = client
        .query_client()
        .retry_tx(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            Ok(())
        })
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
    let client = make_client(&server)?;

    let result = client
        .query_client()
        .retry_tx(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            Ok(())
        })
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
async fn commit_via_query_reports_committed() -> YdbResult<()> {
    let (handler, tx_lifecycle) = CountingHandler::new();
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server)?;

    let result = client
        .query_client()
        .retry_tx(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')")
                .with_commit(true)
                .await?;
            Ok(())
        })
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

/// If an invalidating query error is propagated, `retry_tx` can retry the whole
/// transaction because the server has already ended the failed attempt.
#[tokio::test]
#[tracing_test::traced_test]
async fn invalidating_error_propagated_is_retried_until_success() -> YdbResult<()> {
    let (handler, tx_lifecycle) =
        ScriptedQueryHandler::new(vec![StatusCode::BadSession, StatusCode::Success], vec![]);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server)?;

    let result = client
        .query_client()
        .retry_tx(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            Ok(())
        })
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
    let client = make_client(&server)?;

    let result = client
        .query_client()
        .retry_tx(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (2, 'x')").await?;

            // Duplicate-key-style conflict: server aborts the transaction.
            let conflict = tx.exec("INSERT INTO t (id, val) VALUES (1, 'dup')").await;

            // The application "handles" the error itself and continues — this is the
            // exact swallow-and-continue path from the issue's repro.
            let _ = conflict;

            Ok(())
        })
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

/// A transient query error does not prove the server ended the transaction, so a
/// propagated error must trigger a real rollback before retrying.
#[tokio::test]
#[tracing_test::traced_test]
async fn transient_error_propagated_rolls_back_and_retries() -> YdbResult<()> {
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
    let client = make_client(&server)?;

    let result = client
        .query_client()
        .retry_tx(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            tx.exec("UPSERT INTO t (id, val) VALUES (2, 'y')").await?;
            Ok(())
        })
        .await;

    assert!(result.is_ok(), "expected eventual success, got {result:?}");
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(
        lifecycle.rollback_count, 1,
        "the failed first attempt must be rolled back for real"
    );
    assert_eq!(
        lifecycle.commit_count, 1,
        "the successful retry attempt must commit"
    );
    Ok(())
}

/// Swallowing a transient query error is safe from false-commit reporting because
/// `retry_tx` still verifies the final outcome with a real commit attempt.
#[tokio::test]
#[tracing_test::traced_test]
async fn transient_error_swallowed_falls_through_to_real_commit() -> YdbResult<()> {
    let (handler, tx_lifecycle) =
        ScriptedQueryHandler::new(vec![StatusCode::Success, StatusCode::Unavailable], vec![]);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server)?;

    let result = client
        .query_client()
        .retry_tx(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;

            let transient = tx.exec("UPSERT INTO t (id, val) VALUES (2, 'y')").await;
            let _ = transient; // swallowed

            Ok(())
        })
        .await;

    assert!(
        result.is_ok(),
        "the tx was never invalidated, so a real commit should decide the outcome: {result:?}"
    );
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(
        lifecycle.commit_count, 1,
        "retry_tx must still attempt a real commit rather than trusting the swallowed error"
    );
    assert_eq!(
        lifecycle.rollback_count, 0,
        "single attempt, no retry expected"
    );
    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn explicit_rollback_reports_ok_with_real_rollback_rpc() -> YdbResult<()> {
    let (handler, tx_lifecycle) = CountingHandler::new();
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server)?;

    let result = client
        .query_client()
        .retry_tx(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            tx.rollback().await?;
            Ok(())
        })
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
    let client = make_client(&server)?;

    let result = client
        .query_client()
        .retry_tx(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            tx.rollback().await?;
            Ok(())
        })
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
        ScriptedQueryHandler::new(vec![StatusCode::Success], vec![StatusCode::BadSession]);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server)?;

    let result = client
        .query_client()
        .retry_tx(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            let _ = tx.rollback().await;
            let _ = tx.rollback().await;

            Ok(())
        })
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

#[tokio::test]
#[tracing_test::traced_test]
async fn panic_before_any_terminal_event_rolls_back_and_is_not_retried() -> YdbResult<()> {
    let (handler, tx_lifecycle) = CountingHandler::new();
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server)?;

    let result = client
        .query_client()
        .retry_tx::<_, ()>(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            panic_callback("callback exploded before finishing the tx")
        })
        .await;

    assert!(
        result.is_err(),
        "a panicked callback must be reported as failure"
    );
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(
        lifecycle.rollback_count, 1,
        "a real rollback must be attempted"
    );
    assert_eq!(lifecycle.commit_count, 0);
    Ok(())
}

/// Characterizes today's behavior when the callback panics after a real commit-via-query:
/// `retry_tx` reports failure even though the transaction already committed. This is a
/// false negative, not a false commit, and the panic error is non-retryable.
#[tokio::test]
#[tracing_test::traced_test]
async fn panic_after_commit_via_query_reports_failure_despite_real_commit() -> YdbResult<()> {
    let (handler, tx_lifecycle) = CountingHandler::new();
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server)?;

    let result = client
        .query_client()
        .retry_tx::<_, ()>(async |tx: &mut Transaction| {
            tx.exec("UPSERT INTO t (id, val) VALUES (1, 'x')")
                .with_commit(true)
                .await?;
            panic_callback("callback exploded after the tx already committed")
        })
        .await;

    assert!(
        result.is_err(),
        "known false negative: today this reports failure even though the commit-via-query \
         already succeeded before the panic"
    );
    let lifecycle = tx_lifecycle.lock().unwrap();
    assert_eq!(
        lifecycle.commit_count, 0,
        "committed via query, no separate RPC"
    );
    assert_eq!(
        lifecycle.rollback_count, 0,
        "rollback_quiet is a no-op once the transaction is already terminal"
    );
    Ok(())
}
