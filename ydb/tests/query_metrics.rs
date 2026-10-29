//! Mock-server tests for the query service metrics (cycle 4a).
//!
//! Covers the operation/error/duration/size series for all six operations,
//! the transaction outcome series (commit/rollback/error), and the retry
//! series, all driven end-to-end through the mock query service.

mod mock_server;

use std::time::Duration;

use ydb::{
    Client, ClientBuilder, QueryExecutor, RetrySettings, Transaction, YdbResult,
    YdbResultWithCustomerErr, closure,
};
use ydb_grpc::ydb_proto::status_ids::StatusCode;

use crate::mock_server::handler::{FromHandlerToService, Handler, Incoming, Reply};
use crate::mock_server::metrics::*;
use crate::mock_server::query::parts::status_part;
use crate::mock_server::query::{
    ExecCountingHandler, QueryIncoming, QueryReply, QueryRowHandler, TxQueryRowHandler,
};
use crate::mock_server::server::MockServer;

const ALL_OPERATIONS: [&str; 6] = [
    "exec",
    "query_row",
    "query_result_set",
    "query",
    "execute_script",
    "fetch_script_results",
];

#[tokio::test]
#[tracing_test::traced_test]
async fn operations_recorded_with_result_and_duration() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = QueryRowHandler::new(tokio::sync::mpsc::unbounded_channel().0);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;

    let mut qc = client.query_client();

    qc.exec("SELECT 42 AS val").await?;
    let _row = QueryExecutor::query_row(&mut qc, "SELECT 42 AS val").await?;
    let _set = QueryExecutor::query_result_set(&mut qc, "SELECT 42 AS val").await?;

    // Streaming query: sizes are recorded when the stream is drained and closed.
    let mut stream = QueryExecutor::query(&mut qc, "SELECT 42 AS val").await?;
    while stream.next_result_set().await?.is_some() {}
    stream.close().await?;

    // Script operations go over the unary mock endpoints with default replies.
    let operation = qc
        .execute_script("SELECT 1 AS val")
        .results_ttl(Duration::from_secs(3600))
        .await?;
    let _fetched = qc.fetch_script_results(operation.id).await?;

    for operation in ALL_OPERATIONS {
        assert_eq!(
            counter_value_with_label(
                &registry,
                "ydb_query_operations_total",
                "operation",
                operation
            ),
            1,
            "one successful {operation} call must be counted"
        );
        assert_eq!(
            histogram_sample_count_with_label(
                &registry,
                "ydb_query_operation_duration_milliseconds",
                "operation",
                operation
            ),
            1,
            "one {operation} duration must be recorded"
        );
    }
    assert!(
        histogram_sum(&registry, "ydb_query_operation_duration_milliseconds") > 0.0,
        "ydb_query_operation_duration_milliseconds sum must be positive"
    );

    // Result sizes: every row-carrying operation except execute_script (it
    // returns an operation handle, not rows).
    for operation in ALL_OPERATIONS
        .into_iter()
        .filter(|op| *op != "execute_script")
    {
        assert_eq!(
            histogram_sample_count_with_label(
                &registry,
                "ydb_query_result_rows",
                "operation",
                operation
            ),
            1,
            "one {operation} result-rows sample must be recorded"
        );
    }
    assert!(
        histogram_sum(&registry, "ydb_query_result_rows") > 0.0,
        "ydb_query_result_rows sum must be positive"
    );
    assert!(
        histogram_sum(&registry, "ydb_query_result_bytes") > 0.0,
        "ydb_query_result_bytes sum must be positive"
    );

    Ok(())
}

/// Replies to `ExecuteQuery` with an ABORTED part.
struct AbortedHandler {
    replies: FromHandlerToService,
}

impl Handler for AbortedHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies = tx;
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        let Incoming::Query(QueryIncoming::ExecuteQuery(_, stream_id)) = incoming else {
            return Some(incoming);
        };
        self.replies
            .send(Reply::Query(QueryReply::ExecuteQuery {
                stream_id,
                part: status_part(StatusCode::Aborted),
            }))
            .expect("mock response channel must remain open");
        self.replies
            .send(Reply::Query(QueryReply::ExecuteQueryClose { stream_id }))
            .expect("mock response channel must remain open");
        None
    }
}

#[tokio::test]
#[tracing_test::traced_test]
async fn failed_query_row_records_error_and_status_code() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = AbortedHandler {
        replies: tokio::sync::mpsc::unbounded_channel().0,
    };
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;

    // ABORTED is always retryable; the wall timeout bounds the attempts.
    let mut qc = client.query_client();
    let result = QueryExecutor::query_row(&mut qc, "SELECT 42 AS val")
        .timeout(Duration::from_millis(500))
        .await;
    assert!(result.is_err(), "query_row must fail on ABORTED responses");

    assert!(
        counter_value_with_label(
            &registry,
            "ydb_query_operations_total",
            "operation",
            "query_row",
        ) >= 1,
        "failed query_row attempts must be counted"
    );
    assert!(
        counter_value_with_label(
            &registry,
            "ydb_query_errors_total",
            "operation",
            "query_row"
        ) >= 1,
        "failed query_row attempts must count ydb_query_errors_total"
    );
    assert_eq!(
        counter_value_with_label(
            &registry,
            "ydb_query_errors_total",
            "status_code",
            "ABORTED"
        ),
        counter_value_with_label(
            &registry,
            "ydb_query_errors_total",
            "operation",
            "query_row"
        ),
        "every failed query_row attempt must record the ABORTED status code"
    );

    Ok(())
}

async fn tx_with_client(client: &Client, rollback: bool) -> YdbResultWithCustomerErr<()> {
    client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            QueryExecutor::exec(&mut *tx, "UPSERT INTO t (id) VALUES (1)").await?;
            if rollback {
                tx.rollback().await?;
            }
            Ok(())
        }))
        .await
}

#[tokio::test]
#[tracing_test::traced_test]
async fn transactions_record_commit_and_rollback_outcomes() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = ExecCountingHandler::new(tokio::sync::mpsc::unbounded_channel().0);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;

    tx_with_client(&client, false).await?;
    assert_eq!(
        counter_value_with_label(
            &registry,
            "ydb_query_transactions_total",
            "result",
            "commit"
        ),
        1,
        "a completed retry_tx must be counted as a commit"
    );

    tx_with_client(&client, true).await?;
    assert_eq!(
        counter_value_with_label(
            &registry,
            "ydb_query_transactions_total",
            "result",
            "rollback"
        ),
        1,
        "an explicit rollback must be counted"
    );

    assert_eq!(
        histogram_sample_count(&registry, "ydb_query_transaction_duration_milliseconds"),
        2,
        "each retry_tx outcome must record a transaction duration"
    );
    assert!(
        histogram_sum(&registry, "ydb_query_transaction_duration_milliseconds") > 0.0,
        "ydb_query_transaction_duration_milliseconds sum must be positive"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn named_query_records_common_and_named_series() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    // The transaction-id success part keeps both one-shot and interactive-tx
    // ExecuteQuery calls working (see `TxQueryRowHandler`).
    let handler = TxQueryRowHandler::new(tokio::sync::mpsc::unbounded_channel().0);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;

    let mut qc = client.query_client();

    // Named one-shot call: both the common and the named series move.
    let _row = QueryExecutor::query_row(&mut qc, "SELECT 42 AS val")
        .with_name("named_get")
        .await?;
    // Named streaming query: sizes accumulate into the named histograms too.
    let mut stream = QueryExecutor::query(&mut qc, "SELECT 42 AS val")
        .with_name("named_stream")
        .await?;
    while stream.next_result_set().await?.is_some() {}
    stream.close().await?;
    // Named script operations.
    let operation = qc
        .execute_script("SELECT 1 AS val")
        .with_name("named_script")
        .results_ttl(Duration::from_secs(3600))
        .await?;
    let _fetched = qc
        .fetch_script_results(operation.id)
        .with_name("named_fetch")
        .await?;
    // Named call inside an interactive transaction.
    client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            QueryExecutor::exec(&mut *tx, "UPSERT INTO t (id) VALUES (1)")
                .with_name("named_tx_upsert")
                .await?;
            Ok(())
        }))
        .await?;

    for (name, operations) in [
        ("named_get", 1_u64),
        ("named_stream", 1),
        ("named_script", 1),
        ("named_fetch", 1),
        ("named_tx_upsert", 1),
    ] {
        assert_eq!(
            counter_value_with_label(
                &registry,
                "ydb_query_named_operations_total",
                "query_name",
                name
            ),
            operations,
            "one successful {name} call must be counted in the named series"
        );
        assert_eq!(
            counter_value_with_label(
                &registry,
                "ydb_query_named_operations_total",
                "result",
                "ok"
            ),
            5,
            "every named call must also keep the common operations series moving"
        );
    }
    assert_eq!(
        histogram_sample_count_with_label(
            &registry,
            "ydb_query_named_operation_duration_milliseconds",
            "query_name",
            "named_get"
        ),
        1,
        "one named_get duration must be recorded"
    );
    assert!(
        histogram_sum(&registry, "ydb_query_named_operation_duration_milliseconds") > 0.0,
        "ydb_query_named_operation_duration_milliseconds sum must be positive"
    );
    // Result sizes land in the named histograms (execute_script returns a
    // handle, so named_script has no sizes — the other four do).
    assert_eq!(
        histogram_sample_count_with_label(
            &registry,
            "ydb_query_named_result_rows",
            "query_name",
            "named_get"
        ),
        1
    );
    assert_eq!(
        histogram_sample_count_with_label(
            &registry,
            "ydb_query_named_result_bytes",
            "query_name",
            "named_stream"
        ),
        1
    );
    assert!(
        histogram_sum(&registry, "ydb_query_named_result_bytes") > 0.0,
        "ydb_query_named_result_bytes sum must be positive"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn unnamed_query_writes_only_common_series() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = QueryRowHandler::new(tokio::sync::mpsc::unbounded_channel().0);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;

    let mut qc = client.query_client();
    let _row = QueryExecutor::query_row(&mut qc, "SELECT 42 AS val").await?;

    assert_eq!(
        counter_value_with_label(
            &registry,
            "ydb_query_operations_total",
            "operation",
            "query_row"
        ),
        1,
        "the common series must record the unnamed call"
    );
    assert!(
        !metric_present(&registry, "ydb_query_named_operations_total"),
        "unnamed calls must not create named series"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn failed_named_query_records_named_error_series() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = AbortedHandler {
        replies: tokio::sync::mpsc::unbounded_channel().0,
    };
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;

    let mut qc = client.query_client();
    let result = QueryExecutor::query_row(&mut qc, "SELECT 42 AS val")
        .with_name("named_failing")
        .timeout(Duration::from_millis(500))
        .await;
    assert!(result.is_err(), "query_row must fail on ABORTED responses");

    assert!(
        counter_value_with_label(
            &registry,
            "ydb_query_named_operations_total",
            "query_name",
            "named_failing"
        ) >= 1,
        "failed named attempts must be counted"
    );
    assert!(
        counter_value_with_label(
            &registry,
            "ydb_query_named_errors_total",
            "query_name",
            "named_failing"
        ) >= 1,
        "failed named attempts must count ydb_query_named_errors_total"
    );
    assert_eq!(
        counter_value_with_label(
            &registry,
            "ydb_query_named_errors_total",
            "status_code",
            "ABORTED"
        ),
        counter_value_with_label(
            &registry,
            "ydb_query_named_errors_total",
            "query_name",
            "named_failing"
        ),
        "every failed named attempt must record the ABORTED status code"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn query_name_limit_collapses_to_other_bucket() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = QueryRowHandler::new(tokio::sync::mpsc::unbounded_channel().0);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = ClientBuilder::new_from_connection_string(format!(
        "{}{DATABASE}?use_discovery=false",
        server.endpoint()
    ))?
    .with_metrics_max_query_names(1)
    .with_metrics_recorder(backend(recorder))
    .build()
    .await?;

    let mut qc = client.query_client();
    let _row = QueryExecutor::query_row(&mut qc, "SELECT 42 AS val")
        .with_name("first_named")
        .await?;
    let _row = QueryExecutor::query_row(&mut qc, "SELECT 42 AS val")
        .with_name("second_named")
        .await?;

    assert_eq!(
        counter_value_with_label(
            &registry,
            "ydb_query_named_operations_total",
            "query_name",
            "first_named"
        ),
        1,
        "the first name must be admitted"
    );
    assert_eq!(
        counter_value_with_label(
            &registry,
            "ydb_query_named_operations_total",
            "query_name",
            "second_named"
        ),
        0,
        "a name beyond the limit must not appear as its own series"
    );
    assert_eq!(
        counter_value_with_label(
            &registry,
            "ydb_query_named_operations_total",
            "query_name",
            "__other__"
        ),
        1,
        "names beyond the limit must collapse into the __other__ bucket"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn aborted_transaction_records_error_and_retry() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = AbortedHandler {
        replies: tokio::sync::mpsc::unbounded_channel().0,
    };
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;
    // A no-retry budget keeps the test to a single attempt: the retryable
    // ABORTED is still recorded before the loop gives up.
    let client = client.clone_with_retry_settings(RetrySettings::dont_retry());

    let result = tx_with_client(&client, false).await;
    assert!(
        result.is_err(),
        "retry_tx must fail when exec always aborts"
    );

    assert_eq!(
        counter_value_with_label(&registry, "ydb_query_transactions_total", "result", "error"),
        1,
        "the failed retry_tx must be counted as an error outcome"
    );
    assert_eq!(
        counter_value_with_label(
            &registry,
            "ydb_query_transaction_retries_total",
            "status_code",
            "ABORTED"
        ),
        1,
        "the retryable ABORTED inside the transaction must be recorded as a retry"
    );

    Ok(())
}
