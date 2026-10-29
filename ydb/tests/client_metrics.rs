//! Mock-server tests for the part1 client metrics: creation counters, static
//! labels, operation counters, and row-query-time histograms.
//!
//! Each test injects an isolated `metrics_prometheus::Recorder` backed by its own
//! private `prometheus::Registry` via `ClientBuilder::with_metrics_recorder`, so the
//! tests run in parallel and never touch the process-global recorder.
//! Custom static labels attached via `ClientBuilder::with_metrics_label(s)` are
//! covered here too, including the reserved-key and duplicate-key validation.
#![recursion_limit = "256"]

mod mock_server;

use ydb::{ClientBuilder, QueryExecutor, Transaction, YdbError, YdbResult, closure};

use crate::mock_server::metrics::*;
use crate::mock_server::query::{ExecCountingHandler, QueryRowHandler, TxQueryRowHandler};
use crate::mock_server::server::MockServer;

#[tokio::test]
#[tracing_test::traced_test]
async fn happy_path_collect_metrics() -> YdbResult<()> {
    struct DummyHandler;
    impl crate::mock_server::handler::Handler for DummyHandler {}

    let (registry, recorder) = test_recorder();
    let (server, _reply_tx) = MockServer::start(DummyHandler).await;
    let _ = make_client(&server, recorder).await?;

    let metrics_vec = registry.gather();
    assert!(!metrics_vec.is_empty());

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn client_creation_increments_counter_metric() -> YdbResult<()> {
    struct DummyHandler;
    impl crate::mock_server::handler::Handler for DummyHandler {}

    let (registry, recorder) = test_recorder();
    let (server, _reply_tx) = MockServer::start(DummyHandler).await;
    let client = make_client(&server, recorder).await?;

    assert_eq!(
        counter_value(&registry, "ydb_new_client_counter"),
        1,
        "ClientBuilder::build must increment ydb_new_client_counter by exactly 1"
    );

    // The other four creation counters each fire on the matching accessor.
    for (accessor, metric) in [
        ("table", "ydb_new_table_client_counter"),
        ("query", "ydb_new_query_client_counter"),
        ("scheme", "ydb_new_scheme_client_counter"),
        ("topic", "ydb_new_topic_client_counter"),
    ] {
        let before = counter_value(&registry, metric);
        match accessor {
            "table" => {
                let _ = client.table_client();
            }
            "query" => {
                let _ = client.query_client();
            }
            "scheme" => {
                let _ = client.scheme_client();
            }
            _ => {
                let _ = client.topic_client();
            }
        }
        assert_eq!(
            counter_value(&registry, metric) - before,
            1,
            "client.{accessor}_client() must increment {metric} by exactly 1"
        );
    }

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn query_client_creation_with_driver_name_label() -> YdbResult<()> {
    struct DummyHandler;
    impl crate::mock_server::handler::Handler for DummyHandler {}

    let (registry, recorder) = test_recorder();
    let (server, _reply_tx) = MockServer::start(DummyHandler).await;

    let client = ClientBuilder::new_from_connection_string(format!(
        "{}{DATABASE}?use_discovery=false",
        server.endpoint()
    ))?
    .with_driver_name("custom")
    .with_metrics_recorder(backend(recorder))
    .build()
    .await?;

    let _qc = client.query_client();

    let driver_name = counter_label_value(&registry, "ydb_new_query_client_counter", "driver_name");
    assert_eq!(
        driver_name.as_deref(),
        Some("custom"),
        "query_client counter must carry the custom driver_name label"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn custom_metrics_labels_attached() -> YdbResult<()> {
    struct DummyHandler;
    impl crate::mock_server::handler::Handler for DummyHandler {}

    let (registry, recorder) = test_recorder();
    let (server, _reply_tx) = MockServer::start(DummyHandler).await;
    let _client =
        make_client_with_labels(&server, recorder, vec![("env", "prod"), ("app", "billing")])
            .await?;

    for (key, expected) in [("driver_name", "main"), ("env", "prod"), ("app", "billing")] {
        let value = counter_label_value(&registry, "ydb_new_client_counter", key);
        assert_eq!(
            value.as_deref(),
            Some(expected),
            "ydb_new_client_counter must carry {key}={expected} alongside SDK labels"
        );
    }

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn driver_name_with_custom_labels() -> YdbResult<()> {
    struct DummyHandler;
    impl crate::mock_server::handler::Handler for DummyHandler {}

    let (registry, recorder) = test_recorder();
    let (server, _reply_tx) = MockServer::start(DummyHandler).await;

    let _client = ClientBuilder::new_from_connection_string(format!(
        "{}{DATABASE}?use_discovery=false",
        server.endpoint()
    ))?
    .with_driver_name("custom")
    .with_metrics_label("env", "prod")
    .with_metrics_recorder(backend(recorder))
    .build()
    .await?;

    for (key, expected) in [("driver_name", "custom"), ("env", "prod")] {
        let value = counter_label_value(&registry, "ydb_new_client_counter", key);
        assert_eq!(
            value.as_deref(),
            Some(expected),
            "ydb_new_client_counter must carry {key}={expected}"
        );
    }

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn reserved_metrics_label_key_rejected() {
    let result = ClientBuilder::new_from_connection_string("grpc://localhost:2135/local")
        .expect("valid connection string")
        .with_metrics_label("driver_name", "x")
        .build()
        .await;

    assert!(
        matches!(result, Err(YdbError::Custom(_))),
        "build() must reject the reserved 'driver_name' label key"
    );
}

#[tokio::test]
#[tracing_test::traced_test]
async fn duplicate_metrics_label_keys_rejected() {
    let result = ClientBuilder::new_from_connection_string("grpc://localhost:2135/local")
        .expect("valid connection string")
        .with_metrics_label("env", "prod")
        .with_metrics_label("env", "dev")
        .build()
        .await;

    assert!(
        matches!(result, Err(YdbError::Custom(_))),
        "build() must reject duplicate metrics label keys"
    );
}

#[tokio::test]
#[tracing_test::traced_test]
async fn query_row_increments_counter_metric() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = QueryRowHandler::new(tokio::sync::mpsc::unbounded_channel().0);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;

    let before = counter_value(&registry, "ydb_client_query_row_counter");

    let mut qc = client.query_client();
    let _row = QueryExecutor::query_row(&mut qc, "SELECT 42 AS val").await?;

    assert_eq!(
        counter_value(&registry, "ydb_client_query_row_counter") - before,
        1,
        "query_row() must increment the query_row counter by exactly 1"
    );

    let _row2 = QueryExecutor::query_row(&mut qc, "SELECT 42 AS val").await?;

    assert_eq!(
        counter_value(&registry, "ydb_client_query_row_counter") - before,
        2,
        "second query_row() call must increment the counter again by exactly 1"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn one_shot_query_row_records_time_histogram() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = QueryRowHandler::new(tokio::sync::mpsc::unbounded_channel().0);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;

    let before = histogram_sample_count(&registry, "ydb_row_query_time_histogram");

    let mut qc = client.query_client();
    let _row = QueryExecutor::query_row(&mut qc, "SELECT 42 AS val").await?;

    assert_eq!(
        histogram_sample_count(&registry, "ydb_row_query_time_histogram") - before,
        1,
        "query_row() must record one ydb_row_query_time_histogram sample"
    );
    assert!(
        histogram_sum(&registry, "ydb_row_query_time_histogram") > 0.0,
        "ydb_row_query_time_histogram sum must be positive"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn transaction_exec_increments_counter_metric() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = ExecCountingHandler::new(tokio::sync::mpsc::unbounded_channel().0);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;

    let before = counter_value(&registry, "ydb_client_transaction_exec_counter");

    client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            QueryExecutor::exec(&mut *tx, "UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            Ok(())
        }))
        .await?;

    assert_eq!(
        counter_value(&registry, "ydb_client_transaction_exec_counter") - before,
        1,
        "transaction exec must increment the exec counter by exactly 1"
    );

    client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            QueryExecutor::exec(&mut *tx, "UPSERT INTO t (id, val) VALUES (2, 'y')").await?;
            Ok(())
        }))
        .await?;

    assert_eq!(
        counter_value(&registry, "ydb_client_transaction_exec_counter") - before,
        2,
        "second transaction exec call must increment the counter again by exactly 1"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn transaction_commit_increments_counter_metric() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = ExecCountingHandler::new(tokio::sync::mpsc::unbounded_channel().0);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;

    let before = counter_value(&registry, "ydb_client_transaction_commit_counter");

    client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            QueryExecutor::exec(&mut *tx, "UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            Ok(())
        }))
        .await?;

    assert_eq!(
        counter_value(&registry, "ydb_client_transaction_commit_counter") - before,
        1,
        "first retry_tx must trigger commit and increment the commit counter by exactly 1"
    );

    client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            QueryExecutor::exec(&mut *tx, "UPSERT INTO t (id, val) VALUES (2, 'y')").await?;
            Ok(())
        }))
        .await?;

    assert_eq!(
        counter_value(&registry, "ydb_client_transaction_commit_counter") - before,
        2,
        "second retry_tx must trigger commit and increment the commit counter again by exactly 1"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn transaction_rollback_increments_counter_metric() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = ExecCountingHandler::new(tokio::sync::mpsc::unbounded_channel().0);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;

    let before = counter_value(&registry, "ydb_client_transaction_rollback_counter");

    client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            QueryExecutor::exec(&mut *tx, "UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            tx.rollback().await?;
            Ok(())
        }))
        .await?;

    assert_eq!(
        counter_value(&registry, "ydb_client_transaction_rollback_counter") - before,
        1,
        "explicit rollback must increment the rollback counter by exactly 1"
    );

    client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            QueryExecutor::exec(&mut *tx, "UPSERT INTO t (id, val) VALUES (2, 'y')").await?;
            tx.rollback().await?;
            Ok(())
        }))
        .await?;

    assert_eq!(
        counter_value(&registry, "ydb_client_transaction_rollback_counter") - before,
        2,
        "second explicit rollback must increment the counter again by exactly 1"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn transaction_query_row_increments_counter_metric() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = TxQueryRowHandler::new(tokio::sync::mpsc::unbounded_channel().0);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;

    let before = counter_value(&registry, "ydb_client_transaction_query_row_counter");

    client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            let _row = QueryExecutor::query_row(&mut *tx, "SELECT 42 AS val").await?;
            Ok(())
        }))
        .await?;

    assert_eq!(
        counter_value(&registry, "ydb_client_transaction_query_row_counter") - before,
        1,
        "transaction query_row must increment the transaction_query_row counter by exactly 1"
    );

    client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            let _row = QueryExecutor::query_row(&mut *tx, "SELECT 43 AS val").await?;
            Ok(())
        }))
        .await?;

    assert_eq!(
        counter_value(&registry, "ydb_client_transaction_query_row_counter") - before,
        2,
        "second transaction query_row call must increment the counter again by exactly 1"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn transaction_query_row_records_time_histogram() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = TxQueryRowHandler::new(tokio::sync::mpsc::unbounded_channel().0);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;

    let before = histogram_sample_count(&registry, "ydb_transaction_row_query_time_histogram");

    client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            let _row = QueryExecutor::query_row(&mut *tx, "SELECT 42 AS val").await?;
            Ok(())
        }))
        .await?;

    assert_eq!(
        histogram_sample_count(&registry, "ydb_transaction_row_query_time_histogram") - before,
        1,
        "transaction query_row must record one ydb_transaction_row_query_time_histogram sample"
    );
    assert!(
        histogram_sum(&registry, "ydb_transaction_row_query_time_histogram") > 0.0,
        "ydb_transaction_row_query_time_histogram sum must be positive"
    );

    Ok(())
}
