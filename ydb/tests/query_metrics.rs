//! Mock-server tests for query client metrics (counters).
//!
//! Each test injects an isolated `metrics_prometheus::Recorder` backed by its own
//! private `prometheus::Registry` via `ClientBuilder::with_metrics_recorder`, so the
//! tests run in parallel and never touch the process-global recorder.
//! Custom static labels attached via `ClientBuilder::with_metrics_label(s)` are
//! covered here too, including the reserved-key and duplicate-key validation.
#![recursion_limit = "256"]

mod mock_server;

use ydb::{
    Client, ClientBuilder, MetricsRecorder, QueryExecutor, Transaction, YdbError, YdbResult,
    closure,
};
use ydb_grpc::ydb_proto::query::{ExecuteQueryResponsePart, TransactionMeta};
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::{Column, ResultSet, Type, Value, r#type};

use crate::mock_server::handler::{FromHandlerToService, Handler, Incoming, Reply};
use crate::mock_server::query::{QUERY_TX_ID, QueryIncoming, QueryReply};
use crate::mock_server::server::MockServer;

const DATABASE: &str = "/local";

async fn make_client(
    server: &MockServer,
    recorder: metrics_prometheus::Recorder,
) -> YdbResult<Client> {
    make_client_with_labels(server, recorder, Vec::<(String, String)>::new()).await
}

async fn make_client_with_labels<I, K, V>(
    server: &MockServer,
    recorder: metrics_prometheus::Recorder,
    labels: I,
) -> YdbResult<Client>
where
    I: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<String>,
{
    ClientBuilder::new_from_connection_string(format!(
        "{}{DATABASE}?use_discovery=false",
        server.endpoint()
    ))?
    .with_metrics_labels(labels)
    .with_metrics_recorder(MetricsRecorder::new(recorder))
    .build()
    .await
}

fn test_recorder() -> (prometheus::Registry, metrics_prometheus::Recorder) {
    let registry = prometheus::Registry::new();
    let recorder = metrics_prometheus::Recorder::builder()
        .with_registry(&registry)
        .build();
    (registry, recorder)
}

fn counter_value(registry: &prometheus::Registry, metric_name: &str) -> u64 {
    let gathered = registry.gather();
    gathered
        .iter()
        .find(|mf| mf.name() == metric_name)
        .map(|mf| mf.metric.iter().map(|m| m.counter.value() as u64).sum())
        .unwrap_or(0)
}

fn counter_label_value(
    registry: &prometheus::Registry,
    metric_name: &str,
    label_key: &str,
) -> Option<String> {
    let gathered = registry.gather();
    gathered
        .iter()
        .find(|mf| mf.name() == metric_name)
        .and_then(|mf| mf.metric.first())
        .and_then(|m| {
            m.label
                .iter()
                .find(|l| l.name() == label_key)
                .map(|l| l.value().to_string())
        })
}

fn row_with_value(value: i64) -> ExecuteQueryResponsePart {
    ExecuteQueryResponsePart {
        status: StatusCode::Success as i32,
        issues: vec![],
        result_set_index: 0,
        result_set: Some(ResultSet {
            columns: vec![Column {
                name: "val".to_string(),
                r#type: Some(Type {
                    r#type: Some(r#type::Type::TypeId(
                        ydb_grpc::ydb_proto::r#type::PrimitiveTypeId::Int64 as i32,
                    )),
                }),
            }],
            rows: vec![Value {
                items: vec![Value {
                    value: Some(ydb_grpc::ydb_proto::value::Value::Int64Value(value)),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }),
        exec_stats: None,
        tx_meta: None,
    }
}

#[tokio::test]
#[tracing_test::traced_test]
async fn happy_path_collect_metrics() -> YdbResult<()> {
    struct DummyHandler;
    impl Handler for DummyHandler {}

    let (registry, recorder) = test_recorder();
    let (server, _reply_tx) = MockServer::start(DummyHandler).await;
    let _ = make_client(&server, recorder).await?;

    let metrics_vec = registry.gather();
    assert!(!metrics_vec.is_empty());

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn query_client_creation_increments_counter_metric() -> YdbResult<()> {
    struct DummyHandler;
    impl Handler for DummyHandler {}

    let (registry, recorder) = test_recorder();
    let (server, _reply_tx) = MockServer::start(DummyHandler).await;
    let client = make_client(&server, recorder).await?;

    let before = counter_value(&registry, "ydb_new_query_client_counter");

    let _qc1 = client.query_client();
    assert_eq!(
        counter_value(&registry, "ydb_new_query_client_counter") - before,
        1
    );

    let _qc2 = client.query_client();
    assert_eq!(
        counter_value(&registry, "ydb_new_query_client_counter") - before,
        2
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn query_client_creation_with_driver_name_label() -> YdbResult<()> {
    struct DummyHandler;
    impl Handler for DummyHandler {}

    let (registry, recorder) = test_recorder();
    let (server, _reply_tx) = MockServer::start(DummyHandler).await;

    let client = ClientBuilder::new_from_connection_string(format!(
        "{}{}?use_discovery=false",
        server.endpoint(),
        DATABASE,
    ))?
    .with_driver_name("custom")
    .with_metrics_recorder(MetricsRecorder::new(recorder))
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
    impl Handler for DummyHandler {}

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
    impl Handler for DummyHandler {}

    let (registry, recorder) = test_recorder();
    let (server, _reply_tx) = MockServer::start(DummyHandler).await;

    let _client = ClientBuilder::new_from_connection_string(format!(
        "{}{DATABASE}?use_discovery=false",
        server.endpoint()
    ))?
    .with_driver_name("custom")
    .with_metrics_label("env", "prod")
    .with_metrics_recorder(MetricsRecorder::new(recorder))
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
    struct QueryRowHandler {
        replies: FromHandlerToService,
    }
    impl Handler for QueryRowHandler {
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
                    part: row_with_value(42),
                }))
                .expect("mock response channel must remain open");
            self.replies
                .send(Reply::Query(QueryReply::ExecuteQueryClose { stream_id }))
                .expect("mock response channel must remain open");
            None
        }
    }

    let (registry, recorder) = test_recorder();
    let handler = QueryRowHandler {
        replies: tokio::sync::mpsc::unbounded_channel::<Reply>().0,
    };
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

struct ExecCountingHandler {
    replies: FromHandlerToService,
}

impl Handler for ExecCountingHandler {
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
                part: success_part(Some(QUERY_TX_ID)),
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
async fn transaction_exec_increments_counter_metric() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = ExecCountingHandler {
        replies: tokio::sync::mpsc::unbounded_channel::<Reply>().0,
    };
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
    let handler = ExecCountingHandler {
        replies: tokio::sync::mpsc::unbounded_channel::<Reply>().0,
    };
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
    let handler = ExecCountingHandler {
        replies: tokio::sync::mpsc::unbounded_channel::<Reply>().0,
    };
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
    struct TxQueryRowHandler {
        replies: FromHandlerToService,
    }
    impl Handler for TxQueryRowHandler {
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
                    part: success_part(Some(QUERY_TX_ID)),
                }))
                .expect("mock response channel must remain open");
            self.replies
                .send(Reply::Query(QueryReply::ExecuteQuery {
                    stream_id,
                    part: row_with_value(42),
                }))
                .expect("mock response channel must remain open");
            self.replies
                .send(Reply::Query(QueryReply::ExecuteQueryClose { stream_id }))
                .expect("mock response channel must remain open");
            None
        }
    }

    let (registry, recorder) = test_recorder();
    let handler = TxQueryRowHandler {
        replies: tokio::sync::mpsc::unbounded_channel::<Reply>().0,
    };
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

fn empty_result_set_part() -> ExecuteQueryResponsePart {
    ExecuteQueryResponsePart {
        status: StatusCode::Success as i32,
        issues: vec![],
        result_set_index: 0,
        result_set: Some(ResultSet {
            columns: vec![Column {
                name: "val".to_string(),
                r#type: Some(Type {
                    r#type: Some(r#type::Type::TypeId(
                        ydb_grpc::ydb_proto::r#type::PrimitiveTypeId::Int64 as i32,
                    )),
                }),
            }],
            rows: vec![],
            ..Default::default()
        }),
        exec_stats: None,
        tx_meta: None,
    }
}

struct OptionalRowHandler {
    replies: FromHandlerToService,
}

impl Handler for OptionalRowHandler {
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
                part: row_with_value(42),
            }))
            .expect("mock response channel must remain open");
        self.replies
            .send(Reply::Query(QueryReply::ExecuteQueryClose { stream_id }))
            .expect("mock response channel must remain open");
        None
    }
}

struct EmptyRowHandler {
    replies: FromHandlerToService,
}

impl Handler for EmptyRowHandler {
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
                part: empty_result_set_part(),
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
async fn optional_row_into_future_maps_to_option() -> YdbResult<()> {
    let handler = OptionalRowHandler {
        replies: tokio::sync::mpsc::unbounded_channel::<Reply>().0,
    };
    let (server, _reply_tx) = MockServer::start(handler).await;
    let (_registry, recorder) = test_recorder();
    let client = make_client(&server, recorder).await?;

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
        replies: tokio::sync::mpsc::unbounded_channel::<Reply>().0,
    };
    let (server, _reply_tx) = MockServer::start(handler).await;
    let (_registry, recorder) = test_recorder();
    let client = make_client(&server, recorder).await?;

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
