//! Mock-server tests for query client metrics (counters).
//!
//! These tests are separated from `query_tx.rs` because they
//! share the global `prometheus::default_registry()` and would
//! interfere with parallel runs within the same test binary.
#![recursion_limit = "256"]

mod mock_server;

use std::sync::LazyLock;

use ydb::{Client, ClientBuilder, QueryExecutor, Transaction, YdbResult, closure};
use ydb_grpc::ydb_proto::query::{ExecuteQueryResponsePart, TransactionMeta};
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::{Column, ResultSet, Type, Value, r#type};

use crate::mock_server::handler::{FromHandlerToService, Handler, Incoming, Reply};
use crate::mock_server::query::{QUERY_TX_ID, QueryIncoming, QueryReply};
use crate::mock_server::server::MockServer;

/// Serializes metric tests that share the global prometheus registry.
static METRICS_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

const DATABASE: &str = "/local";

async fn make_client(server: &MockServer) -> YdbResult<Client> {
    ClientBuilder::new_from_connection_string(format!(
        "{}{DATABASE}?use_discovery=false",
        server.endpoint()
    ))?
    .build()
    .await
}

fn counter_value(metric_name: &str) -> u64 {
    let gathered = prometheus::default_registry().gather();
    gathered
        .iter()
        .find(|mf| mf.name() == metric_name)
        .map(|mf| mf.metric.iter().map(|m| m.counter.value() as u64).sum())
        .unwrap_or(0)
}

fn counter_label_value(metric_name: &str, label_key: &str) -> Option<String> {
    let gathered = prometheus::default_registry().gather();
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
    let _guard = METRICS_LOCK.lock().await;
    let _ = metrics_prometheus::try_install().ok(); // here should be `ok` because only one error can be emitted - duplicate attempt

    struct DummyHandler;
    impl Handler for DummyHandler {}

    let (server, _reply_tx) = MockServer::start(DummyHandler).await;
    let _ = make_client(&server).await?;

    let metrics_vec = prometheus::default_registry().gather();
    assert!(!metrics_vec.is_empty());

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn query_client_creation_increments_counter_metric() -> YdbResult<()> {
    let _guard = METRICS_LOCK.lock().await;
    let _ = metrics_prometheus::try_install().ok(); // here should be `ok` because only one error can be emitted - duplicate attempt

    struct DummyHandler;
    impl Handler for DummyHandler {}

    let (server, _reply_tx) = MockServer::start(DummyHandler).await;
    let client = make_client(&server).await?;

    let before = counter_value("ydb_new_query_client_counter");

    let _qc1 = client.query_client();
    assert_eq!(counter_value("ydb_new_query_client_counter") - before, 1);

    let _qc2 = client.query_client();
    assert_eq!(counter_value("ydb_new_query_client_counter") - before, 2);

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn query_client_creation_with_driver_name_label() -> YdbResult<()> {
    let _guard = METRICS_LOCK.lock().await;
    let _ = metrics_prometheus::try_install().ok();

    struct DummyHandler;
    impl Handler for DummyHandler {}

    let (server, _reply_tx) = MockServer::start(DummyHandler).await;

    let client = ClientBuilder::new_from_connection_string(format!(
        "{}{}?use_discovery=false",
        server.endpoint(),
        DATABASE,
    ))?
    .with_driver_name("custom")
    .build()
    .await?;

    let _qc = client.query_client();

    let driver_name = counter_label_value("ydb_new_query_client_counter", "driver_name");
    assert_eq!(
        driver_name.as_deref(),
        Some("custom"),
        "query_client counter must carry the custom driver_name label"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn query_row_increments_counter_metric() -> YdbResult<()> {
    let _guard = METRICS_LOCK.lock().await;
    let _ = metrics_prometheus::try_install().ok(); // here should be `ok` because only one error can be emitted - duplicate attempt

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

    let handler = QueryRowHandler {
        replies: tokio::sync::mpsc::unbounded_channel::<Reply>().0,
    };
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let before = counter_value("ydb_client_query_row_counter");

    let mut qc = client.query_client();
    let _row = QueryExecutor::query_row(&mut qc, "SELECT 42 AS val").await?;

    assert_eq!(
        counter_value("ydb_client_query_row_counter") - before,
        1,
        "query_row() must increment the query_row counter by exactly 1"
    );

    let _row2 = QueryExecutor::query_row(&mut qc, "SELECT 42 AS val").await?;

    assert_eq!(
        counter_value("ydb_client_query_row_counter") - before,
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
    let _guard = METRICS_LOCK.lock().await;
    let _ = metrics_prometheus::try_install().ok(); // here should be `ok` because only one error can be emitted - duplicate attempt

    let handler = ExecCountingHandler {
        replies: tokio::sync::mpsc::unbounded_channel::<Reply>().0,
    };
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let before = counter_value("ydb_client_transaction_exec_counter");

    client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            QueryExecutor::exec(&mut *tx, "UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            Ok(())
        }))
        .await?;

    assert_eq!(
        counter_value("ydb_client_transaction_exec_counter") - before,
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
        counter_value("ydb_client_transaction_exec_counter") - before,
        2,
        "second transaction exec call must increment the counter again by exactly 1"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn transaction_commit_increments_counter_metric() -> YdbResult<()> {
    let _guard = METRICS_LOCK.lock().await;
    let _ = metrics_prometheus::try_install().ok(); // here should be `ok` because only one error can be emitted - duplicate attempt

    let handler = ExecCountingHandler {
        replies: tokio::sync::mpsc::unbounded_channel::<Reply>().0,
    };
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let before = counter_value("ydb_client_transaction_commit_counter");

    client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            QueryExecutor::exec(&mut *tx, "UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            Ok(())
        }))
        .await?;

    assert_eq!(
        counter_value("ydb_client_transaction_commit_counter") - before,
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
        counter_value("ydb_client_transaction_commit_counter") - before,
        2,
        "second retry_tx must trigger commit and increment the commit counter again by exactly 1"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn transaction_rollback_increments_counter_metric() -> YdbResult<()> {
    let _guard = METRICS_LOCK.lock().await;
    let _ = metrics_prometheus::try_install().ok(); // here should be `ok` because only one error can be emitted - duplicate attempt

    let handler = ExecCountingHandler {
        replies: tokio::sync::mpsc::unbounded_channel::<Reply>().0,
    };
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let before = counter_value("ydb_client_transaction_rollback_counter");

    client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            QueryExecutor::exec(&mut *tx, "UPSERT INTO t (id, val) VALUES (1, 'x')").await?;
            tx.rollback().await?;
            Ok(())
        }))
        .await?;

    assert_eq!(
        counter_value("ydb_client_transaction_rollback_counter") - before,
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
        counter_value("ydb_client_transaction_rollback_counter") - before,
        2,
        "second explicit rollback must increment the counter again by exactly 1"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn transaction_query_row_increments_counter_metric() -> YdbResult<()> {
    let _guard = METRICS_LOCK.lock().await;
    let _ = metrics_prometheus::try_install().ok(); // here should be `ok` because only one error can be emitted - duplicate attempt

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

    let handler = TxQueryRowHandler {
        replies: tokio::sync::mpsc::unbounded_channel::<Reply>().0,
    };
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server).await?;

    let before = counter_value("ydb_client_transaction_query_row_counter");

    client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            let _row = QueryExecutor::query_row(&mut *tx, "SELECT 42 AS val").await?;
            Ok(())
        }))
        .await?;

    assert_eq!(
        counter_value("ydb_client_transaction_query_row_counter") - before,
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
        counter_value("ydb_client_transaction_query_row_counter") - before,
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
    let _guard = METRICS_LOCK.lock().await;
    let _ = metrics_prometheus::try_install().ok();

    let handler = OptionalRowHandler {
        replies: tokio::sync::mpsc::unbounded_channel::<Reply>().0,
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
    let _guard = METRICS_LOCK.lock().await;
    let _ = metrics_prometheus::try_install().ok();

    let handler = EmptyRowHandler {
        replies: tokio::sync::mpsc::unbounded_channel::<Reply>().0,
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
