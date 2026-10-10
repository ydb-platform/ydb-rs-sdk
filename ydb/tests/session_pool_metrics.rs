//! Mock-server tests for the driver session pool metrics (cycle 3).
//!
//! The driver pool is exercised end-to-end through `query_client().retry_tx` /
//! one-shot `exec`, which acquire a pooled session (CreateSession + AttachSession
//! RPCs against the mock) and return it after the operation.

mod mock_server;

use std::time::Duration;

use ydb::{
    QueryExecutor, SessionPoolSettings, Transaction, YdbResult, YdbResultWithCustomerErr, closure,
};
use ydb_grpc::ydb_proto::query::SessionState;
use ydb_grpc::ydb_proto::status_ids::StatusCode;

use crate::mock_server::handler::{FromHandlerToService, Handler, Incoming, Reply};
use crate::mock_server::metrics::*;
use crate::mock_server::query::parts::{status_part, success_part};
use crate::mock_server::query::{ExecCountingHandler, QUERY_TX_ID, QueryIncoming, QueryReply};
use crate::mock_server::server::MockServer;

/// Default driver pool limit; asserted as the `ydb_session_pool_size_limit` gauge.
const DEFAULT_POOL_LIMIT: f64 = 50.0;

async fn tx_upsert(client: &ydb::Client, val: i64) -> YdbResultWithCustomerErr<()> {
    client
        .query_client()
        .retry_tx(closure!(async |tx: &mut Transaction| {
            QueryExecutor::exec(
                &mut *tx,
                format!("UPSERT INTO t (id, val) VALUES ({val}, 'x')"),
            )
            .await?;
            Ok(())
        }))
        .await
}

#[tokio::test]
#[tracing_test::traced_test]
async fn acquire_create_gauges_and_use_metrics() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = ExecCountingHandler::new(tokio::sync::mpsc::unbounded_channel().0);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;

    tx_upsert(&client, 1).await?;

    // Acquire: counter and duration by result.
    assert!(
        counter_value_with_label(&registry, "ydb_session_pool_acquire_total", "result", "ok") >= 1,
        "one successful acquire must be counted"
    );
    assert!(
        histogram_sample_count_with_label(
            &registry,
            "ydb_session_pool_acquire_milliseconds",
            "result",
            "ok"
        ) >= 1,
        "one acquire duration must be recorded"
    );
    assert!(
        histogram_sum(&registry, "ydb_session_pool_acquire_milliseconds") > 0.0,
        "ydb_session_pool_acquire_milliseconds sum must be positive"
    );

    // Session creation: counter and duration.
    assert!(
        counter_value(&registry, "ydb_session_pool_sessions_created_total") >= 1,
        "the pooled call must create at least one session"
    );
    assert!(
        histogram_sample_count(&registry, "ydb_session_pool_session_create_milliseconds") >= 1,
        "session creation duration must be recorded"
    );

    // Session lease duration.
    assert!(
        histogram_sample_count(&registry, "ydb_session_pool_session_use_milliseconds") >= 1,
        "returning the session must record a use duration"
    );

    // Gauges are re-emitted synchronously at pool mutations; the returned
    // session must show up as idle. Poll to also absorb background release.
    wait_for(|| {
        gauge_value_with_label(&registry, "ydb_session_pool_sessions", "state", "idle") == 1.0
    })
    .await;
    for state in ["active", "creating"] {
        assert!(
            metric_series_present(&registry, "ydb_session_pool_sessions", "state", state),
            "ydb_session_pool_sessions must have a {{state={state}}} series"
        );
    }
    assert_eq!(
        gauge_value(&registry, "ydb_session_pool_size_limit"),
        DEFAULT_POOL_LIMIT,
        "ydb_session_pool_size_limit must mirror the driver pool limit"
    );
    assert!(
        metric_present(&registry, "ydb_session_pool_pending_requests"),
        "ydb_session_pool_pending_requests must be registered"
    );

    Ok(())
}

/// Replies to `ExecuteQuery` with a BAD_SESSION part, invalidating the pooled
/// session the one-shot call leased.
struct BadSessionHandler {
    replies: FromHandlerToService,
}

impl Handler for BadSessionHandler {
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
                part: status_part(StatusCode::BadSession),
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
async fn bad_session_operation_closes_session_with_reason() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = BadSessionHandler {
        replies: tokio::sync::mpsc::unbounded_channel().0,
    };
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;

    // BAD_SESSION is always retryable; the wall timeout bounds the attempts.
    let result = client
        .query_client()
        .exec("SELECT 1")
        .timeout(Duration::from_millis(500))
        .await;
    assert!(result.is_err(), "exec must fail on BAD_SESSION responses");

    wait_for(|| {
        counter_value_with_label(
            &registry,
            "ydb_session_pool_sessions_closed_total",
            "reason",
            "bad_session",
        ) >= 1
    })
    .await;

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn idle_expired_session_closed_on_next_acquire() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = ExecCountingHandler::new(tokio::sync::mpsc::unbounded_channel().0);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;
    let client = client
        .with_session_pool(
            SessionPoolSettings::new()
                .with_limit(1)
                .with_idle_ttl(Duration::from_millis(30)),
        )
        .await?;

    tx_upsert(&client, 1).await?;

    // The idle timer runs on wall time; let the idle session expire before the
    // next acquire (mirrors the pool's own TTL unit test).
    tokio::time::sleep(Duration::from_millis(60)).await;

    tx_upsert(&client, 2).await?;

    assert_eq!(
        counter_value_with_label(
            &registry,
            "ydb_session_pool_sessions_closed_total",
            "reason",
            "idle_ttl"
        ),
        1,
        "the expired idle session must be closed with the idle_ttl reason on the next acquire"
    );

    Ok(())
}

/// Completes the AttachSession handshake, then closes the attach stream; the
/// liveness watcher must report a successful keepalive.
struct AttachThenCloseHandler {
    replies: FromHandlerToService,
}

impl Handler for AttachThenCloseHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies = tx;
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        match incoming {
            Incoming::Query(QueryIncoming::AttachSession(_, stream_id)) => {
                self.replies
                    .send(Reply::Query(QueryReply::AttachSession {
                        stream_id,
                        state: SessionState {
                            status: StatusCode::Success as i32,
                            issues: Vec::new(),
                            session_hint: None,
                        },
                    }))
                    .expect("mock response channel must remain open");
                self.replies
                    .send(Reply::Query(QueryReply::AttachSessionClose { stream_id }))
                    .expect("mock response channel must remain open");
                None
            }
            Incoming::Query(QueryIncoming::ExecuteQuery(_, stream_id)) => {
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
            other => Some(other),
        }
    }
}

/// Completes the AttachSession handshake, then fails the attach stream; the
/// liveness watcher must report a failed keepalive.
struct AttachThenFailHandler {
    replies: FromHandlerToService,
}

impl Handler for AttachThenFailHandler {
    fn set_channel(&mut self, tx: FromHandlerToService) {
        self.replies = tx;
    }

    fn handle(&self, incoming: Incoming) -> Option<Incoming> {
        match incoming {
            Incoming::Query(QueryIncoming::AttachSession(_, stream_id)) => {
                self.replies
                    .send(Reply::Query(QueryReply::AttachSession {
                        stream_id,
                        state: SessionState {
                            status: StatusCode::Success as i32,
                            issues: Vec::new(),
                            session_hint: None,
                        },
                    }))
                    .expect("mock response channel must remain open");
                self.replies
                    .send(Reply::Query(QueryReply::AttachSessionFail {
                        stream_id,
                        status: tonic::Status::unavailable("mock attach stream failure"),
                    }))
                    .expect("mock response channel must remain open");
                None
            }
            Incoming::Query(QueryIncoming::ExecuteQuery(_, stream_id)) => {
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
            other => Some(other),
        }
    }
}

#[tokio::test]
#[tracing_test::traced_test]
async fn attach_stream_close_reports_successful_keepalive() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = AttachThenCloseHandler {
        replies: tokio::sync::mpsc::unbounded_channel().0,
    };
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;

    tx_upsert(&client, 1).await?;

    // The watcher task observes the stream close asynchronously.
    wait_for(|| {
        counter_value_with_label(
            &registry,
            "ydb_session_pool_keepalive_total",
            "result",
            "ok",
        ) >= 1
    })
    .await;

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn attach_stream_failure_reports_failed_keepalive() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = AttachThenFailHandler {
        replies: tokio::sync::mpsc::unbounded_channel().0,
    };
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;

    tx_upsert(&client, 1).await?;

    wait_for(|| {
        counter_value_with_label(
            &registry,
            "ydb_session_pool_keepalive_total",
            "result",
            "error",
        ) >= 1
    })
    .await;

    Ok(())
}
