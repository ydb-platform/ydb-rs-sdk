//! Mock-server tests for the gRPC transport metrics emitted by
//! `MetricsInterceptor` and the connection pool (cycles 2 + 3.5).
//!
//! The transport metrics are recorded through the client's recorder (the
//! connection manager receives the same recorder at `ClientBuilder::build`
//! time, before any RPC), so injecting a private registry via
//! `with_metrics_recorder` captures them.

mod mock_server;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ydb::{QueryExecutor, TopicReaderOptions, YdbResult};
use ydb_grpc::ydb_proto::topic::stream_read_message::from_client::ClientMessage as ReadFromClient;

use crate::mock_server::handler::{Handler, Incoming};
use crate::mock_server::metrics::*;
use crate::mock_server::query::{QueryIncoming, QueryRowHandler};
use crate::mock_server::server::MockServer;
use crate::mock_server::topic::{TopicIncoming, builders};

const CONSUMER: &str = "consumer";
const TOPIC_PATH: &str = "/local/topic";

#[tokio::test]
#[tracing_test::traced_test]
async fn grpc_request_metrics_after_query() -> YdbResult<()> {
    let (registry, recorder) = test_recorder();
    let handler = QueryRowHandler::new(tokio::sync::mpsc::unbounded_channel().0);
    let (server, _reply_tx) = MockServer::start(handler).await;
    let client = make_client(&server, recorder).await?;
    let authority = endpoint_authority(&server).to_string();

    let mut qc = client.query_client();
    let _row = QueryExecutor::query_row(&mut qc, "SELECT 42 AS val").await?;

    // One requests_total series per (service, method, grpc_code) combination.
    for (key, value) in [
        ("service", "Ydb.Query.V1.QueryService"),
        ("method", "ExecuteQuery"),
        ("grpc_code", "ok"),
        ("endpoint", authority.as_str()),
    ] {
        assert!(
            counter_value_with_label(&registry, "ydb_grpc_requests_total", key, value) >= 1,
            "ydb_grpc_requests_total must have a series with {key}={value}"
        );
    }
    // The pooled one-shot also goes through CreateSession and AttachSession.
    for method in ["CreateSession", "AttachSession"] {
        assert!(
            counter_value_with_label(&registry, "ydb_grpc_requests_total", "method", method) >= 1,
            "ydb_grpc_requests_total must count {method} RPCs"
        );
    }

    // Duration histogram: no grpc_code label, one sample per completed RPC.
    assert!(
        histogram_sample_count(&registry, "ydb_grpc_request_duration_milliseconds") >= 1,
        "each completed RPC must record a duration sample"
    );
    assert!(
        histogram_sum(&registry, "ydb_grpc_request_duration_milliseconds") > 0.0,
        "ydb_grpc_request_duration_milliseconds sum must be positive"
    );

    // Connection pool gauges and the establishment histogram. The channel is
    // lazy: the gauges track pool entries, `state=active` must be present.
    wait_for(|| {
        gauge_value_with_label(&registry, "ydb_grpc_connections", "state", "active") == 1.0
    })
    .await;
    assert!(
        histogram_sample_count(&registry, "ydb_grpc_connection_establish_milliseconds") >= 1,
        "channel establishment must record a duration sample"
    );
    assert!(
        histogram_sum(&registry, "ydb_grpc_connection_establish_milliseconds") > 0.0,
        "ydb_grpc_connection_establish_milliseconds sum must be positive"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn grpc_error_metrics_on_failed_rpc() -> YdbResult<()> {
    // CreateSession always fails with a transport-level status; the client
    // build itself performs no RPC, so the failure hits on the first acquire.
    struct CreateSessionUnavailableHandler;
    impl Handler for CreateSessionUnavailableHandler {
        fn handle(&self, incoming: Incoming) -> Option<Incoming> {
            let Incoming::Query(QueryIncoming::CreateSession(_, reply_tx)) = incoming else {
                return Some(incoming);
            };
            let _ = reply_tx.send(Err(tonic::Status::unavailable(
                "mock CreateSession is unavailable",
            )));
            None
        }
    }

    let (registry, recorder) = test_recorder();
    let (server, _reply_tx) = MockServer::start(CreateSessionUnavailableHandler).await;
    let client = make_client(&server, recorder).await?;

    let result = client
        .query_client()
        .exec("UPSERT INTO t (id) VALUES (1)")
        .timeout(Duration::from_secs(2))
        .await;
    assert!(result.is_err(), "exec must fail when CreateSession fails");

    wait_for(|| {
        counter_value_with_label(
            &registry,
            "ydb_grpc_requests_total",
            "grpc_code",
            "unavailable",
        ) >= 1
    })
    .await;
    assert!(
        counter_value_with_label(
            &registry,
            "ydb_grpc_requests_total",
            "method",
            "CreateSession"
        ) >= 1,
        "the failed RPC must be counted in ydb_grpc_requests_total"
    );
    assert_eq!(
        counter_value_with_label(
            &registry,
            "ydb_grpc_errors_total",
            "grpc_code",
            "unavailable"
        ),
        1,
        "one failed RPC must increment ydb_grpc_errors_total exactly once"
    );

    Ok(())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn grpc_stream_messages_total_on_topic_stream() -> YdbResult<()> {
    struct StreamIdCapture {
        stream_id: Arc<Mutex<Option<u64>>>,
    }
    impl Handler for StreamIdCapture {
        fn handle(&self, incoming: Incoming) -> Option<Incoming> {
            if let Incoming::Topic(TopicIncoming::StreamRead {
                stream_id,
                msg: ReadFromClient::InitRequest(_),
            }) = &incoming
            {
                *self.stream_id.lock().expect("stream id lock") = Some(*stream_id);
            }
            Some(incoming)
        }
    }

    let (registry, recorder) = test_recorder();
    let stream_id = Arc::new(Mutex::new(None));
    let (server, reply_tx) = MockServer::start(StreamIdCapture {
        stream_id: stream_id.clone(),
    })
    .await;
    let client = make_client(&server, recorder).await?;

    let mut reader = client
        .topic_client()
        .create_reader_with_params(
            TopicReaderOptions::builder()
                .consumer(CONSUMER.to_string())
                .topic(TOPIC_PATH.to_string())
                .build(),
        )
        .await?;

    // The reader opens the StreamRead channel lazily: wait until its
    // InitRequest reaches the handler, then deliver one read response.
    wait_for(|| stream_id.lock().expect("stream id lock").is_some()).await;

    let stream_id = *stream_id.lock().expect("stream id lock");
    let stream_id = stream_id.expect("StreamRead InitRequest must have reached the handler");
    reply_tx
        .send(builders::read_response(stream_id, 1, 0, b"hello").into())
        .expect("mock server dropped");

    let batch = reader.read_batch().await?;
    assert_eq!(batch.messages.len(), 1);

    // Received stream messages are counted per message through the stream
    // wrapper. The `sent` direction is not assertable e2e yet: every sender
    // (reader/writer loops, coordination controllers) bypasses the counting
    // wrapper via `clone_sender()` — registered as a finding for review.
    assert!(
        counter_value_with_label(
            &registry,
            "ydb_grpc_stream_messages_total",
            "direction",
            "received"
        ) >= 1,
        "ydb_grpc_stream_messages_total must count received stream messages"
    );
    for (key, value) in [
        ("service", "topic_service"),
        ("method", "stream_read"),
        ("endpoint", endpoint_authority(&server)),
    ] {
        assert!(
            counter_value_with_label(&registry, "ydb_grpc_stream_messages_total", key, value) >= 1,
            "ydb_grpc_stream_messages_total must carry {key}={value}"
        );
    }

    Ok(())
}
