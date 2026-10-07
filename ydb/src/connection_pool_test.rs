use crate::YdbResult;
use crate::connection_pool::{ConnectionPool, Simple, normalize_uri_scheme};
use http::uri::{Scheme, Uri};

#[test]
fn test_normalize_uri_scheme_grpc_to_http() -> YdbResult<()> {
    let uri = Uri::from_static("grpc://localhost:7135/path");
    let normalized = normalize_uri_scheme(uri)?;

    assert_eq!(normalized.scheme(), Some(&Scheme::HTTP));
    assert_eq!(normalized.host(), Some("localhost"));
    assert_eq!(
        normalized.port().map(|p| p.as_str().to_string()),
        Some("7135".to_string())
    );
    assert_eq!(normalized.path(), "/path");

    Ok(())
}

#[test]
fn test_normalize_uri_scheme_grpcs_to_https() -> YdbResult<()> {
    let uri = Uri::from_static("grpcs://ydb.serverless.yandexcloud.net:2135/local");
    let normalized = normalize_uri_scheme(uri)?;

    assert_eq!(normalized.scheme(), Some(&Scheme::HTTPS));
    assert_eq!(normalized.host(), Some("ydb.serverless.yandexcloud.net"));
    assert_eq!(
        normalized.port().map(|p| p.as_str().to_string()),
        Some("2135".to_string())
    );
    assert_eq!(normalized.path(), "/local");

    Ok(())
}

#[tokio::test]
async fn test_connection_creates_new_connection() -> YdbResult<()> {
    let pool = ConnectionPool::<Simple>::default();
    let uri = Uri::from_static("grpc://localhost:7135/path");

    let channel = pool.connection(&uri).await?;
    let _ = channel;

    Ok(())
}

#[tokio::test]
async fn test_connection_connection_reuse() -> YdbResult<()> {
    let pool = ConnectionPool::<Simple>::default();
    let uri = Uri::from_static("grpcs://localhost:2135/local");

    let first_channel = pool.connection(&uri).await?;

    let second_channel = pool.connection(&uri).await?;

    let _ = first_channel;
    let _ = second_channel;

    Ok(())
}

#[tokio::test]
async fn test_connection_without_host_fails() {
    let pool = ConnectionPool::<Simple>::default();

    let uri = Uri::builder()
        .scheme("grpcs")
        .path_and_query("/path")
        .build();

    if let Ok(uri) = uri {
        let result = pool.connection(&uri).await;
        assert!(result.is_err());
    } else {
        assert!(uri.is_err());
    }
}

mod metrics_tests {
    use super::*;
    use crate::GrpcOptions;
    use crate::YdbError;
    use crate::client_metrics::test_recorder::TestRecorder;
    use crate::client_metrics::{DefaultMetricsRecorder, MetricsRecorder};
    use crate::connection_pool::Connection;
    use std::sync::Arc;
    use tonic::transport::Channel;

    struct MockOkConnection;

    impl Connection for MockOkConnection {
        async fn init(_uri: Uri, _opts: &GrpcOptions) -> YdbResult<Self> {
            Ok(Self)
        }

        async fn channel(&self) -> YdbResult<Channel> {
            let endpoint = tonic::transport::Endpoint::from_static("http://127.0.0.1:1");
            Ok(endpoint.connect_lazy())
        }
    }

    struct MockFailConnection;

    impl Connection for MockFailConnection {
        async fn init(_uri: Uri, _opts: &GrpcOptions) -> YdbResult<Self> {
            Err(YdbError::custom("mock connection failure"))
        }

        async fn channel(&self) -> YdbResult<Channel> {
            let endpoint = tonic::transport::Endpoint::from_static("http://127.0.0.1:1");
            Ok(endpoint.connect_lazy())
        }
    }

    fn pool_with_recorder<ConnectionT: Connection>()
    -> (ConnectionPool<ConnectionT>, Arc<TestRecorder>) {
        let recorder = Arc::new(TestRecorder::new());
        let metrics: Arc<dyn MetricsRecorder> = recorder.clone();
        (
            ConnectionPool::new(GrpcOptions::default(), metrics),
            recorder,
        )
    }

    #[tokio::test]
    async fn successful_connection_updates_gauges_and_duration() -> YdbResult<()> {
        let (pool, recorder) = pool_with_recorder::<MockOkConnection>();
        let uri = Uri::from_static("grpc://localhost:7135/path");

        pool.connection(&uri).await?;

        assert_eq!(
            recorder.gauge_value_with_label("ydb_grpc_connections", "state", "connecting"),
            0.0,
            "connecting must return to zero after the attempt"
        );
        assert_eq!(
            recorder.gauge_value_with_label("ydb_grpc_connections", "state", "active"),
            1.0,
            "successful connection must count as active"
        );
        assert_eq!(
            recorder.gauge_value_with_label("ydb_grpc_connections", "state", "failed"),
            0.0
        );

        let observations =
            recorder.histogram_observations("ydb_grpc_connection_establish_milliseconds");
        assert_eq!(observations.len(), 1, "exactly one establish observation");
        assert!(
            observations[0] >= 0.0,
            "duration must be recorded in milliseconds"
        );
        Ok(())
    }

    #[tokio::test]
    async fn failed_connection_counts_failed_state() -> YdbResult<()> {
        let (pool, recorder) = pool_with_recorder::<MockFailConnection>();
        let uri = Uri::from_static("grpc://localhost:7135/path");

        let result = pool.connection(&uri).await;
        assert!(result.is_err(), "mock connection must fail");

        assert_eq!(
            recorder.gauge_value_with_label("ydb_grpc_connections", "state", "connecting"),
            0.0,
            "connecting must return to zero after the attempt"
        );
        assert_eq!(
            recorder.gauge_value_with_label("ydb_grpc_connections", "state", "failed"),
            1.0,
            "failed connection must count as failed"
        );
        assert_eq!(
            recorder.gauge_value_with_label("ydb_grpc_connections", "state", "active"),
            0.0
        );

        let observations =
            recorder.histogram_observations("ydb_grpc_connection_establish_milliseconds");
        assert_eq!(observations.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn reused_connection_records_establish_once() -> YdbResult<()> {
        let (pool, recorder) = pool_with_recorder::<MockOkConnection>();
        let uri = Uri::from_static("grpc://localhost:7135/path");

        pool.connection(&uri).await?;
        pool.connection(&uri).await?;

        assert_eq!(
            recorder
                .histogram_observations("ydb_grpc_connection_establish_milliseconds")
                .len(),
            1,
            "pooled channel reuse must not re-record establishment"
        );
        assert_eq!(
            recorder.gauge_value_with_label("ydb_grpc_connections", "state", "active"),
            1.0,
            "reuse must keep a single active pool entry"
        );
        Ok(())
    }

    #[test]
    fn default_metrics_recorder_smoke() {
        // Ensures the Default import stays valid for the pool's default constructor.
        let _recorder = DefaultMetricsRecorder::new();
    }
}
