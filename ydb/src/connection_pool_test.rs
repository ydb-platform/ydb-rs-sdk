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
    use once_cell::sync::Lazy;
    use std::sync::Arc;
    use tokio::sync::Barrier;
    use tonic::transport::Channel;

    static CONCURRENT_INIT_STARTED: Lazy<Barrier> = Lazy::new(|| Barrier::new(2));
    static CONCURRENT_INIT_RELEASE: Lazy<Barrier> = Lazy::new(|| Barrier::new(2));
    static CANCELLED_INIT_STARTED: Lazy<Barrier> = Lazy::new(|| Barrier::new(2));

    struct MockOkConnection;

    impl Connection for MockOkConnection {
        async fn init(_uri: Uri, _opts: &GrpcOptions) -> YdbResult<Self> {
            Ok(Self)
        }

        async fn channel(&self) -> YdbResult<Channel> {
            mock_channel()
        }
    }

    struct MockFailConnection;

    impl Connection for MockFailConnection {
        async fn init(_uri: Uri, _opts: &GrpcOptions) -> YdbResult<Self> {
            Err(YdbError::custom("mock connection failure"))
        }

        async fn channel(&self) -> YdbResult<Channel> {
            mock_channel()
        }
    }

    struct MockConcurrentConnection;

    impl Connection for MockConcurrentConnection {
        async fn init(_uri: Uri, _opts: &GrpcOptions) -> YdbResult<Self> {
            CONCURRENT_INIT_STARTED.wait().await;
            CONCURRENT_INIT_RELEASE.wait().await;
            Ok(Self)
        }

        async fn channel(&self) -> YdbResult<Channel> {
            mock_channel()
        }
    }

    struct MockPendingConnection;

    impl Connection for MockPendingConnection {
        async fn init(_uri: Uri, _opts: &GrpcOptions) -> YdbResult<Self> {
            CANCELLED_INIT_STARTED.wait().await;
            std::future::pending().await
        }

        async fn channel(&self) -> YdbResult<Channel> {
            mock_channel()
        }
    }

    fn mock_channel() -> YdbResult<Channel> {
        let endpoint = tonic::transport::Endpoint::from_static("http://127.0.0.1:1");
        Ok(endpoint.connect_lazy())
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

        let observations = recorder.histogram_observations("ydb_grpc_connection_establish_seconds");
        assert_eq!(observations.len(), 1, "exactly one establish observation");
        assert!(
            observations[0] >= 0.0,
            "duration must be recorded in seconds"
        );
        Ok(())
    }

    #[tokio::test]
    async fn failed_connection_records_only_establish_error() -> YdbResult<()> {
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
            recorder.gauge_value_with_label("ydb_grpc_connections", "state", "active"),
            0.0
        );
        assert_eq!(
            recorder.keys("ydb_grpc_connections").len(),
            1,
            "a failed attempt must not create a failed or active gauge series"
        );

        let observations = recorder.histogram_observations("ydb_grpc_connection_establish_seconds");
        assert_eq!(observations.len(), 1);
        assert_eq!(
            recorder.label_of_last("ydb_grpc_connection_establish_seconds", "result"),
            Some("error".to_string())
        );
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
                .histogram_observations("ydb_grpc_connection_establish_seconds")
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

    #[tokio::test]
    async fn concurrent_connections_initialize_and_count_once() -> YdbResult<()> {
        let (pool, recorder) = pool_with_recorder::<MockConcurrentConnection>();
        let pool = Arc::new(pool);
        let uri = Uri::from_static("grpc://localhost:7135/path");

        let first = tokio::spawn({
            let pool = Arc::clone(&pool);
            let uri = uri.clone();
            async move { pool.connection(&uri).await }
        });
        let second = tokio::spawn({
            let pool = Arc::clone(&pool);
            async move { pool.connection(&uri).await }
        });

        CONCURRENT_INIT_STARTED.wait().await;
        tokio::task::yield_now().await;
        assert_eq!(
            recorder.gauge_value_with_label("ydb_grpc_connections", "state", "connecting"),
            1.0,
            "only the initializer may count an in-flight attempt"
        );
        assert_eq!(
            recorder.histogram_count("ydb_grpc_connection_establish_seconds"),
            0
        );

        CONCURRENT_INIT_RELEASE.wait().await;
        first.await.unwrap()?;
        second.await.unwrap()?;

        assert_eq!(
            recorder.histogram_count("ydb_grpc_connection_establish_seconds"),
            1,
            "concurrent waiters must not record establishment"
        );
        assert_eq!(
            recorder.gauge_value_with_label("ydb_grpc_connections", "state", "connecting"),
            0.0
        );
        assert_eq!(
            recorder.gauge_value_with_label("ydb_grpc_connections", "state", "active"),
            1.0,
            "concurrent callers must share one active pool entry"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_initialization_balances_connecting() {
        let (pool, recorder) = pool_with_recorder::<MockPendingConnection>();
        let pool = Arc::new(pool);
        let task = tokio::spawn({
            let pool = Arc::clone(&pool);
            async move {
                pool.connection(&Uri::from_static("grpc://localhost:7135/path"))
                    .await
            }
        });

        CANCELLED_INIT_STARTED.wait().await;
        assert_eq!(
            recorder.gauge_value_with_label("ydb_grpc_connections", "state", "connecting"),
            1.0
        );

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            recorder.gauge_value_with_label("ydb_grpc_connections", "state", "connecting"),
            0.0,
            "dropping the initializer must balance connecting"
        );
        assert_eq!(
            recorder.histogram_count("ydb_grpc_connection_establish_seconds"),
            0,
            "a cancelled attempt has no establishment result"
        );
        assert_eq!(
            recorder.gauge_value_with_label("ydb_grpc_connections", "state", "active"),
            0.0
        );
    }

    #[tokio::test]
    async fn dropping_pool_removes_active_entries() -> YdbResult<()> {
        let (pool, recorder) = pool_with_recorder::<MockOkConnection>();
        pool.connection(&Uri::from_static("grpc://localhost:7135/path"))
            .await?;
        assert_eq!(
            recorder.gauge_value_with_label("ydb_grpc_connections", "state", "active"),
            1.0
        );

        drop(pool);
        assert_eq!(
            recorder.gauge_value_with_label("ydb_grpc_connections", "state", "active"),
            0.0,
            "pool drop must remove its active gauge contribution"
        );
        Ok(())
    }

    #[test]
    fn default_metrics_recorder_smoke() {
        // Ensures the Default import stays valid for the pool's default constructor.
        let _recorder = DefaultMetricsRecorder::new();
    }
}
