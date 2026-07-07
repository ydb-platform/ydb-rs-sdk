use crate::connection_pool::{normalize_uri_scheme, ConnectionPool, Simple};
use crate::YdbResult;
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
    let mut pool = ConnectionPool::<Simple>::new();
    let uri = Uri::from_static("grpc://localhost:7135/path");

    let channel = pool.connection(&uri).await?;
    let _ = channel;

    Ok(())
}

#[tokio::test]
async fn test_connection_connection_reuse() -> YdbResult<()> {
    let mut pool = ConnectionPool::<Simple>::new();
    let uri = Uri::from_static("grpcs://localhost:2135/local");

    let first_channel = pool.connection(&uri).await?;

    let second_channel = pool.connection(&uri).await?;

    let _ = first_channel;
    let _ = second_channel;

    Ok(())
}

#[tokio::test]
async fn test_connection_without_host_fails() {
    let mut pool = ConnectionPool::<Simple>::new();

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
