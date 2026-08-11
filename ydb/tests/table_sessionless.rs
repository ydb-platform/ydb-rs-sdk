use std::time::Duration;

use ydb::{ClientBuilder, SessionPoolSettings, YdbResult};

#[tokio::test]
#[ignore] // need YDB access
async fn describe_table_options_does_not_acquire_a_session() -> YdbResult<()> {
    let connection_string = std::env::var("YDB_CONNECTION_STRING")
        .unwrap_or_else(|_| "grpc://localhost:2136/local".to_string());
    let client = ClientBuilder::new_from_connection_string(connection_string)?
        .build()
        .await?
        .with_session_pool(
            SessionPoolSettings::new()
                .with_limit(1)
                .with_acquire_timeout(Duration::from_millis(300)),
        )
        .await?;

    let mut query = client.query_client();
    let _held_stream = query.query("SELECT 1").await?;
    assert_eq!(client.session_pool_stats().in_use, 1);

    client.table_client().describe_table_options().await?;
    assert_eq!(client.session_pool_stats().in_use, 1);

    Ok(())
}
