#![recursion_limit = "256"]
use std::time::Duration;
use std::{env, str::FromStr};
use tokio::time::timeout;
use tracing::{Level, info};
use ydb::{ClientBuilder, ServiceAccountCredentials, YdbError, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    init_logs();
    info!("Building client");

    let connection_string =
        env::var("YDB_CONNECTION_STRING").map_err(|_| "YDB_CONNECTION_STRING not set")?;

    let client = ClientBuilder::new_from_connection_string(connection_string)?
        // get credentials from file located at path specified in YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS
        .with_credentials(ServiceAccountCredentials::from_env()?)
        //  or with credentials from env:
        // .with_credentials(FromEnvCredentials::new()?)
        // or you can use custom url
        // .with_credentials(ServiceAccountCredentials::from_env()?.with_url("https://iam.api.cloud.yandex.net/iam/v1/tokens"))
        .client()?;

    info!("Waiting for client");

    match timeout(Duration::from_secs(3), client.wait()).await {
        Ok(res) => res?,
        _ => {
            return Err(YdbError::from("Connection timeout"));
        }
    };

    let mut row = client
        .query_client()
        .query_row("SELECT 1 + 1 as sum")
        .await?;
    let sum: i32 = row.remove_field_by_name("sum")?.try_into()?;
    info!("sum: {}", sum);
    Ok(())
}

fn init_logs() {
    let level = env::var("RUST_LOG").unwrap_or("INFO".to_string());
    let log_level = Level::from_str(&level).unwrap();
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(log_level)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Error setting subscriber");
}
