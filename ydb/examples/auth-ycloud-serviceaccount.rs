use std::{env, str::FromStr};

use tracing::{info, Level};
use ydb::{ClientBuilder, Query, ServiceAccountCredentials, YdbResult};

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
    client.wait().await?;
    let sum: Option<decimal_rs::Decimal> = client
        .table_client()
        .retry_transaction(|mut t| async move {
            let res = t
                .query(Query::from(
                    "select CAST(\"-1233333333333333333333345.34\" AS Decimal(28, 2)) as sum",
                ))
                .await?;
            Ok(res.into_only_row()?.remove_field_by_name("sum")?)
        })
        .await?
        .try_into()
        .unwrap();
    info!("sum: {}", sum.unwrap().to_string());
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
