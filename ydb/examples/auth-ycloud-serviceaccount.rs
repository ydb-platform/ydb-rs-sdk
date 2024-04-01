use std::{env, fs, str::FromStr};

use tracing::{info, Level};
use ydb::{ClientBuilder, Query, ServiceAccountCredentials, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    dotenv::dotenv().ok();
    init_logs();
    info!("Building client");

    let connection_string =
        env::var("YDB_CONNECTION_STRING").expect("YDB_CONNECTION_STRING not set");

    let client = ClientBuilder::new_from_connection_string(connection_string)?
        .with_credentials(ServiceAccountCredentials::from_env().unwrap())
        .client()?;

    info!("Waiting for client");
    client.wait().await?;
    let sum: i32 = client
        .table_client()
        .retry_transaction(|mut t| async move {
            let res = t.query(Query::from("SELECT 1 + 1 as sum")).await?;
            Ok(res.into_only_row()?.remove_field_by_name("sum")?)
        })
        .await?
        .try_into()
        .unwrap();
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
