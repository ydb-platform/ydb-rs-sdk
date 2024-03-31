use std::{env, fs};

use tracing::{info, Level};
use ydb::{ClientBuilder, Query, ServiceAccount, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    dotenv::dotenv().ok();
    init_logs();
    info!("Building client");

    let endpoint = env::var("YDB_ENDPOINT").expect("YDB_ENDPOINT not set");
    let account_id = env::var("YC_ACCOUNT_ID").expect("YC_ACCOUNT_ID not set");
    let key_id = env::var("YC_KEY_ID").expect("YC_KEY_ID not set");
    let key_file_path = env::var("YC_SA_KEY_FILE").expect("YC_SA_KEY_FILE not set");

    let key_file = fs::read_to_string(key_file_path).expect("Error reading key file");

    let client = ClientBuilder::new_from_connection_string(endpoint)?
        .with_credentials(ServiceAccount::new(account_id, key_id, key_file))
        .client()?;
    info!("Waiting for client");
    client.wait().await?;
    let sum: i32 = client
        .table_client()
        .retry_transaction(|mut t| async move {
            let res = t.query(Query::from("SELECT 1 + 1 AS sum")).await?;
            Ok(res.into_only_row()?.remove_field_by_name("sum")?)
        })
        .await?
        .try_into()
        .unwrap();
    info!("sum: {}", sum);
    Ok(())
}

fn init_logs() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Error setting subscriber");
}
