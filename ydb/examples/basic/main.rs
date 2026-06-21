//! Basic "series / seasons / episodes" demo via Query Service (parity with
//! `ydb-go-sdk/examples/basic/native/query`).

mod data;
mod series_ops;

use ydb::{ClientBuilder, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let connection_string = std::env::var("YDB_CONNECTION_STRING")
        .unwrap_or_else(|_| "grpc://localhost:2136/local".to_string());

    let client = ClientBuilder::new_from_connection_string(connection_string)?.client()?;
    client.wait().await?;

    let mut qc = client.query_client().clone_with_idempotent_operations(true);
    let prefix = "native/query";

    series_ops::drop_tables(&mut qc, prefix).await?;
    series_ops::create_tables(&mut qc, prefix).await?;
    series_ops::fill_tables(&mut qc, prefix, data::sample_data()).await?;
    series_ops::read_series(&mut qc, prefix).await?;

    Ok(())
}
