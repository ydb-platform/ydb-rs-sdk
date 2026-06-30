//! Table Service: stream-read a table via [`Session`] (session-only API).

use std::time::Duration;

use tokio::time::timeout;
use ydb::{
    ydb_struct, ClientBuilder, ReadTableOptions, YdbError, YdbResult,
};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client =
        ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?.client()?;

    if let Ok(res) = timeout(Duration::from_secs(3), client.wait()).await {
        res?
    } else {
        return Err(YdbError::from("Connection timeout"));
    }

    let table_client = client.table_client();
    let table_name = "stream_read_demo";
    let table_path = format!("/local/{table_name}");

    let _ = table_client
        .retry_execute_scheme_query(format!("DROP TABLE {table_name}"))
        .await;

    table_client
        .retry_execute_scheme_query(format!(
            "CREATE TABLE {table_name} (id Int64 NOT NULL, val Int64, PRIMARY KEY (id))"
        ))
        .await?;

    table_client
        .retry_bulk_upsert(
            table_path.clone(),
            vec![
                ydb_struct!("id" => 1_i64, "val" => 10_i64),
                ydb_struct!("id" => 2_i64, "val" => 20_i64),
            ],
        )
        .await?;

    let mut session = table_client.create_session().await?;
    let mut stream = session
        .stream_read_table(table_path, ReadTableOptions::default())
        .await?;

    let mut row_count = 0usize;
    while let Some(result_set) = stream.next_result_set().await? {
        row_count += result_set.rows().count();
    }
    println!("stream_read_table rows: {row_count}");

    table_client
        .retry_execute_scheme_query(format!("DROP TABLE {table_name}"))
        .await?;

    Ok(())
}
