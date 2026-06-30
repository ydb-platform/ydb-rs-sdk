//! Table Service: streaming scan query via a pooled session (go-sdk: `Client.Do`).

use std::time::Duration;

use tokio::time::timeout;
use ydb::{ydb_struct, ClientBuilder, Query, YdbError, YdbResult};

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
    let table_name = "scan_query_demo";
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
            table_path,
            vec![
                ydb_struct!("id" => 1_i64, "val" => 100_i64),
                ydb_struct!("id" => 2_i64, "val" => 200_i64),
            ],
        )
        .await?;

    table_client
        .retry(|mut session| {
            let table_name = table_name.to_string();
            async move {
                let mut stream = session
                    .execute_scan_query(Query::new(format!(
                        "SELECT id, val FROM {table_name} ORDER BY id"
                    )))
                    .await?;

                let mut ids = Vec::new();
                while let Some(result_set) = stream.next().await? {
                    for mut row in result_set.rows() {
                        let id: i64 = row.remove_field_by_name("id")?.try_into()?;
                        ids.push(id);
                    }
                }
                println!("scan query ids: {ids:?}");
                Ok(())
            }
        })
        .await?;

    table_client
        .retry_execute_scheme_query(format!("DROP TABLE {table_name}"))
        .await?;

    Ok(())
}
