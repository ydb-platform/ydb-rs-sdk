use std::time::Duration;
use tokio::time::timeout;
use ydb::{ClientBuilder, Value, YdbError, YdbResult, ydb_struct};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client =
        ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?.client()?;

    match timeout(Duration::from_secs(3), client.wait()).await {
        Ok(res) => res?,
        _ => {
            return Err(YdbError::from("Connection timeout"));
        }
    };

    let table_client = client.table_client();
    let mut query_client = client.query_client();
    let table_name = "test";

    let _ = query_client.exec(format!("DROP TABLE {table_name}")).await; // ignore drop error

    query_client
        .exec(format!(
            "CREATE TABLE {table_name} (id Int64 NOT NULL, val Utf8, PRIMARY KEY(id))",
        ))
        .await?;

    // Create vec of structs, there values will insert to table
    let rows: Vec<Value> = vec![
        ydb_struct!(
            "id" => 1_i64,
            "val" => Value::Text("test".to_string()),
        ),
        ydb_struct!(
            "id" => 2_i64,
            "val" => Value::Null,
        ),
    ];

    table_client
        .bulk_upsert(format!("/local/{table_name}"), rows)
        .await?;

    let result_set = client
        .query_client()
        .query_result_set(format!("SELECT * FROM {table_name} ORDER BY id"))
        .await?;

    let read_rows_id: YdbResult<Vec<i64>> = result_set
        .rows()
        .map(|mut row| {
            let val = row.remove_field_by_name("id")?;
            let res: i64 = val.try_into()?;
            Ok(res)
        })
        .collect();
    let read_rows_id = read_rows_id?;

    assert_eq!(vec![1, 2], read_rows_id);

    println!("OK");

    Ok(())
}
