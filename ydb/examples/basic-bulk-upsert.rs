use std::time::Duration;
use tokio::time::timeout;
use ydb::{ydb_struct, ClientBuilder, Query, Value, YdbError, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
        .client()?;

    if let Ok(res) = timeout(Duration::from_secs(3), client.wait()).await {
        res?
    } else {
        return Err(YdbError::from("Connection timeout"));
    };

    let table_client = client.table_client();
    let table_name = "test";

    let _ = table_client
        .retry_execute_scheme_query(format!("DROP TABLE {table_name}"))
        .await; // ignore drop error

    // create table
    table_client
        .retry_execute_scheme_query(format!(
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
        .retry_execute_bulk_upsert(format!("/local/{table_name}").to_string(), rows)
        .await?;

    let read = table_client
        .retry_transaction(|t| async {
            let mut t = t;
            let res = t
                .query(Query::new(format!(
                    "SELECT * FROM {table_name} ORDER BY id"
                )))
                .await?;
            Ok(res)
        })
        .await?;

    let read_rows_id: YdbResult<Vec<i64>> = read
        .into_only_result()?
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
