use std::time::Duration;
use tokio::time::timeout;
use ydb::{ydb_struct, BulkRows, ClientBuilder, Query, Value, YdbError, YdbResult};

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
    let _ = table_client
        .retry_execute_scheme_query("DROP TABLE test")
        .await; // ignore drop error

    // create table
    table_client
        .retry_execute_scheme_query(
            "CREATE TABLE test (id Int64 NOT NULL, val Utf8, PRIMARY KEY(id))",
        )
        .await?;

    let my_optional_value: Option<String> = Some("hello".to_string());
    let ydb_value: Value = my_optional_value.into();

    let fields: Vec<(String, Value)> = vec![
        ("id".to_string(), 1_i64.into()),
        ("val".to_string(), ydb_value.clone()),
    ];

    // Create vec of structs, there values will insert to table
    let rows = vec![
        ydb_struct!(
            "id" => 1_i64,
            "val" => "test",
        ),
        ydb_struct!(
            "id" => 2_i64,
            "val" => Value::Null,
        ),
    ];

    table_client
        .retry_execute_bulk_upsert("/local/test".to_string(), BulkRows::new(fields, rows))
        .await?;

    let read = table_client
        .retry_transaction(|t| async {
            let mut t = t;
            let res = t
                .query(Query::new("SELECT * FROM test ORDER BY id"))
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
