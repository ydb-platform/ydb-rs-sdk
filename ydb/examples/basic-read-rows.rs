#![recursion_limit = "256"]
use std::time::Duration;
use tokio::time::timeout;
use ydb::{ClientBuilder, Value, YdbError, YdbResult, ydb_params, ydb_struct};

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
            "CREATE TABLE {table_name} (id Int64 NOT NULL, val Utf8, PRIMARY KEY(id))"
        ))
        .await?;

    // Create vec of structs, there values will insert to table
    let rows = vec![
        ydb_struct!(
            "id" => 1_i64,
            "val" => Value::Text("test1".into()),
        ),
        ydb_struct!(
            "id" => 2_i64,
            "val" => Value::Text("test2".into()),
        ),
        ydb_struct!(
            "id" => 3_i64,
            "val" => Value::Text("test3".into())),
    ];

    // example value will use only for type description
    let example = ydb_struct!(
        "id" => 1_i64,
        "val" => "test",
    );

    let list = Value::list_from(example, rows)?;

    query_client
        .exec(
            "
UPSERT INTO test
SELECT * FROM AS_TABLE($list)
",
        )
        .params(ydb_params!("$list" => list))
        .await?;

    let keys = vec![
        ydb_struct!("id" => 1i64),
        ydb_struct!("id" => 3i64),
        ydb_struct!("id" => 42i64),
    ];

    let result_set = table_client
        .read_rows(format!("/local/{table_name}"), keys, None)
        .await?;

    let mut rows = result_set.rows();

    let row_to_val = |mut row: ydb::Row| -> YdbResult<(i64, String)> {
        let id: Option<i64> = row.remove_field_by_name("id")?.try_into()?;
        let val: Option<String> = row.remove_field_by_name("val")?.try_into()?;

        Ok((id.unwrap(), val.unwrap()))
    };

    assert_eq!(
        row_to_val(rows.next().unwrap()).unwrap(),
        (1i64, "test1".to_string())
    );

    assert_eq!(
        row_to_val(rows.next().unwrap()).unwrap(),
        (3i64, "test3".to_string())
    );

    assert!(rows.next().is_none());

    println!("OK");

    Ok(())
}
