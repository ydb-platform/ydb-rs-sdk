use std::time::Duration;
use tokio::time::timeout;
use ydb::{ydb_params, ydb_struct, ClientBuilder, Query, Value, YdbError, YdbResult};

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
            "CREATE TABLE {table_name} (id Int64 NOT NULL, val Utf8, PRIMARY KEY(id))"
        ))
        .await?;

    // Create vec of structs, there values will insert to table
    let rows = vec![
        ydb_struct!(
            "id" => 1_i64,
            "val" => "test",
        ),
        ydb_struct!(
            "id" => 2_i64,
            "val" => "test2",
        ),
        ydb_struct!(
            "id" => 3_i64,
            "val" => "test3"),
    ];

    // example value will use only for type description
    let example = ydb_struct!(
        "id" => 1_i64,
        "val" => "test",
    );

    let list = Value::list_from(example, rows)?;

    let query = Query::new(
        "DECLARE $list AS List<Struct<
id: Int64,
val: Utf8,
>>;

UPSERT INTO test
SELECT * FROM AS_TABLE($list)
",
    )
    .with_params(ydb_params!("$list" => list));

    table_client
        .retry_transaction(|t| async {
            let mut t = t;
            t.query(query.clone()).await?;
            t.commit().await?;
            Ok(())
        })
        .await?;

    let example_key = Value::from(1_i64);
    let keys = Value::list_from(example_key, vec![1i64.into(), 3_i64.into(), 42_i64.into()])?;

    let result_set = table_client
        .retry_read_rows(format!("/local/{table_name}"), keys, None)
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
