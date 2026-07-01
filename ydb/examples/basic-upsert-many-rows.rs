use std::time::Duration;
use tokio::time::timeout;
use ydb::{ydb_params, ydb_struct, ClientBuilder, Value, YdbError, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client =
        ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?.client()?;

    if let Ok(res) = timeout(Duration::from_secs(3), client.wait()).await {
        res?
    } else {
        return Err(YdbError::from("Connection timeout"));
    };

    let mut query_client = client.query_client();
    let _ = query_client.exec("DROP TABLE test").await; // ignore drop error

    query_client
        .exec("CREATE TABLE test (id Int64 NOT NULL, val Utf8, PRIMARY KEY(id))")
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
    ];

    // example value will use only for type description
    let example = ydb_struct!(
        "id" => 1_i64,
        "val" => "test",
    );

    let list = Value::list_from(example, rows)?;

    client
        .query_client()
        .exec(
            "
UPSERT INTO test
SELECT * FROM AS_TABLE($list)
",
        )
        .params(ydb_params!("$list" => list))
        .await?;

    let result_set = client
        .query_client()
        .query_result_set("SELECT * FROM test ORDER BY id")
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
