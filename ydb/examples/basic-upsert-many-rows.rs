use ydb::{ClientBuilder, Query, Value, ydb_params, ydb_struct, YdbResult};

#[tokio::main]
async fn main()->YdbResult<()>{
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
        .client()?;
    client.wait().await?;
    let table_client = client.table_client();
    let _ = table_client
        .retry_execute_scheme_query("DROP TABLE test")
        .await; // ignore drop error

    // create table
    table_client
        .retry_execute_scheme_query("CREATE TABLE test (id Int64 NOT NULL, val Utf8, PRIMARY KEY(id))")
        .await?;

    // Create vec of structs, there values will insert to table
    let rows = vec![
        ydb_struct!(
            "id" => 1 as i64,
            "val" => "test",
        ),
        ydb_struct!(
            "id" => 2 as i64,
            "val" => "test2",
        )
    ];

    // example value will use only for type description
    let example = ydb_struct!(
        "id" => 1 as i64,
        "val" => "test",
    );

    let list = Value::list_from(example, rows)?;

    let query = Query::new("DECLARE $list AS List<Struct<
id: Int64,
val: Utf8,
>>;

UPSERT INTO test
SELECT * FROM AS_TABLE($list)
")
        .with_params(ydb_params!("$list" => list));

    table_client.retry_transaction(|t| async {
        let mut t = t;
        t.query(query.clone()).await?;
        t.commit().await?;
        Ok(())
    }).await?;

    let read = table_client.retry_transaction(|t| async {
        let mut t = t;
        let res = t.query(Query::new("SELECT * FROM test ORDER BY id")).await?;
        Ok(res)
    }).await?;

    let read_rows_id: YdbResult<Vec<i64>> = read.into_only_result()?.rows().map(|mut row|{
        let val = row.remove_field_by_name("id")?;
        let res: i64 = val.try_into()?;
        Ok(res)
    }).collect();
    let read_rows_id = read_rows_id?;

    assert_eq!(vec![1,2], read_rows_id);

    println!("OK");

    Ok(())
}