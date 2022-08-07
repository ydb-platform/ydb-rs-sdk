use ydb::{ydb_params, ClientBuilder, Query, Row, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::from_str("grpc://localhost:2136?database=local")?.client()?;
    client.wait().await?;
    let table_client = client.table_client();
    let _ = table_client
        .retry_execute_scheme_query("DROP TABLE test")
        .await; // ignore drop error

    // create table
    table_client
        .retry_execute_scheme_query("CREATE TABLE test (id Int64, val Utf8, PRIMARY KEY(id))")
        .await?;

    // fill with data
    table_client
        .retry_transaction(|mut t| async move {
            // upsert 100 rows in loop
            // use upsert instead of insert because insert need check if previous row exist
            // and can't execute second time in the transaction
            for i in 1..100 {
                t.query(
                    Query::new(
                        "
                    DECLARE $id AS Int64;
                    DECLARE $val AS Utf8;

                    UPSERT INTO test (id, val) VALUES ($id, $val)
                    ",
                    )
                    .with_params(ydb_params!(
                        "$id" => i as i64,
                        "$val" => format!("val: {}", i)
                    )),
                )
                .await?;
            }
            t.commit().await?;
            Ok(())
        })
        .await
        .unwrap();

    // Select one row result
    let sum: Option<i64> = table_client
        .retry_transaction(|mut t| async move {
            let value = t
                .query(Query::new("SELECT SUM(id) AS sum FROM test"))
                .await?
                .into_only_row()?
                .remove_field_by_name("sum")?;
            let res = value.try_into(); // res: YdbResult<Option<i64>>
            Ok(res.unwrap())
        })
        .await?;
    println!("sum: {}", sum.unwrap_or(-1));

    // select first 10 rows
    let rows: Vec<Row> = table_client
        .retry_transaction(|mut t| async move {
            Ok(
                t.query(Query::new("SELECT * FROM test ORDER BY id LIMIT 10"))
                    .await?
                    .into_only_result()?
                    .rows()
                    .collect(),
            )
        })
        .await?;

    for mut row in rows {
        let id: Option<i64> = row.remove_field_by_name("id")?.try_into()?;
        let val: Option<String> = row.remove_field_by_name("val")?.try_into()?;
        println!("row id '{}' with value '{}'", id.unwrap(), val.unwrap())
    }
    Ok(())
}
