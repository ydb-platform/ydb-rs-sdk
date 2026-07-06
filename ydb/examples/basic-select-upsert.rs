use std::time::Duration;
use tokio::time::timeout;
use ydb::{ClientBuilder, Row, YdbError, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client =
        ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?.client()?;

    if let Ok(res) = timeout(Duration::from_secs(3), client.wait()).await {
        res?
    } else {
        return Err(YdbError::from("Connection timeout"));
    };

    let mut qc = client.query_client();
    let _ = qc.exec("DROP TABLE test").await; // ignore drop error

    qc.exec("CREATE TABLE test (id Int64, val Utf8, PRIMARY KEY(id))")
        .await?;

    // fill with data
    qc.retry_tx(async |tx| {
        for i in 1..100 {
            tx.exec("UPSERT INTO test (id, val) VALUES ($id, $val)")
                .param("$id", i as i64)
                .param("$val", format!("val: {i}"))
                .await?;
        }
        Ok(())
    })
    .await?;

    // Select one row result
    let mut row = qc.query_row("SELECT SUM(id) AS sum FROM test").await?;
    let sum: Option<i64> = row.remove_field_by_name("sum")?.try_into()?;
    println!("sum: {}", sum.unwrap_or(-1));

    // select first 10 rows
    let result_set = qc
        .query_result_set("SELECT * FROM test ORDER BY id LIMIT 10")
        .await?;
    let rows: Vec<Row> = result_set.rows().collect();

    for mut row in rows {
        let id: Option<i64> = row.remove_field_by_name("id")?.try_into()?;
        let val: Option<String> = row.remove_field_by_name("val")?.try_into()?;
        println!("row id '{}' with value '{}'", id.unwrap(), val.unwrap())
    }
    Ok(())
}
