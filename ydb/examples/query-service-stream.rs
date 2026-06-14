//! Multi-result-set streaming via [`QueryExecutor::query`] (implicit sessions).

use ydb::{ClientBuilder, FromYdbRow, Row, YdbResult};

#[derive(Debug)]
struct ValueRow {
    a: i64,
}

impl FromYdbRow for ValueRow {
    fn from_row(mut row: Row) -> YdbResult<Self> {
        Ok(Self {
            a: row.remove_field_by_name("a")?.try_into()?,
        })
    }
}

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
        .client()?;
    client.wait().await?;

    let mut qc = client.query_client();
    let mut stream = qc.query("SELECT 42 AS a; SELECT 1 AS b, 2 AS c;").await?;

    let mut set_count = 0;
    while let Some(result_set) = stream.next_result_set().await? {
        for row in result_set {
            let typed = ValueRow::from_row(row)?;
            println!("a = {}", typed.a);
        }
        set_count += 1;
    }
    stream.close().await?;

    println!("result sets: {set_count}");
    Ok(())
}
