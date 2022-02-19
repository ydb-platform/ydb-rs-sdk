use std::collections::HashMap;
use ydb::{ClientBuilder, Query, Row, Value, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::from_str("grpc://localhost:2136?database=local")?.client()?;
    client.wait().await?;

    let table_client = client.table_client();

    // List
    let _ = table_client
        .retry_transaction(|mut t| async move {
            let res: Vec<i32> = t
                .query(Query::new("SELECT AsList(1,2,3) AS val"))
                .await?
                .into_only_row()?
                .remove_field_by_name("val")?
                .try_into()?;
            println!("{:?}", res);
            return Ok(());
        })
        .await?;

    println!("done");
    return Ok(());
}
