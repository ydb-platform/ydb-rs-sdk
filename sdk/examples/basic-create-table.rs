use ydb::{ClientBuilder, YdbResult};

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

    table_client
        .retry_transaction(|mut t| async {
            for i in 1..100 {}
            return Ok(());
        })
        .await
        .unwrap();

    println!("done");
    return Ok(());
}
