//! Table Service: prepare and execute a data query on a pooled session (go-sdk: `Client.Do`).

use std::time::Duration;

use tokio::time::timeout;
use ydb::{ydb_params, ClientBuilder, Mode, Query, YdbError, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client =
        ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?.client()?;

    if let Ok(res) = timeout(Duration::from_secs(3), client.wait()).await {
        res?
    } else {
        return Err(YdbError::from("Connection timeout"));
    }

    let table_client = client.table_client();

    table_client
        .retry(|mut session| async move {
            let prepared = session
                .prepare_data_query("SELECT $v + $v AS res".to_string())
                .await?;

            let result = session
                .execute_prepared_query(
                    &prepared,
                    Query::new("").with_params(ydb_params!("$v" => 21_i32)),
                    Mode::OnlineReadonly,
                )
                .await?;

            let mut row = result.into_only_result()?.rows().next().unwrap();
            let res: i32 = row.remove_field_by_name("res")?.try_into()?;
            println!("prepared query result: {res}");
            assert_eq!(res, 42);
            Ok(())
        })
        .await?;

    Ok(())
}
