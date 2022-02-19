use std::collections::HashMap;
use ydb::{ClientBuilder, Query, Row, Value, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::from_str("grpc://localhost:2136?database=local")?.client()?;
    client.wait().await?;

    let table_client = client.table_client();

    // List
    table_client
        .retry_transaction(|mut t| async move {
            let source = vec![1 as i32, 2, 3];
            let source_value = Value::from_iter(source.clone());
            let res: Vec<i32> = t
                .query(
                    Query::new(
                        "
                    DECLARE $val AS List<Int32>;

                    SELECT $val AS val;
                ",
                    )
                    .with_params(HashMap::from_iter(vec![(
                        "$val".to_string(),
                        source_value, //
                    )])),
                )
                .await?
                .into_only_row()?
                .remove_field_by_name("val")?
                .try_into()?;

            assert_eq!(vec![1, 2, 3], res);
            return Ok(());
        })
        .await?;

    println!("done");
    return Ok(());
}
