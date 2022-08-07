use std::collections::HashMap;
use ydb::{ydb_params, ClientBuilder, Query, Value, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
        .client()?;
    client.wait().await?;

    let table_client = client.table_client();

    // List
    table_client
        .retry_transaction(|mut t| async move {
            let source = vec![1_i32, 2, 3];
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
            println!("List: {:?}", res);
            Ok(())
        })
        .await?;

    // Struct
    table_client
        .retry_transaction(|mut t| async move {
            let source: HashMap<String, Value> = HashMap::from_iter([
                ("a".into(), 12_i32.into()),
                ("b".into(), "test".to_string().into()),
                ("c".into(), 1.0_f64.into()),
            ]);

            let res: HashMap<String, Value> = t
                .query(
                    Query::new(
                        "
            DECLARE $val AS Struct<
                a: Int32,
                b: Utf8,
                c: Double,
            >;

            SELECT $val AS res;
        ",
                    )
                    .with_params(ydb_params!("$val" => source.clone())),
                )
                .await?
                .into_only_row()?
                .remove_field_by_name("res")?
                .try_into()?;

            assert_eq!(source, res);
            println!("Struct: {:?}", res);
            Ok(())
        })
        .await?;

    println!("done");
    Ok(())
}
