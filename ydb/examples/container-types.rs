#![recursion_limit = "256"]
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::timeout;
use ydb::{ClientBuilder, Value, YdbError, YdbResult, ydb_params};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client_builder = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?;

    let client = match timeout(Duration::from_secs(3), client_builder.build()).await {
        Ok(res) => res?,
        _ => {
            return Err(YdbError::from("Connection timeout"));
        }
    };

    let mut qc = client.query_client();

    // List
    {
        let source = vec![1_i32, 2, 3];
        let source_value = Value::from_iter(source.clone());
        let mut row = qc
            .query_row("SELECT $val AS val")
            .params(HashMap::from_iter(vec![("$val".to_string(), source_value)]))
            .await?;
        let res: Vec<i32> = row.remove_field_by_name("val")?.try_into()?;

        assert_eq!(vec![1, 2, 3], res);
        println!("List: {res:?}");
    }

    // Struct
    {
        let source: HashMap<String, Value> = HashMap::from_iter([
            ("a".into(), 12_i32.into()),
            ("b".into(), "test".to_string().into()),
            ("c".into(), 1.0_f64.into()),
        ]);

        let mut row = qc
            .query_row("SELECT $val AS res")
            .params(ydb_params!("$val" => source.clone()))
            .await?;
        let res: HashMap<String, Value> = row.remove_field_by_name("res")?.try_into()?;

        assert_eq!(source, res);
        println!("Struct: {res:?}");
    }

    // Set - YDB transfers Set<T> as Dict<T, Void>
    {
        let source: HashSet<i32> = HashSet::from([1, 2, 3]);

        let mut row = qc
            .query_row("SELECT $val AS res")
            .params(ydb_params!("$val" => source.clone()))
            .await?;
        let res: HashSet<i32> = row.remove_field_by_name("res")?.try_into()?;

        assert_eq!(source, res);

        // the server sees a real set, not an opaque value
        let mut row = qc
            .query_row("SELECT DictContains($val, 2) AS has")
            .params(ydb_params!("$val" => source))
            .await?;
        let has: bool = row.remove_field_by_name("has")?.try_into()?;

        assert!(has);
        println!("Set: {res:?}, contains 2: {has}");
    }

    println!("done");
    Ok(())
}
