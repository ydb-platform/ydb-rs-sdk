use std::collections::HashMap;
use std::iter::FromIterator;
use std::sync::Arc;
use std::time;
use std::time::UNIX_EPOCH;
use tokio::sync::Mutex as AsyncMutex;

use rand::distributions::{Alphanumeric, DistString};
use tonic::{Code, Status};
use tracing::trace;
use tracing_test::traced_test;

use crate::client_table::RetryOptions;
use crate::errors::{YdbError, YdbOrCustomerError, YdbResult};
use crate::query::Query;
use crate::table_service_types::CopyTableItem;
use crate::test_integration_helper::create_client;
use crate::transaction::Mode;
use crate::transaction::Transaction;
use crate::types::{Value, ValueList, ValueStruct};
use crate::{ydb_params, Bytes, TableClient};

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn create_session() -> YdbResult<()> {
    let res = create_client()
        .await?
        .table_client()
        .create_session()
        .await?;
    trace!("session: {:?}", res);
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn execute_data_query() -> YdbResult<()> {
    let client = create_client().await?;
    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let res = transaction.query("SELECT 1+1".into()).await?;
    trace!("result: {:?}", &res);
    assert_eq!(
        Value::Int32(2),
        res.into_only_result()
            .unwrap()
            .rows()
            .next()
            .unwrap()
            .remove_field(0)
            .unwrap()
    );
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn execute_data_query_field_name() -> YdbResult<()> {
    let client = create_client().await?;
    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let res = transaction.query("SELECT 1+1 as s".into()).await?;
    trace!("result: {:?}", &res);
    assert_eq!(
        Value::Int32(2),
        res.into_only_result()
            .unwrap()
            .rows()
            .next()
            .unwrap()
            .remove_field_by_name("s")
            .unwrap()
    );
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn execute_data_query_params() -> YdbResult<()> {
    let client = create_client().await?;
    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let mut params = HashMap::new();
    params.insert("$v".to_string(), Value::Int32(3));
    let res = transaction
        .query(
            Query::new(
                "
                DECLARE $v AS Int32;
                SELECT $v+$v
",
            )
            .with_params(params),
        )
        .await?;
    trace!("result: {:?}", res);
    assert_eq!(
        Value::Int32(6),
        res.into_only_result()
            .unwrap()
            .rows()
            .next()
            .unwrap()
            .remove_field(0)
            .unwrap()
    );
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore]
async fn query_yson() -> YdbResult<()>{
    let client = create_client().await?;

    let res = client
        .table_client()
        .retry_transaction(|mut t| async move {
            let tst_query = "DECLARE $p AS YSON; \
SELECT $p";

            let res = t
                .query(
                    Query::new(tst_query)
                        .with_params(ydb_params!("$p" => Value::Yson("[]".into()))),
                )
                .await?;

            Ok(res
                .into_only_result()?
                .rows()
                .next()
                .unwrap()
                .remove_field(0))
        })
        .await??;

    assert!(res == Value::Yson("[]".into()));

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn interactive_transaction() -> YdbResult<()> {
    let client = create_client().await?;

    client
        .table_client()
        .create_session()
        .await?
        .execute_schema_query(
            "CREATE TABLE test_values (id Int64, vInt64 Int64, PRIMARY KEY (id))".to_string(),
        )
        .await?;

    let mut tx_auto = client
        .table_client()
        .create_autocommit_transaction(Mode::SerializableReadWrite);

    let mut tx = client.table_client().create_interactive_transaction();
    tx.query(Query::new("DELETE FROM test_values")).await?;
    tx.commit().await?;

    let mut tx = client.table_client().create_interactive_transaction();
    tx.query(Query::new(
        "UPSERT INTO test_values (id, vInt64) VALUES (1, 2)",
    ))
    .await?;
    tx.query(
        Query::new(
            "
                DECLARE $key AS Int64;
                DECLARE $val AS Int64;

                UPSERT INTO test_values (id, vInt64) VALUES ($key, $val)
            ",
        )
        .with_params(HashMap::from([
            ("$key".into(), Value::Int64(2)),
            ("$val".into(), Value::Int64(3)),
        ])),
    )
    .await?;

    // check table before commit
    let auto_res = tx_auto
        .query(Query::new("SELECT vInt64 FROM test_values WHERE id=1"))
        .await?;
    assert!(auto_res.into_only_result().unwrap().rows().next().is_none());

    tx.commit().await?;

    // check table after commit
    let auto_res = tx_auto
        .query(Query::new("SELECT vInt64 FROM test_values WHERE id=1"))
        .await?;
    assert_eq!(
        Value::optional_from(Value::Int64(0), Some(Value::Int64(2)))?,
        auto_res
            .into_only_result()
            .unwrap()
            .rows()
            .next()
            .unwrap()
            .remove_field_by_name("vInt64")
            .unwrap()
    );

    client
        .table_client()
        .create_session()
        .await?
        .execute_schema_query("DROP TABLE test_values".to_string())
        .await?;

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn copy_table() -> YdbResult<()> {
    let client = create_client().await?;
    let table_client = client.table_client();

    let rand_str = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
    let table_name = format!("temp_table_{}", rand_str);
    let copy_table_name = format!("copy_{}", table_name);

    table_client
        .retry_with_session(RetryOptions::new(), |session| async {
            let mut session = session; // force borrow for lifetime of t inside closure
            session
                .execute_schema_query(format!(
                    "CREATE TABLE {} (id Int64, vInt64 Int64, PRIMARY KEY (id))",
                    table_name
                ))
                .await?;

            Ok(())
        })
        .await
        .unwrap();

    let mut transaction = table_client.create_autocommit_transaction(Mode::SerializableReadWrite);

    let mut interactive_tx = table_client.create_interactive_transaction();

    interactive_tx
        .query(format!("UPSERT INTO {} (id, vInt64) VALUES (1, 2)", table_name).into())
        .await?;

    interactive_tx.commit().await?;

    let database_path = client.database();
    table_client
        .copy_table(
            format!("{}/{}", database_path, table_name),
            format!("{}/{}", database_path, copy_table_name),
        )
        .await
        .unwrap();

    let res = transaction
        .query(format!("SELECT vInt64 FROM {} WHERE id=1", copy_table_name).into())
        .await?;

    assert_eq!(
        Value::optional_from(Value::Int64(0), Some(Value::Int64(2)))?,
        res.into_only_result()
            .unwrap()
            .rows()
            .next()
            .unwrap()
            .remove_field_by_name("vInt64")
            .unwrap()
    );

    for &target in [&table_name, &copy_table_name].iter() {
        table_client
            .retry_with_session(RetryOptions::new(), |session| async {
                let mut session = session; // force borrow for lifetime of t inside closure
                session
                    .execute_schema_query(format!("DROP TABLE {}", target))
                    .await?;

                Ok(())
            })
            .await
            .unwrap();
    }

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn copy_tables() -> YdbResult<()> {
    let client = create_client().await?;
    let table_client = client.table_client();

    let rand_str = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
    let table_name = format!("temp_table_{}", rand_str);
    let copy_table_name = format!("copy_{}", table_name);

    table_client
        .retry_with_session(RetryOptions::new(), |session| async {
            let mut session = session; // force borrow for lifetime of t inside closure
            session
                .execute_schema_query(format!(
                    "CREATE TABLE {} (id Int64, vInt64 Int64, PRIMARY KEY (id))",
                    table_name
                ))
                .await?;

            Ok(())
        })
        .await
        .unwrap();

    let mut transaction = table_client.create_autocommit_transaction(Mode::SerializableReadWrite);

    let mut interactive_tx = table_client.create_interactive_transaction();

    interactive_tx
        .query(format!("UPSERT INTO {} (id, vInt64) VALUES (1, 2)", table_name).into())
        .await?;

    interactive_tx.commit().await?;

    let database_path = client.database();
    table_client
        .copy_tables(vec![CopyTableItem::new(
            format!("{}/{}", database_path, table_name),
            format!("{}/{}", database_path, copy_table_name),
            true,
        )])
        .await
        .unwrap();

    let res = transaction
        .query(format!("SELECT vInt64 FROM {} WHERE id=1", copy_table_name).into())
        .await?;

    assert_eq!(
        Value::optional_from(Value::Int64(0), Some(Value::Int64(2)))?,
        res.into_only_result()
            .unwrap()
            .rows()
            .next()
            .unwrap()
            .remove_field_by_name("vInt64")
            .unwrap()
    );

    for &target in [&table_name, &copy_table_name].iter() {
        table_client
            .retry_with_session(RetryOptions::new(), |session| async {
                let mut session = session; // force borrow for lifetime of t inside closure
                session
                    .execute_schema_query(format!("DROP TABLE {}", target))
                    .await?;

                Ok(())
            })
            .await
            .unwrap();
    }

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn retry_test() -> YdbResult<()> {
    let client = create_client().await?;

    let attempt = Arc::new(AsyncMutex::new(0));
    let res = client
        .table_client()
        .retry_transaction(|t| async {
            let mut t = t; // force borrow for lifetime of t inside closure
            let mut locked_res = attempt.lock().await;
            *locked_res += 1;

            let res = t.query(Query::new("SELECT 1+1 as res")).await?;
            let res = res
                .into_only_result()
                .unwrap()
                .rows()
                .next()
                .unwrap()
                .remove_field_by_name("res")
                .unwrap();

            assert_eq!(Value::Int32(2), res);

            if *locked_res < 3 {
                return Err(YdbOrCustomerError::YDB(YdbError::TransportGRPCStatus(
                    Arc::new(Status::new(Code::Aborted, "test")),
                )));
            }
            t.commit().await?;
            Ok(*locked_res)
        })
        .await;

    match res {
        Ok(val) => assert_eq!(val, 3),
        Err(err) => panic!("retry test failed with error result: {:?}", err),
    }

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn scheme_query() -> YdbResult<()> {
    let client = create_client().await?;
    let table_client = client.table_client();

    let time_now = time::SystemTime::now().duration_since(UNIX_EPOCH)?;
    let table_name = format!("test_table_{}", time_now.as_millis());

    table_client
        .retry_with_session(RetryOptions::new(), |session| async {
            let mut session = session; // force borrow for lifetime of t inside closure
            session
                .execute_schema_query(format!(
                    "CREATE TABLE {} (id String, PRIMARY KEY (id))",
                    table_name
                ))
                .await?;

            Ok(())
        })
        .await
        .unwrap();

    table_client
        .retry_with_session(RetryOptions::new(), |session| async {
            let mut session = session; // force borrow for lifetime of t inside closure
            session
                .execute_schema_query(format!("DROP TABLE {}", table_name))
                .await?;

            Ok(())
        })
        .await
        .unwrap();

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn select_int() -> YdbResult<()> {
    let client = create_client().await?;
    let v = Value::Int32(123);

    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let res = transaction
        .query(
            Query::new(
                "
DECLARE $test AS Int32;

SELECT $test AS test;
",
            )
            .with_params(HashMap::from_iter([("$test".into(), v.clone())])),
        )
        .await?;

    let res = res.results.into_iter().next().unwrap();
    assert_eq!(1, res.columns().len());
    assert_eq!(v, res.rows().next().unwrap().remove_field_by_name("test")?);

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn select_optional() -> YdbResult<()> {
    let client = create_client().await?;
    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let res = transaction
        .query(
            Query::new(
                "
DECLARE $test AS Optional<Int32>;

SELECT $test AS test;
",
            )
            .with_params(HashMap::from_iter([(
                "$test".into(),
                Value::optional_from(Value::Int32(0), Some(Value::Int32(3)))?,
            )])),
        )
        .await?;

    let res = res.results.into_iter().next().unwrap();
    assert_eq!(1, res.columns().len());
    assert_eq!(
        Value::optional_from(Value::Int32(0), Some(Value::Int32(3)))?,
        res.rows().next().unwrap().remove_field_by_name("test")?
    );

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn select_list() -> YdbResult<()> {
    let client = create_client().await?;
    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let res = transaction
        .query(
            Query::new(
                "
DECLARE $l AS List<Int32>;

SELECT $l AS l;
",
            )
            .with_params(HashMap::from_iter([(
                "$l".into(),
                Value::List(Box::new(ValueList {
                    t: Value::Int32(0),
                    values: Vec::from([Value::Int32(1), Value::Int32(2), Value::Int32(3)]),
                })),
            )])),
        )
        .await?;
    trace!("{:?}", res);
    let res = res.results.into_iter().next().unwrap();
    assert_eq!(1, res.columns().len());
    assert_eq!(
        Value::list_from(
            Value::Int32(0),
            vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]
        )?,
        res.rows().next().unwrap().remove_field_by_name("l")?
    );
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn select_struct() -> YdbResult<()> {
    let client = create_client().await?;
    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let res = transaction
        .query(
            Query::new(
                "
DECLARE $l AS List<Struct<
    a: Int64
>>;

SELECT
    SUM(a) AS s
FROM
    AS_TABLE($l);
;
",
            )
            .with_params(HashMap::from_iter([(
                "$l".into(),
                Value::List(Box::new(ValueList {
                    t: Value::Struct(ValueStruct::from_names_and_values(
                        vec!["a".into()],
                        vec![Value::Int64(0)],
                    )?),
                    values: vec![
                        Value::Struct(ValueStruct::from_names_and_values(
                            vec!["a".into()],
                            vec![Value::Int64(1)],
                        )?),
                        Value::Struct(ValueStruct::from_names_and_values(
                            vec!["a".into()],
                            vec![Value::Int64(2)],
                        )?),
                        Value::Struct(ValueStruct::from_names_and_values(
                            vec!["a".into()],
                            vec![Value::Int64(3)],
                        )?),
                    ],
                })),
            )])),
        )
        .await?;
    trace!("{:?}", res);
    let res = res.results.into_iter().next().unwrap();
    assert_eq!(1, res.columns().len());

    assert_eq!(
        Value::optional_from(Value::Int64(0), Some(Value::Int64(6)))?,
        res.rows().next().unwrap().remove_field_by_name("s")?
    );
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn select_int64_null4() -> YdbResult<()> {
    let client = create_client().await?;
    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let res = transaction
        .query(Query::new(
            "
SELECT CAST(NULL AS Optional<Int64>)
;
",
        ))
        .await?;
    trace!("{:?}", res);
    let res = res.results.into_iter().next().unwrap();
    assert_eq!(1, res.columns().len());

    assert_eq!(
        Value::optional_from(Value::Int64(0), None)?,
        res.rows().next().unwrap().remove_field(0)?
    );
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn select_void_null() -> YdbResult<()> {
    let client = create_client().await?;
    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let res = transaction
        .query(Query::new(
            "
SELECT NULL
;
",
        ))
        .await?;
    trace!("{:?}", res);
    let res = res.results.into_iter().next().unwrap();
    assert_eq!(1, res.columns().len());

    assert_eq!(Value::Null, res.rows().next().unwrap().remove_field(0)?);
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn stream_query() -> YdbResult<()> {
    let client = create_client().await?.table_client();
    let mut session = client.create_session().await?;

    let _ = session
        .execute_schema_query("DROP TABLE stream_query".to_string())
        .await;

    session
        .execute_schema_query(
            "CREATE TABLE stream_query (id Int64, val Bytes, PRIMARY KEY (val))".into(),
        )
        .await?;

    const ONE_ROW_SIZE_BYTES: usize = 1024 * 1024;
    const KEY_SIZE_BYTES: usize = 8;

    fn gen_value_by_id(id: i64) -> Vec<u8> {
        const VECTOR_SIZE: usize = ONE_ROW_SIZE_BYTES - KEY_SIZE_BYTES;

        let mut res: Vec<u8> = Vec::with_capacity(VECTOR_SIZE);
        let mut last_byte: u8 = (id % 256) as u8;

        for _ in 0..VECTOR_SIZE {
            res.push(last_byte);
            last_byte = last_byte.wrapping_add(1);
        }

        res
    }

    async fn insert_values(client: &TableClient, ids: Vec<i64>) -> YdbResult<()> {
        client
            .retry_transaction(|tr| async {
                let mut ydb_values: Vec<Value> = Vec::with_capacity(ids.len());
                for v in ids.iter() {
                    ydb_values.push(Value::Struct(ValueStruct::from_names_and_values(
                        vec!["id".to_string(), "val".to_string()],
                        vec![
                            Value::Int64(*v),
                            Value::Bytes(Bytes::from(gen_value_by_id(*v))),
                        ],
                    )?))
                }

                let ydb_values = Value::list_from(ydb_values[0].clone(), ydb_values)?;

                let query = Query::new(
                    "
DECLARE $values AS List<Struct<
    id: Int64,
    val: Bytes,
> >;

UPSERT INTO stream_query
SELECT
    *
FROM
    AS_TABLE($values);
",
                )
                .with_params(ydb_params!(
                    "$values" => ydb_values
                ));

                let mut tr = tr;
                tr.query(query).await?;
                tr.commit().await?;
                Ok(())
            })
            .await?;

        Ok(())
    }

    // need send/receive more then 50MB
    let min_target_bytes = (60 * 1024 * 1024) as usize;
    let target_row_count = min_target_bytes / ONE_ROW_SIZE_BYTES + 1;
    let target_batch_count = 10;
    let target_batch_size = target_row_count / target_batch_count;
    let mut expected_sum: i64 = 0;

    let mut last_item_value = 0;
    for _ in 0..target_batch_count {
        let mut values = Vec::with_capacity(target_batch_size);
        for _ in 0..target_batch_size {
            last_item_value += 1;
            expected_sum += last_item_value;
            values.push(last_item_value);
        }
        insert_values(&client, values).await?;
    }
    let expected_item_count = last_item_value;

    let mut expected_id: i64 = 1;
    let query = Query::new("SELECT * FROM stream_query ORDER BY id".to_string());
    let mut res = session.execute_scan_query(query).await?;
    let mut sum: i64 = 0;
    let mut item_count = 0;
    let mut result_set_count = 0;
    while let Some(result_set) = res.next().await? {
        result_set_count += 1;

        for mut row in result_set.into_iter() {
            item_count += 1;
            match row.remove_field_by_name("id")? {
                Value::Optional(boxed_id) => match boxed_id.value.unwrap() {
                    Value::Int64(id) => {
                        assert_eq!(id, expected_id);
                        sum += id
                    }
                    id => panic!("unexpected ydb boxed_id type: {:?}", id),
                },
                id => panic!("unexpected ydb id type: {:?}", id),
            };

            match row.remove_field_by_name("val")? {
                Value::Optional(boxed_val) => match boxed_val.value.unwrap() {
                    Value::Bytes(content) => {
                        assert_eq!(gen_value_by_id(expected_id), Vec::<u8>::from(content))
                    }
                    val => panic!("unexpected ydb id type: {:?}", val),
                },
                val => panic!("unexpected ydb boxed_id type: {:?}", val),
            };

            expected_id += 1;
        }
    }

    assert_eq!(expected_item_count, item_count);
    assert_eq!(expected_sum, sum);

    // TODO: need improove for non flap in tests for will strong more then 1
    assert!(result_set_count > 1); // ensure get multiply results
    Ok(())
}
