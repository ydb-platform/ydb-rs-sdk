use itertools::Itertools;
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
use crate::table_requests::{
    AlterTableRequest, CreateTableRequest, DropTableRequest, ReadRowsRequest, ReadTableKeyBound,
    ReadTableKeyRange, ReadTableOptions, TableColumn,
};
use crate::table_service_types::{CopyTableItem, IndexType, StoreType};
use crate::test_integration_helper::create_client;
use crate::transaction::Mode;
use crate::transaction::Transaction;
use crate::types::{Value, ValueList, ValueStruct};
use crate::{ydb_params, ydb_struct, Bytes, TableClient};

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
async fn explain_data_query() -> YdbResult<()> {
    let client = create_client().await?;
    let table_client = client.table_client();

    // Execute explain data query with retry policy using a system query
    let result = table_client
        .retry_explain_data_query("SELECT MIN(NodeId) FROM `.sys/nodes`", false)
        .await?;

    // Verify that we got valid explain results
    assert!(
        !result.query_ast.is_empty(),
        "Query AST should not be empty"
    );
    assert!(
        !result.query_plan.is_empty(),
        "Query Plan should not be empty"
    );

    // Query full diagnostics should be empty when not enabled
    assert!(
        result.query_full_diagnostics.is_empty(),
        "Full diagnostics should be empty when not enabled"
    );

    // Test with full diagnostics enabled
    let result_with_diagnostics = table_client
        .retry_explain_data_query("SELECT MIN(NodeId) FROM `.sys/nodes`", true)
        .await?;

    // Verify that we got valid explain results with diagnostics
    assert!(
        !result_with_diagnostics.query_ast.is_empty(),
        "Query AST should not be empty"
    );
    assert!(
        !result_with_diagnostics.query_plan.is_empty(),
        "Query Plan should not be empty"
    );

    // Query full diagnostics should not be empty when enabled
    assert!(
        !result_with_diagnostics.query_full_diagnostics.is_empty(),
        "Full diagnostics should not be empty when enabled"
    );

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
async fn query_yson() -> YdbResult<()> {
    let client = create_client().await?;

    let res = client
        .table_client()
        .retry_transaction(|mut t| async move {
            let tst_query = "SELECT $p";

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
    let table_name = format!("temp_table_{rand_str}");
    let copy_table_name = format!("copy_{table_name}");

    table_client
        .retry_with_session(RetryOptions::new(), |session| async {
            let mut session = session; // force borrow for lifetime of t inside closure
            session
                .execute_schema_query(format!(
                    "CREATE TABLE {table_name} (id Int64, vInt64 Int64, PRIMARY KEY (id))"
                ))
                .await?;

            Ok(())
        })
        .await
        .unwrap();

    let mut transaction = table_client.create_autocommit_transaction(Mode::SerializableReadWrite);

    let mut interactive_tx = table_client.create_interactive_transaction();

    interactive_tx
        .query(format!("UPSERT INTO {table_name} (id, vInt64) VALUES (1, 2)").into())
        .await?;

    interactive_tx.commit().await?;

    let database_path = client.database();
    table_client
        .copy_table(
            format!("{database_path}/{table_name}"),
            format!("{database_path}/{copy_table_name}"),
        )
        .await
        .unwrap();

    let res = transaction
        .query(format!("SELECT vInt64 FROM {copy_table_name} WHERE id=1").into())
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
                    .execute_schema_query(format!("DROP TABLE {target}"))
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
    let table_name = format!("temp_table_{rand_str}");
    let copy_table_name = format!("copy_{table_name}");

    table_client
        .retry_with_session(RetryOptions::new(), |session| async {
            let mut session = session; // force borrow for lifetime of t inside closure
            session
                .execute_schema_query(format!(
                    "CREATE TABLE {table_name} (id Int64, vInt64 Int64, PRIMARY KEY (id))"
                ))
                .await?;

            Ok(())
        })
        .await
        .unwrap();

    let mut transaction = table_client.create_autocommit_transaction(Mode::SerializableReadWrite);

    let mut interactive_tx = table_client.create_interactive_transaction();

    interactive_tx
        .query(format!("UPSERT INTO {table_name} (id, vInt64) VALUES (1, 2)").into())
        .await?;

    interactive_tx.commit().await?;

    let database_path = client.database();
    table_client
        .copy_tables(vec![CopyTableItem::new(
            format!("{database_path}/{table_name}"),
            format!("{database_path}/{copy_table_name}"),
            true,
        )])
        .await
        .unwrap();

    let res = transaction
        .query(format!("SELECT vInt64 FROM {copy_table_name} WHERE id=1").into())
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
                    .execute_schema_query(format!("DROP TABLE {target}"))
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
        Err(err) => panic!("retry test failed with error result: {err:?}"),
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
                    "CREATE TABLE {table_name} (id String, PRIMARY KEY (id))"
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
                .execute_schema_query(format!("DROP TABLE {table_name}"))
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
async fn read_rows() -> YdbResult<()> {
    const TABLE_NAME: &str = "read_rows";
    const TABLE_PATH: &str = "/local/read_rows";

    let client = create_client().await?;
    let table_client = client.table_client();

    table_client
        .retry_with_session(RetryOptions::new(), |mut session| async move {
            session
                .execute_schema_query(format!(
                    "CREATE TABLE {TABLE_NAME} (id Int64 NOT NULL, first Int64 NOT NULL, second Int64 NOT NULL, PRIMARY KEY (id))"
                ))
                .await?;

            Ok(())
        })
        .await
        .unwrap();

    let values: [(i64, i64); 4] = [(0, 0), (0, 1), (1, 0), (1, 1)];
    let ydb_values = values.map(|pair| (Value::Int64(pair.0), Value::Int64(pair.1)));

    let rows = values
        .into_iter()
        .enumerate()
        .map(|t| {
            let (id, (first, second)) = t;

            ydb_struct!("id" => id as i64, "first" => first, "second" => second)
        })
        .collect_vec();

    table_client
        .retry_bulk_upsert(TABLE_PATH, rows)
        .await?;

    // Empty
    let empty = table_client.retry_read_rows(TABLE_PATH, vec![], None).await;
    assert_eq!(empty.unwrap().rows().count(), 0);

    // Non-list keys
    let non_structs = table_client
        .retry_read_rows(TABLE_PATH, vec![Value::Int64(1i64)], None)
        .await;
    assert!(non_structs.is_err());

    let vec_to_values = |ids: Vec<i64>| {
        ids.into_iter()
            .map(|id| ydb_struct!("id" => id))
            .collect_vec()
    };

    // Basic all columns
    let all_columns = table_client
        .retry_read_rows(TABLE_PATH, vec_to_values((0i64..4i64).collect_vec()), None)
        .await;

    for (mut row, (first, second)) in all_columns.unwrap().rows().zip(ydb_values.iter()) {
        assert_eq!(&row.remove_field_by_name("first").unwrap(), first);
        assert_eq!(&row.remove_field_by_name("second").unwrap(), second);
    }

    // Basic reversed
    let all_columns_rev = table_client
        .retry_read_rows(
            TABLE_PATH,
            vec_to_values((0i64..4i64).rev().collect_vec()),
            None,
        )
        .await;
    for (mut row, (first, second)) in all_columns_rev.unwrap().rows().zip(ydb_values.iter().rev()) {
        assert_eq!(&row.remove_field_by_name("first").unwrap(), first);
        assert_eq!(&row.remove_field_by_name("second").unwrap(), second);
    }

    // Partial
    let keys = vec![0i64, 2i64, 4i64, 6i64];
    let partial = table_client
        .retry_read_rows(
            TABLE_PATH,
            vec_to_values(keys.clone()),
            Some(vec!["first".into()]),
        )
        .await;
    let rows = partial
        .unwrap()
        .rows()
        .map(|mut t| {
            assert!(t.remove_field_by_name("second").is_err());
            assert!(t.remove_field_by_name("third").is_err());
            t.remove_field_by_name("first").unwrap()
        })
        .collect_vec();

    for key in keys {
        if let Some((first, _)) = ydb_values.get(key as usize) {
            assert!(rows.contains(first));
        }
    }

    // Unknown column
    let unknown = table_client
        .retry_read_rows(
            TABLE_PATH,
            vec_to_values(vec![1i64]),
            Some(vec!["first".into(), "unknown".into()]),
        )
        .await;
    assert!(unknown.is_err());

    // Clear table
    table_client
        .retry_with_session(RetryOptions::new(), |mut session| async move {
            session
                .execute_schema_query(format!("DROP TABLE {TABLE_NAME}"))
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
async fn select_with_u8_param() -> YdbResult<()> {
    let client = create_client().await?;
    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let res = transaction
        .query(
            Query::from(
                r#"
            SELECT $val as s
        "#,
            )
            .with_params(ydb_params!(
                "$val" => 99u8
            )),
        )
        .await?;
    trace!("result: {:?}", &res);
    assert_eq!(
        Value::Uint8(99u8),
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
async fn select_with_u16_param() -> YdbResult<()> {
    let client = create_client().await?;
    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let res = transaction
        .query(
            Query::from(
                r#"
            SELECT $val as s
        "#,
            )
            .with_params(ydb_params!(
                "$val" => 34111u16
            )),
        )
        .await?;
    trace!("result: {:?}", &res);
    assert_eq!(
        Value::Uint16(34111u16),
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
                    id => panic!("unexpected ydb boxed_id type: {id:?}"),
                },
                id => panic!("unexpected ydb id type: {id:?}"),
            };

            match row.remove_field_by_name("val")? {
                Value::Optional(boxed_val) => match boxed_val.value.unwrap() {
                    Value::Bytes(content) => {
                        assert_eq!(gen_value_by_id(expected_id), Vec::<u8>::from(content))
                    }
                    val => panic!("unexpected ydb id type: {val:?}"),
                },
                val => panic!("unexpected ydb boxed_id type: {val:?}"),
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

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn bulk_upsert() -> YdbResult<()> {
    let client = create_client().await?;
    let table_client = client.table_client();
    let table_name = "bulk_upsert";

    table_client
        .retry_execute_scheme_query(format!(
            "
                CREATE TABLE {table_name} (
                    id Int64 NOT NULL,
                    val Utf8,
                    PRIMARY KEY (id)
                );
            "
        ))
        .await?;

    let rows = vec![
        ydb_struct!(
            "id" => 3_i64,
            "val" => Value::Text("test".to_string()),
        ),
        ydb_struct!(
            "id" => 6_i64,
            "val" => Value::Null,
        ),
    ];

    table_client
        .retry_bulk_upsert(format!("/local/{table_name}"), rows)
        .await?;

    let read = table_client
        .retry_transaction(|t| async {
            let mut t = t;
            let res = t
                .query(Query::new(format!(
                    "SELECT * FROM {table_name} ORDER BY id"
                )))
                .await?;
            Ok(res)
        })
        .await?;

    let read_rows_id: YdbResult<Vec<i64>> = read
        .into_only_result()?
        .rows()
        .map(|mut row| {
            let val = row.remove_field_by_name("id")?;
            let res: i64 = val.try_into()?;
            Ok(res)
        })
        .collect();
    let read_rows_id = read_rows_id?;

    assert_eq!(vec![3, 6], read_rows_id);

    table_client
        .retry_with_session(RetryOptions::new(), |session| async {
            let mut session = session; // force borrow for lifetime of t inside closure
            session
                .execute_schema_query(format!("DROP TABLE {table_name}"))
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
async fn describe_table() -> YdbResult<()> {
    let client = create_client().await?;
    let table_client = client.table_client();
    let table_name = "temp_describe_test";

    table_client
        .retry_with_session(RetryOptions::new(), |session| async {
            let mut session = session;
            session
                .execute_schema_query(format!("DROP TABLE IF EXISTS {table_name}"))
                .await?;
            session
                .execute_schema_query(format!(
                    "
                CREATE TABLE {table_name} (
                    id Utf8 NOT NULL,
                    id_hash Uint32 NOT NULL,
                    timestamp Timestamp,
                    host Utf8,
                    message Utf8,
                    level Int32,
                    payload JsonDocument,
                    optional_field Int32,
                    price Decimal(22, 9),
                    PRIMARY KEY(id_hash, id),
                    INDEX idx_timestamp GLOBAL ON (timestamp),
                    INDEX idx_host GLOBAL ON (host)
                );
            "
                ))
                .await?;
            Ok(())
        })
        .await?;

    let database_path = client.database();
    let table_desc = table_client
        .describe_table(format!("{database_path}/{table_name}"))
        .await?;

    trace!("describe_table result: {:?}", table_desc);

    assert_eq!(table_desc.columns.len(), 9);
    assert_eq!(table_desc.primary_key, vec!["id_hash", "id"]);
    assert_eq!(table_desc.indexes.len(), 2);
    assert_eq!(table_desc.store_type, StoreType::Unspecified);

    let id_col = table_desc.columns.iter().find(|c| c.name == "id").unwrap();
    assert!(matches!(id_col.type_value, Ok(Value::Text(_))));

    let id_hash_col = table_desc
        .columns
        .iter()
        .find(|c| c.name == "id_hash")
        .unwrap();
    assert!(matches!(id_hash_col.type_value, Ok(Value::Uint32(_))));

    let price_col = table_desc
        .columns
        .iter()
        .find(|c| c.name == "price")
        .unwrap();
    match &price_col.type_value {
        Ok(Value::Optional(opt)) => match &opt.t {
            Value::Decimal(d) => {
                // Verify that precision and scale are preserved from the schema
                assert!(d.precision() > 0, "precision should be set from schema");
            }
            _ => panic!("Expected Optional<Decimal>"),
        },
        Err(e) => panic!("Type conversion failed: {:?}", e),
        _ => panic!("Expected Ok(Optional<Decimal>)"),
    }

    for idx in &table_desc.indexes {
        assert_eq!(idx.index_type, IndexType::Global);
    }

    table_client
        .retry_with_session(RetryOptions::new(), |session| async {
            let mut session = session;
            session
                .execute_schema_query(format!("DROP TABLE {table_name}"))
                .await?;
            Ok(())
        })
        .await?;

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn grpc_max_message_size_limit_exceeded() -> YdbResult<()> {
    use crate::test_helpers::test_client_builder;

    // 1 MiB limit — discovery and small queries still fit, but a ~2 MiB UPSERT must fail.
    const LIMIT_BYTES: usize = 1024 * 1024;
    const PAYLOAD_BYTES: usize = 2 * 1024 * 1024;
    const TABLE: &str = "grpc_limit_test";

    // Step 1: schema with a permissive client.
    let setup_client = test_client_builder().client()?;
    setup_client.wait().await?;
    let setup_table_client = setup_client.table_client();
    let _ = setup_table_client
        .create_session()
        .await?
        .execute_schema_query(format!("DROP TABLE {TABLE}"))
        .await;
    setup_table_client
        .create_session()
        .await?
        .execute_schema_query(format!(
            "CREATE TABLE {TABLE} (id Int64, val Bytes, PRIMARY KEY (id))"
        ))
        .await?;

    // Step 2: pre-seed an oversized row using the permissive client so the decode test
    // has something to read back.
    let payload = vec![0xABu8; PAYLOAD_BYTES];
    setup_table_client
        .retry_transaction(|mut tr| {
            let payload = payload.clone();
            async move {
                tr.query(
                    Query::new(format!("UPSERT INTO {TABLE} (id, val) VALUES ($id, $val)"))
                        .with_params(ydb_params!(
                            "$id" => Value::Int64(1),
                            "$val" => Value::Bytes(Bytes::from(payload))
                        )),
                )
                .await?;
                tr.commit().await?;
                Ok(())
            }
        })
        .await?;

    // Step 3: small-limit client. Discovery passes because endpoint list fits in 1 MiB.
    let limited_client = test_client_builder()
        .with_grpc_max_message_size(LIMIT_BYTES)
        .client()?;
    limited_client.wait().await?;
    let limited = limited_client.table_client();

    // Step 3a: ENCODE path — UPSERT with a 2 MiB blob. tonic 0.14 emits the encode-limit
    // failure as a body-stream error which is observed by the client as an HTTP/2 stream
    // reset (Reason::INTERNAL_ERROR initiated by us), not as a proper Status::OutOfRange.
    // So the error surfaces as YdbError::TransportGRPCStatus with Code::Unknown.
    let mut tx = limited.create_interactive_transaction();
    let encode_err = tx
        .query(
            Query::new(format!("UPSERT INTO {TABLE} (id, val) VALUES ($id, $val)")).with_params(
                ydb_params!(
                    "$id" => Value::Int64(2),
                    "$val" => Value::Bytes(Bytes::from(vec![0xCDu8; PAYLOAD_BYTES]))
                ),
            ),
        )
        .await
        .expect_err("upsert exceeding grpc encoding limit must fail");
    trace!("encode-limit error: {:?}", encode_err);
    match &encode_err {
        YdbError::TransportGRPCStatus(status) => {
            // tonic 0.14: encode-limit becomes a transport-level RST_STREAM(INTERNAL_ERROR).
            assert_eq!(
                status.code(),
                Code::Unknown,
                "expected Unknown (transport reset from local encode limit), got {status:?}"
            );
            assert!(
                status.message().contains("INTERNAL_ERROR"),
                "expected message mentioning the stream reset, got: {}",
                status.message()
            );
        }
        other => panic!("expected TransportGRPCStatus, got {other:?}"),
    }

    // Step 3b: DECODE path — SELECT the pre-seeded 2 MiB blob. tonic surfaces the decode
    // limit as a clean Status::OutOfRange.
    let mut tx = limited.create_autocommit_transaction(Mode::OnlineReadonly);
    let decode_err = tx
        .query(Query::new(format!("SELECT val FROM {TABLE} WHERE id = 1")))
        .await
        .expect_err("select exceeding grpc decoding limit must fail");
    trace!("decode-limit error: {:?}", decode_err);
    match &decode_err {
        YdbError::TransportGRPCStatus(status) => {
            assert_eq!(
                status.code(),
                Code::OutOfRange,
                "expected OutOfRange (tonic decode-size limit), got {status:?}"
            );
        }
        other => panic!("expected TransportGRPCStatus(OutOfRange), got {other:?}"),
    }

    let _ = setup_table_client
        .create_session()
        .await?
        .execute_schema_query(format!("DROP TABLE {TABLE}"))
        .await;

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn bulk_upsert_rpc() -> YdbResult<()> {
    let client = create_client().await?;
    let table_client = client.table_client();
    let rand_str = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
    let table_name = format!("bulk_rpc_{rand_str}");
    let table_path = format!("/local/{table_name}");

    table_client
        .retry_create_table(
            CreateTableRequest::new(table_path.clone())
                .with_column(TableColumn::new("id", Value::Int64(0)))
                .with_column(TableColumn::new("val", Value::Text(String::new())))
                .with_primary_key(["id"]),
        )
        .await?;

    table_client
        .retry_bulk_upsert(
            table_path.clone(),
            vec![
                ydb_struct!("id" => 1_i64, "val" => Value::Text("one".into())),
                ydb_struct!("id" => 2_i64, "val" => Value::Text("two".into())),
            ],
        )
        .await?;

    let result = table_client
        .retry_read_rows_request(
            ReadRowsRequest::new(table_path.clone())
                .with_keys(vec![ydb_struct!("id" => 1_i64), ydb_struct!("id" => 2_i64)]),
        )
        .await?;

    let mut vals = Vec::new();
    for mut row in result.rows() {
        vals.push(row.remove_field_by_name("val")?);
    }
    assert!(vals.contains(&Value::Text("one".into())));
    assert!(vals.contains(&Value::Text("two".into())));

    table_client
        .retry_drop_table(DropTableRequest::new(table_path))
        .await?;

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn read_rows_on_session_rpc() -> YdbResult<()> {
    let client = create_client().await?;
    let table_client = client.table_client();
    let rand_str = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
    let table_name = format!("session_read_{rand_str}");
    let table_path = format!("/local/{table_name}");

    table_client
        .retry_create_table(
            CreateTableRequest::new(table_path.clone())
                .with_column(TableColumn::new("id", Value::Int64(0)))
                .with_column(TableColumn::new("val", Value::Int64(0)))
                .with_primary_key(["id"]),
        )
        .await?;

    table_client
        .retry_bulk_upsert(
            table_path.clone(),
            vec![ydb_struct!("id" => 42_i64, "val" => 7_i64)],
        )
        .await?;

    let mut session = table_client.create_session().await?;
    let result = session
        .read_rows(
            ReadRowsRequest::new(table_path.clone())
                .with_keys(vec![ydb_struct!("id" => 42_i64)])
                .with_column("val"),
            false,
        )
        .await?;

    let mut row = result.rows().next().unwrap();
    assert_eq!(row.remove_field_by_name("val")?, Value::Int64(7));

    table_client
        .retry_drop_table(DropTableRequest::new(table_path))
        .await?;

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn create_drop_table_rpc() -> YdbResult<()> {
    let client = create_client().await?;
    let table_client = client.table_client();
    let rand_str = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
    let table_name = format!("rpc_table_{rand_str}");
    let database_path = client.database();
    let table_path = format!("{database_path}/{table_name}");

    let request = CreateTableRequest::new(table_path.clone())
        .with_column(TableColumn::new("id", Value::Int64(0)))
        .with_column(TableColumn::new("val", Value::Text(String::new())))
        .with_primary_key(["id"]);
    table_client.retry_create_table(request).await?;

    let desc = table_client.describe_table(table_path.clone()).await?;
    assert_eq!(desc.columns.len(), 2);
    assert_eq!(desc.primary_key, vec!["id"]);

    table_client
        .retry_drop_table(DropTableRequest::new(table_path))
        .await?;

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn stream_read_table_rpc() -> YdbResult<()> {
    let client = create_client().await?;
    let table_client = client.table_client();
    let rand_str = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
    let table_name = format!("stream_read_{rand_str}");
    let table_path = format!("/local/{table_name}");

    table_client
        .retry_execute_scheme_query(format!(
            "CREATE TABLE {table_name} (id Int64 NOT NULL, val Int64, PRIMARY KEY (id))"
        ))
        .await?;

    table_client
        .retry_bulk_upsert(
            table_path.clone(),
            vec![
                ydb_struct!("id" => 1_i64, "val" => 10_i64),
                ydb_struct!("id" => 2_i64, "val" => 20_i64),
            ],
        )
        .await?;

    let mut stream = table_client
        .retry_stream_read_table(table_path, ReadTableOptions::default())
        .await?;
    let mut row_count = 0;
    while let Some(result_set) = stream.next_result_set().await? {
        row_count += result_set.rows().count();
    }
    assert_eq!(row_count, 2);

    table_client
        .retry_execute_scheme_query(format!("DROP TABLE {table_name}"))
        .await?;

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn prepare_data_query_rpc() -> YdbResult<()> {
    let client = create_client().await?;
    let table_client = client.table_client();

    let result = table_client
        .retry_execute_prepared_query(
            "SELECT $v + $v AS res",
            Query::new("").with_params(ydb_params!("$v" => 21_i32)),
            Mode::OnlineReadonly,
        )
        .await?;

    assert_eq!(
        Value::Int32(42),
        result
            .into_only_result()?
            .rows()
            .next()
            .unwrap()
            .remove_field_by_name("res")?
    );

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn describe_table_options_rpc() -> YdbResult<()> {
    let client = create_client().await?;
    let options = client.table_client().retry_describe_table_options().await?;
    trace!("describe_table_options: {:?}", options);
    // Presets may be empty on minimal clusters; the RPC itself must succeed.
    let _ = options.table_profile_presets;
    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn alter_table_rpc() -> YdbResult<()> {
    let client = create_client().await?;
    let table_client = client.table_client();
    let rand_str = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
    let table_name = format!("alter_rpc_{rand_str}");
    let database_path = client.database();
    let table_path = format!("{database_path}/{table_name}");

    table_client
        .retry_create_table(
            CreateTableRequest::new(table_path.clone())
                .with_column(TableColumn::new("id", Value::Int64(0)))
                .with_column(TableColumn::new("val", Value::Int64(0)))
                .with_primary_key(["id"]),
        )
        .await?;

    table_client
        .retry_alter_table(
            AlterTableRequest::new(table_path.clone())
                .add_column(TableColumn::new("extra", Value::Text(String::new()))),
        )
        .await?;

    let desc = table_client.describe_table(table_path.clone()).await?;
    assert_eq!(desc.columns.len(), 3);
    assert!(desc.columns.iter().any(|c| c.name == "extra"));

    table_client
        .retry_alter_table(AlterTableRequest::new(table_path.clone()).drop_column("val"))
        .await?;

    let desc = table_client.describe_table(table_path.clone()).await?;
    assert_eq!(desc.columns.len(), 2);
    assert!(!desc.columns.iter().any(|c| c.name == "val"));

    table_client
        .retry_drop_table(DropTableRequest::new(table_path))
        .await?;

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn table_attributes_rpc() -> YdbResult<()> {
    let client = create_client().await?;
    let table_client = client.table_client();
    let rand_str = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
    let table_name = format!("attrs_rpc_{rand_str}");
    let database_path = client.database();
    let table_path = format!("{database_path}/{table_name}");

    table_client
        .retry_create_table(
            CreateTableRequest::new(table_path.clone())
                .with_column(TableColumn::new("id", Value::Int64(0)))
                .with_primary_key(["id"])
                .with_attribute("owner", "integration-test"),
        )
        .await?;

    let desc = table_client.describe_table(table_path.clone()).await?;
    assert_eq!(
        desc.attributes.get("owner").map(String::as_str),
        Some("integration-test")
    );

    table_client
        .retry_alter_table(
            AlterTableRequest::new(table_path.clone()).alter_attribute("owner", "updated"),
        )
        .await?;

    let desc = table_client.describe_table(table_path.clone()).await?;
    assert_eq!(
        desc.attributes.get("owner").map(String::as_str),
        Some("updated")
    );

    table_client
        .retry_drop_table(DropTableRequest::new(table_path))
        .await?;

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn prepare_data_query_on_session_rpc() -> YdbResult<()> {
    let client = create_client().await?;
    let table_client = client.table_client();

    let prepared = table_client
        .retry_prepare_data_query("SELECT $v * 2 AS res")
        .await?;
    assert!(!prepared.query_id().is_empty());
    assert_eq!(prepared.text(), "SELECT $v * 2 AS res");

    let result = table_client
        .retry_with_session(RetryOptions::new(), |session| async move {
            let mut session = session;
            let prepared = session
                .prepare_data_query("SELECT $v * 2 AS res".to_string())
                .await?;
            let result = session
                .execute_prepared_query(
                    &prepared,
                    Query::new("").with_params(ydb_params!("$v" => 11_i32)),
                    Mode::OnlineReadonly,
                )
                .await?;
            Ok(result)
        })
        .await?;

    assert_eq!(
        Value::Int32(22),
        result
            .into_only_result()?
            .rows()
            .next()
            .unwrap()
            .remove_field_by_name("res")?
    );

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn stream_read_table_options_rpc() -> YdbResult<()> {
    let client = create_client().await?;
    let table_client = client.table_client();
    let rand_str = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
    let table_name = format!("stream_opts_{rand_str}");
    let table_path = format!("/local/{table_name}");

    table_client
        .retry_create_table(
            CreateTableRequest::new(table_path.clone())
                .with_column(TableColumn::new("id", Value::Int64(0)))
                .with_column(TableColumn::new("val", Value::Int64(0)))
                .with_primary_key(["id"]),
        )
        .await?;

    let rows: Vec<Value> = (1_i64..=5)
        .map(|id| ydb_struct!("id" => id, "val" => id * 10))
        .collect();
    table_client
        .retry_bulk_upsert(table_path.clone(), rows)
        .await?;

    // Column projection
    let mut stream = table_client
        .retry_stream_read_table(
            table_path.clone(),
            ReadTableOptions::new()
                .with_column("id")
                .with_ordered(true),
        )
        .await?;
    let mut ids = Vec::new();
    while let Some(result_set) = stream.next_result_set().await? {
        for mut row in result_set.rows() {
            match row.remove_field_by_name("id")? {
                Value::Int64(id) => ids.push(id),
                other => panic!("unexpected id type: {other:?}"),
            }
            assert!(row.remove_field_by_name("val").is_err());
        }
    }
    assert_eq!(ids, vec![1_i64, 2, 3, 4, 5]);

    // Key range [2, 4]
    let key_range = ReadTableKeyRange::new()
        .with_from(ReadTableKeyBound::GreaterOrEqual(Value::Int64(2)))
        .with_to(ReadTableKeyBound::LessOrEqual(Value::Int64(4)));
    let mut stream = table_client
        .retry_stream_read_table(
            table_path.clone(),
            ReadTableOptions::new()
                .with_key_range(key_range)
                .with_ordered(true),
        )
        .await?;
    let mut ranged_ids = Vec::new();
    while let Some(result_set) = stream.next_result_set().await? {
        for mut row in result_set.rows() {
            match row.remove_field_by_name("id")? {
                Value::Int64(id) => ranged_ids.push(id),
                other => panic!("unexpected id type: {other:?}"),
            }
        }
    }
    assert_eq!(ranged_ids, vec![2_i64, 3, 4]);

    // Row limit with truncated flag (no error by default)
    let mut stream = table_client
        .retry_stream_read_table(
            table_path.clone(),
            ReadTableOptions::new().with_row_limit(2).with_ordered(true),
        )
        .await?;
    let mut limited_count = 0;
    let mut saw_truncated = false;
    while let Some(result_set) = stream.next_result_set().await? {
        if result_set.is_truncated() {
            saw_truncated = true;
        }
        limited_count += result_set.rows().count();
    }
    assert!(saw_truncated);
    assert_eq!(limited_count, 5);

    table_client
        .retry_drop_table(DropTableRequest::new(table_path))
        .await?;

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn truncated_result_on_data_query_rpc() -> YdbResult<()> {
    const ROWS: i64 = 1001;

    let client = create_client().await?;
    let table_client = client.table_client();
    let rand_str = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
    let table_name = format!("truncate_{rand_str}");
    let table_path = format!("/local/{table_name}");
    let select_query = Arc::new(format!("SELECT * FROM {table_name}"));

    table_client
        .retry_create_table(
            CreateTableRequest::new(table_path.clone())
                .with_column(TableColumn::new("id", Value::Int64(0)))
                .with_column(TableColumn::new("val", Value::Text(String::new())))
                .with_primary_key(["id"]),
        )
        .await?;

    let rows: Vec<Value> = (0..ROWS)
        .map(|id| {
            ydb_struct!(
                "id" => id,
                "val" => Value::Text(id.to_string()),
            )
        })
        .collect();
    table_client
        .retry_bulk_upsert(table_path.clone(), rows)
        .await?;

    let truncate_err = table_client
        .retry_transaction(|mut t| {
            let select_query = Arc::clone(&select_query);
            async move {
                Ok(t.query(Query::new((*select_query).clone())).await?)
            }
        })
        .await;
    match truncate_err {
        Err(YdbOrCustomerError::YDB(YdbError::TruncatedResult { .. })) => {}
        other => panic!("expected TruncatedResult, got {other:?}"),
    }

    let result = table_client
        .clone()
        .with_ignore_truncated(true)
        .retry_transaction(|mut t| {
            let select_query = Arc::clone(&select_query);
            async move {
                Ok(t.query(Query::new((*select_query).clone())).await?)
            }
        })
        .await?;
    let result_set = result.into_only_result()?;
    assert!(result_set.is_truncated());
    assert_eq!(result_set.rows().count(), 1000);

    table_client
        .retry_drop_table(DropTableRequest::new(table_path))
        .await?;

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn truncated_result_on_read_rows_rpc() -> YdbResult<()> {
    let client = create_client().await?;
    let table_client = client.table_client();
    let rand_str = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
    let table_name = format!("read_rows_trunc_{rand_str}");
    let table_path = format!("/local/{table_name}");

    table_client
        .retry_create_table(
            CreateTableRequest::new(table_path.clone())
                .with_column(TableColumn::new("id", Value::Int64(0)))
                .with_column(TableColumn::new("val", Value::Int64(0)))
                .with_primary_key(["id"]),
        )
        .await?;

    let rows: Vec<Value> = (0..1001)
        .map(|id| ydb_struct!("id" => id, "val" => id))
        .collect();
    table_client
        .retry_bulk_upsert(table_path.clone(), rows)
        .await?;

    let keys: Vec<Value> = (0..1001)
        .map(|id| ydb_struct!("id" => id))
        .collect();

    let err = table_client
        .retry_read_rows(table_path.clone(), keys.clone(), None)
        .await;
    assert!(matches!(err, Err(YdbError::TruncatedResult { .. })));

    let result_set = table_client
        .clone()
        .with_ignore_truncated(true)
        .retry_read_rows(table_path.clone(), keys, None)
        .await?;
    assert!(result_set.is_truncated());
    assert_eq!(result_set.rows().count(), 1000);

    table_client
        .retry_drop_table(DropTableRequest::new(table_path))
        .await?;

    Ok(())
}
