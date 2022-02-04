use crate::errors::{YdbError, YdbOrCustomerError, YdbResult};
use crate::internal::client_fabric::ClientFabric;
use crate::internal::client_table::{RetryOptions, TransactionOptions};
use crate::internal::discovery::StaticDiscovery;
use crate::internal::query::Query;
use crate::internal::test_helpers::CONNECTION_INFO;
use crate::internal::transaction::Mode;
use crate::internal::transaction::Mode::SerializableReadWrite;
use crate::internal::transaction::Transaction;
use crate::types::{YdbList, YdbStruct, YdbValue};
use http::Uri;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time;
use std::time::{Duration, UNIX_EPOCH};
use tonic::{Code, Status};
use ydb_protobuf::generated::ydb::discovery::{ListEndpointsRequest, WhoAmIRequest};

async fn create_client() -> YdbResult<ClientFabric> {
    let _endpoint_uri = Uri::from_str(CONNECTION_INFO.discovery_endpoint.as_str())?;
    let discovery = StaticDiscovery::from_str(CONNECTION_INFO.discovery_endpoint.as_str())?;

    let client = ClientFabric::new(
        CONNECTION_INFO.credentials.clone(),
        CONNECTION_INFO.database.clone(),
        Box::new(discovery),
    )?;
    client.wait().await?;
    return Ok(client);
}

#[tokio::test]
async fn create_session() -> YdbResult<()> {
    let res = create_client()
        .await?
        .table_client()
        .create_session()
        .await?;
    println!("session: {:?}", res);
    Ok(())
}

#[tokio::test]
async fn endpoints() -> YdbResult<()> {
    let _res = create_client()
        .await?
        .endpoints(ListEndpointsRequest::default())
        .await?;
    println!("{:?}", _res);
    Ok(())
}

#[tokio::test]
async fn execute_data_query() -> YdbResult<()> {
    let client = create_client().await?;
    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let res = transaction.query("SELECT 1+1".into()).await?;
    println!("result: {:?}", &res);
    assert_eq!(
        YdbValue::Int32(2),
        res.first()
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
async fn execute_data_query_field_name() -> YdbResult<()> {
    let client = create_client().await?;
    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let res = transaction.query("SELECT 1+1 as s".into()).await?;
    println!("result: {:?}", &res);
    assert_eq!(
        YdbValue::Int32(2),
        res.first()
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
async fn execute_data_query_params() -> YdbResult<()> {
    let client = create_client().await?;
    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let mut params = HashMap::new();
    params.insert("$v".to_string(), YdbValue::Int32(3));
    let res = transaction
        .query(
            Query::new()
                .with_query(
                    "
                DECLARE $v AS Int32;
                SELECT $v+$v
        "
                    .into(),
                )
                .with_params(params),
        )
        .await?;
    println!("result: {:?}", res);
    assert_eq!(
        YdbValue::Int32(6),
        res.first()
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
async fn interactive_transaction() -> YdbResult<()> {
    let client = create_client().await?;

    let _ = client
        .table_client()
        .create_session()
        .await?
        .execute_schema_query(
            "CREATE TABLE test_values (id Int64, vInt64 Int64, PRIMARY KEY (id))".to_string(),
        )
        .await?;

    let mut tx_auto = client
        .table_client()
        .create_autocommit_transaction(SerializableReadWrite);

    let mut tx = client.table_client().create_interactive_transaction();
    tx.query(Query::new().with_query("DELETE FROM test_values".into()))
        .await?;
    tx.commit().await?;

    let mut tx = client.table_client().create_interactive_transaction();
    tx.query(Query::new().with_query("UPSERT INTO test_values (id, vInt64) VALUES (1, 2)".into()))
        .await?;
    tx.query(
        Query::new()
            .with_query(
                "
                DECLARE $key AS Int64;
                DECLARE $val AS Int64;

                UPSERT INTO test_values (id, vInt64) VALUES ($key, $val)
            "
                .into(),
            )
            .with_params(HashMap::from([
                ("$key".into(), YdbValue::Int64(2)),
                ("$val".into(), YdbValue::Int64(3)),
            ])),
    )
    .await?;

    // check table before commit
    let auto_res = tx_auto
        .query(Query::new().with_query("SELECT vInt64 FROM test_values WHERE id=1".into()))
        .await?;
    assert!(auto_res.first().unwrap().rows().next().is_none());

    tx.commit().await?;

    // check table after commit
    let auto_res = tx_auto
        .query(Query::new().with_query("SELECT vInt64 FROM test_values WHERE id=1".into()))
        .await?;
    assert_eq!(
        YdbValue::optional_from(YdbValue::Int64(0), Some(YdbValue::Int64(2)))?,
        auto_res
            .first()
            .unwrap()
            .rows()
            .next()
            .unwrap()
            .remove_field_by_name("vInt64")
            .unwrap()
    );

    return Ok(());
}

#[tokio::test]
async fn retry_test() -> YdbResult<()> {
    let client = create_client().await?;

    let attempt = Arc::new(Mutex::new(0));
    let opts = RetryOptions::new().with_timeout(Duration::from_secs(15));
    let res = client
        .table_client()
        .retry_transaction(TransactionOptions::new(), opts, |t| async {
            let mut t = t; // force borrow for lifetime of t inside closure
            let mut locked_res = attempt.lock().unwrap();
            *locked_res += 1;

            let res = t
                .query(Query::new().with_query("SELECT 1+1 as res".into()))
                .await?;
            let res = res
                .first()
                .unwrap()
                .rows()
                .next()
                .unwrap()
                .remove_field_by_name("res")
                .unwrap();

            assert_eq!(YdbValue::Int32(2), res);

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

    return Ok(());
}

#[tokio::test]
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

            return Ok(());
        })
        .await
        .unwrap();

    table_client
        .retry_with_session(RetryOptions::new(), |session| async {
            let mut session = session; // force borrow for lifetime of t inside closure
            session
                .execute_schema_query(format!("DROP TABLE {}", table_name))
                .await?;

            return Ok(());
        })
        .await
        .unwrap();

    return Ok(());
}

#[tokio::test]
async fn select_int() -> YdbResult<()> {
    let client = create_client().await?;
    let v = YdbValue::Int32(123);

    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let res = transaction
        .query(
            Query::new()
                .with_query(
                    "
DECLARE $test AS Int32;

SELECT $test AS test;
"
                    .into(),
                )
                .with_params(HashMap::from_iter([("$test".into(), v.clone())])),
        )
        .await?;

    let res = res.results.into_iter().next().unwrap();
    assert_eq!(1, res.columns().len());
    assert_eq!(v, res.rows().next().unwrap().remove_field_by_name("test")?);

    return Ok(());
}

#[tokio::test]
async fn select_optional() -> YdbResult<()> {
    let client = create_client().await?;
    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let res = transaction
        .query(
            Query::new()
                .with_query(
                    "
DECLARE $test AS Optional<Int32>;

SELECT $test AS test;
"
                    .into(),
                )
                .with_params(HashMap::from_iter([(
                    "$test".into(),
                    YdbValue::optional_from(YdbValue::Int32(0), Some(YdbValue::Int32(3)))?,
                )])),
        )
        .await?;

    let res = res.results.into_iter().next().unwrap();
    assert_eq!(1, res.columns().len());
    assert_eq!(
        YdbValue::optional_from(YdbValue::Int32(0), Some(YdbValue::Int32(3)))?,
        res.rows().next().unwrap().remove_field_by_name("test")?
    );

    return Ok(());
}

#[tokio::test]
async fn select_list() -> YdbResult<()> {
    let client = create_client().await?;
    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let res = transaction
        .query(
            Query::new()
                .with_query(
                    "
DECLARE $l AS List<Int32>;

SELECT $l AS l;
"
                    .into(),
                )
                .with_params(HashMap::from_iter([(
                    "$l".into(),
                    YdbValue::List(Box::new(YdbList {
                        t: YdbValue::Int32(0),
                        values: Vec::from([
                            YdbValue::Int32(1),
                            YdbValue::Int32(2),
                            YdbValue::Int32(3),
                        ]),
                    })),
                )])),
        )
        .await?;
    println!("{:?}", res);
    let res = res.results.into_iter().next().unwrap();
    assert_eq!(1, res.columns().len());
    assert_eq!(
        YdbValue::list_from(
            YdbValue::Int32(0),
            vec![YdbValue::Int32(1), YdbValue::Int32(2), YdbValue::Int32(3)]
        )?,
        res.rows().next().unwrap().remove_field_by_name("l")?
    );
    Ok(())
}

#[tokio::test]
async fn select_struct() -> YdbResult<()> {
    let client = create_client().await?;
    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let res = transaction
        .query(
            Query::new()
                .with_query(
                    "
DECLARE $l AS List<Struct<
    a: Int64
>>;

SELECT
    SUM(a) AS s
FROM
    AS_TABLE($l);
;
"
                    .into(),
                )
                .with_params(HashMap::from_iter([(
                    "$l".into(),
                    YdbValue::List(Box::new(YdbList {
                        t: YdbValue::Struct(YdbStruct::from_names_and_values(
                            vec!["a".into()],
                            vec![YdbValue::Int64(0)],
                        )?),
                        values: vec![
                            YdbValue::Struct(YdbStruct::from_names_and_values(
                                vec!["a".into()],
                                vec![YdbValue::Int64(1)],
                            )?),
                            YdbValue::Struct(YdbStruct::from_names_and_values(
                                vec!["a".into()],
                                vec![YdbValue::Int64(2)],
                            )?),
                            YdbValue::Struct(YdbStruct::from_names_and_values(
                                vec!["a".into()],
                                vec![YdbValue::Int64(3)],
                            )?),
                        ],
                    })),
                )])),
        )
        .await?;
    println!("{:?}", res);
    let res = res.results.into_iter().next().unwrap();
    assert_eq!(1, res.columns().len());

    assert_eq!(
        YdbValue::optional_from(YdbValue::Int64(0), Some(YdbValue::Int64(6)))?,
        res.rows().next().unwrap().remove_field_by_name("s")?
    );
    Ok(())
}

#[tokio::test]
async fn select_int64_null4() -> YdbResult<()> {
    let client = create_client().await?;
    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let res = transaction
        .query(
            Query::new().with_query(
                "
SELECT CAST(NULL AS Optional<Int64>)
;
"
                .into(),
            ),
        )
        .await?;
    println!("{:?}", res);
    let res = res.results.into_iter().next().unwrap();
    assert_eq!(1, res.columns().len());

    assert_eq!(
        YdbValue::optional_from(YdbValue::Int64(0), None)?,
        res.rows().next().unwrap().remove_field(0)?
    );
    Ok(())
}

#[tokio::test]
async fn select_void_null() -> YdbResult<()> {
    let client = create_client().await?;
    let mut transaction = client
        .table_client()
        .create_autocommit_transaction(Mode::OnlineReadonly);
    let res = transaction
        .query(
            Query::new().with_query(
                "
SELECT NULL
;
"
                .into(),
            ),
        )
        .await?;
    println!("{:?}", res);
    let res = res.results.into_iter().next().unwrap();
    assert_eq!(1, res.columns().len());

    assert_eq!(
        YdbValue::optional_from(YdbValue::Void, None)?,
        res.rows().next().unwrap().remove_field(0)?
    );
    Ok(())
}

#[tokio::test]
async fn stream_query() -> YdbResult<()> {
    let mut client = create_client().await?.table_client();
    let mut session = client.create_session().await?;

    let _ = session
        .execute_schema_query("DROP TABLE stream_query".to_string())
        .await;

    session
        .execute_schema_query("CREATE TABLE stream_query (val Int32, PRIMARY KEY (val))".into())
        .await?;

    let generate_count = 20000;
    client
        .retry_transaction(TransactionOptions::new(), RetryOptions::new(), |tr| async {
            let mut tr = tr;

            let mut values = Vec::new();
            for i in 1..=generate_count {
                values.push(YdbValue::Struct(YdbStruct::from_names_and_values(
                    vec!["val".to_string()],
                    vec![YdbValue::Int32(i)],
                )?))
            }

            let query = Query::new()
                .with_query(
                    "
DECLARE $values AS List<Struct<
    val: Int32,
> >;

UPSERT INTO stream_query
SELECT
    val 
FROM
    AS_TABLE($values);            
"
                    .to_string(),
                )
                .with_params(
                    [(
                        "$values".to_string(),
                        YdbValue::list_from(values[0].clone(), values)?,
                    )]
                    .into_iter()
                    .collect(),
                );

            tr.query(query).await?;
            tr.commit().await?;
            return Ok(());
        })
        .await
        .unwrap();

    let query = Query::new().with_query("SELECT val FROM stream_query".to_string());
    let mut res = session.execute_scan_query(query).await?;
    let mut sum = 0;
    let mut result_set_count = 0;
    loop {
        let result_set = if let Some(result_set) = res.next().await? {
            result_set_count += 1;
            result_set
        } else {
            break;
        };

        for mut row in result_set.into_iter() {
            match row.remove_field(0)? {
                YdbValue::Optional(boxed_val) => match boxed_val.value.unwrap() {
                    YdbValue::Int32(val) => sum += val,
                    val => panic!("unexpected ydb boxed_value type: {:?}", val),
                },
                val => panic!("unexpected ydb valye type: {:?}", val),
            };
        }
    }

    let mut expected_sum = 0;
    for i in 1..=generate_count {
        expected_sum += i;
    }
    assert_eq!(expected_sum, sum);
    assert!(result_set_count > 1); // ensure get multiply results
    return Ok(());
}

// #[tokio::test]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn who_am_i() -> YdbResult<()> {
    let res = create_client()
        .await?
        .who_am_i(WhoAmIRequest::default())
        .await?;
    assert!(res.user.len() > 0);
    Ok(())
}
