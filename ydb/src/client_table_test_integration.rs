use itertools::Itertools;
use std::time;
use std::time::UNIX_EPOCH;

use rand::distributions::{Alphanumeric, DistString};
use tracing::trace;
use tracing_test::traced_test;

use crate::errors::{YdbError, YdbResult};
use crate::table_requests::{
    AlterTableRequest, CreateTableRequest, DropTableRequest, ReadRowsRequest, TableColumn,
};
use crate::table_service_types::{CopyTableItem, IndexType, StoreType};
use crate::test_integration_helper::create_client;
use crate::types::Value;
use crate::ydb_struct;

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
        .execute_scheme_query(format!(
            "CREATE TABLE {table_name} (id Int64, vInt64 Int64, PRIMARY KEY (id))"
        ))
        .await?;

    table_client
        .bulk_upsert(
            format!("/local/{table_name}"),
            vec![ydb_struct!("id" => 1_i64, "vInt64" => 2_i64)],
        )
        .await?;

    let database_path = client.database();
    table_client
        .copy_table(
            format!("{database_path}/{table_name}"),
            format!("{database_path}/{copy_table_name}"),
        )
        .await
        .unwrap();

    let res = table_client
        .read_rows(
            format!("{database_path}/{copy_table_name}"),
            vec![ydb_struct!("id" => 1_i64)],
            Some(vec!["vInt64".into()]),
        )
        .await?;

    let field = res
        .rows()
        .next()
        .unwrap()
        .remove_field_by_name("vInt64")?;
    let v_int64 = match field {
        Value::Int64(v) => v,
        Value::Optional(opt) => match opt.value {
            Some(Value::Int64(v)) => v,
            Some(other) => {
                return Err(YdbError::custom(format!(
                    "expected Int64 inside Optional, got {other:?}"
                )));
            }
            None => return Err(YdbError::custom("vInt64 is NULL")),
        },
        other => {
            return Err(YdbError::custom(format!(
                "expected Int64 or Optional<Int64>, got {other:?}"
            )));
        }
    };
    assert_eq!(2, v_int64);

    for &target in [&table_name, &copy_table_name].iter() {
        table_client
            .execute_scheme_query(format!("DROP TABLE {target}"))
            .await?;
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
        .execute_scheme_query(format!(
            "CREATE TABLE {table_name} (id Int64, vInt64 Int64, PRIMARY KEY (id))"
        ))
        .await?;

    table_client
        .bulk_upsert(
            format!("/local/{table_name}"),
            vec![ydb_struct!("id" => 1_i64, "vInt64" => 2_i64)],
        )
        .await?;

    let database_path = client.database();
    table_client
        .copy_tables(vec![CopyTableItem::new(
            format!("{database_path}/{table_name}"),
            format!("{database_path}/{copy_table_name}"),
            true,
        )])
        .await
        .unwrap();

    let res = table_client
        .read_rows(
            format!("{database_path}/{copy_table_name}"),
            vec![ydb_struct!("id" => 1_i64)],
            Some(vec!["vInt64".into()]),
        )
        .await?;

    let field = res
        .rows()
        .next()
        .unwrap()
        .remove_field_by_name("vInt64")?;
    let v_int64 = match field {
        Value::Int64(v) => v,
        Value::Optional(opt) => match opt.value {
            Some(Value::Int64(v)) => v,
            Some(other) => {
                return Err(YdbError::custom(format!(
                    "expected Int64 inside Optional, got {other:?}"
                )));
            }
            None => return Err(YdbError::custom("vInt64 is NULL")),
        },
        other => {
            return Err(YdbError::custom(format!(
                "expected Int64 or Optional<Int64>, got {other:?}"
            )));
        }
    };
    assert_eq!(2, v_int64);

    for &target in [&table_name, &copy_table_name].iter() {
        table_client
            .execute_scheme_query(format!("DROP TABLE {target}"))
            .await?;
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
        .execute_scheme_query(format!(
            "CREATE TABLE {table_name} (id String, PRIMARY KEY (id))"
        ))
        .await?;

    table_client
        .execute_scheme_query(format!("DROP TABLE {table_name}"))
        .await?;

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
        .execute_scheme_query(format!(
            "CREATE TABLE {TABLE_NAME} (id Int64 NOT NULL, first Int64 NOT NULL, second Int64 NOT NULL, PRIMARY KEY (id))"
        ))
        .await?;

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

    table_client.bulk_upsert(TABLE_PATH, rows).await?;

    // Empty
    let empty = table_client.read_rows(TABLE_PATH, vec![], None).await;
    assert_eq!(empty.unwrap().rows().count(), 0);

    // Non-list keys
    let non_structs = table_client
        .read_rows(TABLE_PATH, vec![Value::Int64(1i64)], None)
        .await;
    assert!(non_structs.is_err());

    let vec_to_values = |ids: Vec<i64>| {
        ids.into_iter()
            .map(|id| ydb_struct!("id" => id))
            .collect_vec()
    };

    // Basic all columns
    let all_columns = table_client
        .read_rows(TABLE_PATH, vec_to_values((0i64..4i64).collect_vec()), None)
        .await;

    for (mut row, (first, second)) in all_columns.unwrap().rows().zip(ydb_values.iter()) {
        assert_eq!(&row.remove_field_by_name("first").unwrap(), first);
        assert_eq!(&row.remove_field_by_name("second").unwrap(), second);
    }

    // Basic reversed
    let all_columns_rev = table_client
        .read_rows(
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
        .read_rows(
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
        .read_rows(
            TABLE_PATH,
            vec_to_values(vec![1i64]),
            Some(vec!["first".into(), "unknown".into()]),
        )
        .await;
    assert!(unknown.is_err());

    // Clear table
    table_client
        .execute_scheme_query(format!("DROP TABLE {TABLE_NAME}"))
        .await?;

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
        .execute_scheme_query(format!(
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
        .bulk_upsert(format!("/local/{table_name}"), rows)
        .await?;

    let result_set = client
        .query_client()
        .query_result_set(format!("SELECT * FROM {table_name} ORDER BY id"))
        .await?;

    let read_rows_id: YdbResult<Vec<i64>> = result_set
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
        .execute_scheme_query(format!("DROP TABLE {table_name}"))
        .await?;

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
        .execute_scheme_query(format!("DROP TABLE IF EXISTS {table_name}"))
        .await?;
    table_client
        .execute_scheme_query(format!(
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
        .execute_scheme_query(format!("DROP TABLE {table_name}"))
        .await?;

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
        .create_table(
            CreateTableRequest::new(table_path.clone())
                .with_column(TableColumn::new("id", Value::Int64(0)))
                .with_column(TableColumn::new("val", Value::Text(String::new())))
                .with_primary_key(["id"]),
        )
        .await?;

    table_client
        .bulk_upsert(
            table_path.clone(),
            vec![
                ydb_struct!("id" => 1_i64, "val" => Value::Text("one".into())),
                ydb_struct!("id" => 2_i64, "val" => Value::Text("two".into())),
            ],
        )
        .await?;

    let result = table_client
        .read_rows_request(
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
        .drop_table(DropTableRequest::new(table_path))
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
    table_client.create_table(request).await?;

    let desc = table_client.describe_table(table_path.clone()).await?;
    assert_eq!(desc.columns.len(), 2);
    assert_eq!(desc.primary_key, vec!["id"]);

    table_client
        .drop_table(DropTableRequest::new(table_path))
        .await?;

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn describe_table_options_rpc() -> YdbResult<()> {
    let client = create_client().await?;
    let options = client.table_client().describe_table_options().await?;
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
        .create_table(
            CreateTableRequest::new(table_path.clone())
                .with_column(TableColumn::new("id", Value::Int64(0)))
                .with_column(TableColumn::new("val", Value::Int64(0)))
                .with_primary_key(["id"]),
        )
        .await?;

    table_client
        .alter_table(
            AlterTableRequest::new(table_path.clone()).add_column(
                TableColumn::new("extra", Value::optional_from(Value::Int64(0), None)?)
                    .with_not_null(false),
            ),
        )
        .await?;

    let desc = table_client.describe_table(table_path.clone()).await?;
    assert_eq!(desc.columns.len(), 3);
    assert!(desc.columns.iter().any(|c| c.name == "extra"));

    table_client
        .alter_table(AlterTableRequest::new(table_path.clone()).drop_column("val"))
        .await?;

    let desc = table_client.describe_table(table_path.clone()).await?;
    assert_eq!(desc.columns.len(), 2);
    assert!(!desc.columns.iter().any(|c| c.name == "val"));

    table_client
        .drop_table(DropTableRequest::new(table_path))
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
        .create_table(
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
        .alter_table(AlterTableRequest::new(table_path.clone()).alter_attribute("owner", "updated"))
        .await?;

    let desc = table_client.describe_table(table_path.clone()).await?;
    assert_eq!(
        desc.attributes.get("owner").map(String::as_str),
        Some("updated")
    );

    table_client
        .drop_table(DropTableRequest::new(table_path))
        .await?;

    Ok(())
}
