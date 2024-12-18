use std::sync::Arc;
use slo::cli::SloTestsCli;
use slo::generator::{Generator, Row};
use ydb::{ydb_params, ydb_struct, Client, Query, TableClient, Value};

const UPSERT_TEMPLATE: &str = r#"
PRAGMA TablePathPrefix("{}");

DECLARE $id AS Uint64;
DECLARE $payload_str AS Utf8;
DECLARE $payload_double AS Double;
DECLARE $payload_timestamp AS Timestamp;

UPSERT INTO `{}` (
    id, hash, payload_str, payload_double, payload_timestamp
) VALUES (
    $id, Digest::NumericHash($id), $payload_str, $payload_double, $payload_timestamp
);
"#;

const SELECT_TEMPLATE: &str = r#"
PRAGMA TablePathPrefix("{}");

DECLARE $id AS Uint64;
SELECT id, payload_str, payload_double, payload_timestamp, payload_hash
FROM `{}` WHERE id = $id AND hash = Digest::NumericHash($id);
"#;

pub struct Storage {
    client: Client,
    cfg: Config,
    prefix: String,
    upsert_query: String,
    select_query: String,
    // retry_budget: Budget, // Нужно определить, как это будет работать в Rust
}

impl Storage {
    pub async fn new(
        // ctx: &tokio::task::Context<'_>,
        cli: &SloTestsCli,
        pool_size: usize,
    ) -> Result<Storage, Box<dyn std::error::Error>> {
        // let timeout_duration = Duration::from_secs(5 * 60); // 5 minutes
        // let ctx = ctx.clone();
        // let cancellation = ctx.deadline().clone();
        //
        // let retry_budget = budget::Limited::new((pool_size as f64 * 0.1) as usize);

        let client =
            ydb::ClientBuilder::new_from_connection_string(cli.endpoint)?
                .with_database(cli.db)
                .client()?;

        client.wait().await?;

        let prefix = format!("{}/{}", db.name(), label);

        let upsert_query = format!(UPSERT_TEMPLATE, prefix, cli.table_name);
        let select_query = format!(SELECT_TEMPLATE, prefix, cli.table_name);

        Ok(Storage {
            db: Arc::new(db),
            cfg,
            prefix,
            upsert_query,
            select_query,
        })
    }

    pub async fn read(
        ctx: &tokio::task::Context<'_>,
        storage: &Storage,
        entry_id: u64,
    ) -> Result<(Row, usize), StorageError> {
        let timeout_duration = Duration::from_millis(storage.cfg.read_timeout as u64);
        let ctx = ctx.clone();
        let cancellation = ctx.deadline().clone();

        let session = storage.db.table().create_session().await?;

        let tx_control = TransactionControl::new()
            .with_serializable_read_write()
            .with_commit();

        let query_params = QueryParameters::new().with_value("$id", ydb::Value::Uint64(entry_id));

        let result = session
            .execute(ctx, tx_control, &storage.select_query, query_params)
            .await?;

        let mut attempts = 0;
        // Здесь нужно обработать трейс, чтобы получить количество попыток
        // Предположим, что это делается через трейсинг, который нужно настроить отдельно

        let mut rows = result.rows().await?;
        if !rows.next().await? {
            return Err(StorageError::EntryNotFound(entry_id));
        }

        let row = rows.current().unwrap();

        let mut rows = result.rows().await?;
        if !rows.next().await? {
            return Err(StorageError::EntryNotFound(entry_id));
        }

        let row = rows.current().unwrap();

        let id = row
            .get("id")
            .and_then(|cell| cell.as_uint64())
            .ok_or_else(|| StorageError::Other("id not found".to_string()))?;
        let payload_str = row
            .get("payload_str")
            .and_then(|cell| cell.as_utf8().map(String::from));
        let payload_double = row.get("payload_double").and_then(|cell| cell.as_double());
        let payload_timestamp = row
            .get("payload_timestamp")
            .and_then(|cell| cell.as_timestamp().map(DateTime::<Utc>::from));

        let e = generator::Row {
            id,
            payload_str,
            payload_double,
            payload_timestamp,
        };

        Ok((e, attempts))
    }

    pub async fn write(
        ctx: &tokio::task::Context<'_>,
        table_client: &TableClient,
        storage: &Storage,
        e: Row,
    ) -> Result<(usize, ()), StorageError> {
        table_client
            .retry_transaction(|tx| async {
                let mut tx = tx; // move tx lifetime into code block

                tx.query(
                    ydb::Query::from(
                        "DECLARE $hash as Utf8;
                        DECLARE $src as Utf8;

                        REPLACE INTO
                            urls (hash, src)
                        VALUES
                            ($hash, $src);
                        ",
                    )
                        .with_params(ydb_params!("$hash" => hash.clone(), "$src" => long.clone())),
                )
                    .await?;
                tx.commit().await?;
                Ok(())
            })
            .await?;

        let row = ydb_params!(
            "id" => 1_i64,
            "payload_str" => "test",
            "payload_double" => "",
            "payload_timestamp" => "",
        );

        let list = Value::list_from(example, rows)?;

        let query = Query::new(
            "DECLARE $list AS List<Struct<
                id: Int64,
                val: Utf8,
                >>;

                UPSERT INTO test
                SELECT * FROM AS_TABLE($list)
                ",
        )
            .with_params(ydb_params!("$list" => list));

        table_client
            .retry_transaction(|tx| async {
                let mut tx = tx; // move tx lifetime into code block

                tx.query(
                    ydb::Query::from(
                        "DECLARE $hash as Utf8;
			    DECLARE $src as Utf8;

			    REPLACE INTO
				    urls (hash, src)
			    VALUES
				    ($hash, $src);
",
                    )
                        .with_params(ydb_params!("$hash" => hash.clone(), "$src" => long.clone())),
                )
                    .await?;
                tx.commit().await?;
                Ok(())
            })
            .await?;

        let query_params = QueryParameters::new()
            .with_value("$id", ydb::Value::Uint64(e.id))
            .with_value(
                "$payload_str",
                ydb::Value::Utf8(e.payload_str.unwrap_or_default()),
            )
            .with_value(
                "$payload_double",
                ydb::Value::Double(e.payload_double.unwrap_or_default()),
            )
            .with_value(
                "$payload_timestamp",
                ydb::Value::Timestamp(ydb::Timestamp::from_datetime(
                    e.payload_timestamp.unwrap_or_default(),
                )),
            );

        let result = session
            .execute(ctx, tx_control, &storage.upsert_query, query_params)
            .await?;

        Ok((attempts, ()))
    }

    async fn create_table(
        ctx: &tokio::task::Context<'_>,
        storage: &Storage,
    ) -> Result<(), StorageError> {
        let timeout_duration = Duration::from_millis(storage.cfg.write_timeout as u64);
        let ctx = ctx.clone();
        let cancellation = ctx.deadline().clone();

        let session = storage.db.table().create_session().await?;

        let table_path = format!("{}/{}", storage.prefix, storage.cfg.table);

        let column_defs = vec![
            ydb::table::ColumnDefinition::new("hash", ydb::types::Type::Uint64).with_optional(),
            ydb::table::ColumnDefinition::new("id", ydb::types::Type::Uint64).with_optional(),
            ydb::table::ColumnDefinition::new("payload_str", ydb::types::Type::Utf8)
                .with_optional(),
            ydb::table::ColumnDefinition::new("payload_double", ydb::types::Type::Double)
                .with_optional(),
            ydb::table::ColumnDefinition::new("payload_timestamp", ydb::types::Type::Timestamp)
                .with_optional(),
            ydb::table::ColumnDefinition::new("payload_hash", ydb::types::Type::Uint64)
                .with_optional(),
        ];

        let primary_key = ydb::table::PrimaryKey::new(vec!["hash", "id"]);

        let partitioning = ydb::table::PartitioningSettings::new()
            .with_partitioning_by_size(true)
            .with_partition_size_mb(storage.cfg.partition_size)
            .with_min_partitions_count(storage.cfg.min_partitions_count)
            .with_max_partitions_count(storage.cfg.max_partitions_count);

        let partitions = ydb::table::Partitions::uniform(storage.cfg.min_partitions_count);

        session
            .create_table(
                ctx,
                &table_path,
                column_defs,
                primary_key,
                partitioning,
                partitions,
            )
            .await?;

        Ok(())
    }

    fn drop_table(table_client: &Storage) {
        let _ = table_client
            .retry_execute_scheme_query("DROP TABLE test")
            .await; // ignore drop error
    }
}
