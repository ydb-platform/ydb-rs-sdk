use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use slo_framework::kv::{Database, KvWorkload, Params};
use slo_framework::{test_row_from_row, Framework, RowID, TestRow, Workload};
use ydb::{
    ydb_params, ClientBuilder, Mode, Query, SessionPoolSettings, TableClient,
    TransactionOptions, YdbOrCustomerError,
};

pub struct Storage {
    read_table_client: TableClient,
    write_table_client: TableClient,
    table_path: String,
    read_timeout: Duration,
    write_timeout: Duration,
    partition_size: u64,
    min_partition_count: u64,
    max_partition_count: u64,
}

impl Storage {
    pub async fn new(fw: &Framework, params: &Params) -> Result<Self, String> {
        let client = ClientBuilder::new_from_connection_string(&fw.config.connection_string)
            .map_err(|err| err.to_string())?
            .client()
            .map_err(|err| err.to_string())?;

        client.wait().await.map_err(|err| err.to_string())?;

        let pool_limit = params.pool_size() as usize;
        let session_rpc_timeout = params.read_timeout.max(params.write_timeout);
        let client = client
            .with_session_pool(
                SessionPoolSettings::new()
                    .with_limit(pool_limit)
                    .with_warm_up(pool_limit)
                    .with_session_create_timeout(session_rpc_timeout)
                    .with_session_delete_timeout(session_rpc_timeout),
            )
            .await
            .map_err(|err| err.to_string())?;

        let table_client = client.table_client().clone_with_idempotent_operations(true);

        Ok(Self {
            read_table_client: table_client.clone_with_transaction_options(
                TransactionOptions::default()
                    .with_autocommit(true)
                    .with_mode(Mode::SnapshotReadOnly),
            ),
            write_table_client: table_client.clone_with_transaction_options(
                TransactionOptions::default()
                    .with_autocommit(true)
                    .with_mode(Mode::SerializableReadWrite),
            ),
            table_path: params.table_path.clone(),
            read_timeout: params.read_timeout,
            write_timeout: params.write_timeout,
            partition_size: params.partition_size,
            min_partition_count: params.min_partition_count,
            max_partition_count: params.max_partition_count,
        })
    }
}

#[async_trait]
impl Database for Storage {
    async fn create_table(&self) -> Result<(), String> {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS `{table}` (
                hash Uint64,
                id Uint64,
                payload_str Text?,
                payload_double Double?,
                payload_timestamp Timestamp?,
                payload_hash Uint64?,
                PRIMARY KEY (hash, id)
            ) WITH (
                UNIFORM_PARTITIONS = {min_partition_count},
                AUTO_PARTITIONING_BY_SIZE = ENABLED,
                AUTO_PARTITIONING_PARTITION_SIZE_MB = {partition_size},
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {min_partition_count},
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = {max_partition_count}
            )",
            table = self.table_path,
            min_partition_count = self.min_partition_count,
            max_partition_count = self.max_partition_count,
            partition_size = self.partition_size,
        );

        tokio::time::timeout(
            self.write_timeout,
            self.write_table_client.retry_execute_scheme_query(query),
        )
        .await
        .map_err(|_| "create table timeout".to_string())?
        .map_err(|err| err.to_string())
    }

    async fn drop_table(&self) -> Result<(), String> {
        let query = format!("DROP TABLE `{table}`", table = self.table_path);
        tokio::time::timeout(
            self.write_timeout,
            self.write_table_client.retry_execute_scheme_query(query),
        )
        .await
        .map_err(|_| "drop table timeout".to_string())?
        .map_err(|err| err.to_string())
    }

    async fn read(&self, id: RowID) -> Result<(TestRow, u64), String> {
        let attempts = Arc::new(AtomicUsize::new(0));
        let query = Query::from(format!(
            r#"
            SELECT id, payload_str, payload_double, payload_timestamp, payload_hash
            FROM `{table}`
            WHERE id = $id AND hash = Digest::NumericHash($id);
            "#,
            table = self.table_path
        ))
        .with_params(ydb_params!("$id" => id));

        let attempts_for_tx = attempts.clone();
        let result = tokio::time::timeout(self.read_timeout, async {
            self.read_table_client
                .retry_transaction(|t| {
                    let query = query.clone();
                    let attempts_for_tx = attempts_for_tx.clone();
                    async move {
                        attempts_for_tx.fetch_add(1, Ordering::Relaxed);
                        let mut t = t;
                        Ok(t.query(query).await?)
                    }
                })
                .await
        })
        .await
        .map_err(|_| "read timeout".to_string())?
        .map_err(map_ydb_error)?;

        let row = result.into_only_row().map_err(|err| err.to_string())?;
        let test_row = test_row_from_row(row)?;
        Ok((test_row, attempts.load(Ordering::Relaxed) as u64))
    }

    async fn write(&self, row: TestRow) -> Result<u64, String> {
        let attempts = Arc::new(AtomicUsize::new(0));
        let query = Query::from(format!(
            r#"
            UPSERT INTO `{table}` (
                id,
                hash,
                payload_str,
                payload_double,
                payload_timestamp
            ) VALUES (
                $id,
                Digest::NumericHash($id),
                $payload_str,
                $payload_double,
                $payload_timestamp
            );
            "#,
            table = self.table_path,
        ))
        .with_params(ydb_params!(
            "$id" => row.id,
            "$payload_str" => row.payload_str,
            "$payload_double" => row.payload_double,
            "$payload_timestamp" => row.payload_timestamp,
        ));

        let attempts_for_tx = attempts.clone();
        tokio::time::timeout(self.write_timeout, async {
            self.write_table_client
                .retry_transaction(|t| {
                    let query = query.clone();
                    let attempts_for_tx = attempts_for_tx.clone();
                    async move {
                        attempts_for_tx.fetch_add(1, Ordering::Relaxed);
                        let mut t = t;
                        t.query(query).await?;
                        t.commit().await?;
                        Ok(())
                    }
                })
                .await
        })
        .await
        .map_err(|_| "write timeout".to_string())?
        .map_err(map_ydb_error)?;

        Ok(attempts.load(Ordering::Relaxed) as u64)
    }

    async fn close(&self) -> Result<(), String> {
        Ok(())
    }
}

fn map_ydb_error(err: YdbOrCustomerError) -> String {
    err.to_string()
}

pub async fn new_workload(fw: Framework) -> Result<Box<dyn Workload>, String> {
    let params = slo_framework::kv::parse_params(&fw);
    let storage = Storage::new(&fw, &params).await?;
    Ok(Box::new(KvWorkload::new(fw, params, storage)))
}
