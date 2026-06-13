use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use slo_framework::kv::{Database, KvWorkload, Params};
use slo_framework::{Framework, RowID, TestRow, Workload};
use ydb::ClientBuilder;
use ydb::QueryClient;

pub struct Storage {
    query_client: QueryClient,
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

        Ok(Self {
            query_client: client.query_client(),
            table_path: params.table_path.clone(),
            read_timeout: params.read_timeout,
            write_timeout: params.write_timeout,
            partition_size: params.partition_size,
            min_partition_count: params.min_partition_count,
            max_partition_count: params.max_partition_count,
        })
    }

    fn idempotent_client(&self) -> QueryClient {
        self.query_client.clone_with_idempotent_operations(true)
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

        let mut qc = self.idempotent_client();
        tokio::time::timeout(self.write_timeout, async move { qc.exec(query).await })
            .await
            .map_err(|_| "create table timeout".to_string())?
            .map_err(|err| err.to_string())
    }

    async fn drop_table(&self) -> Result<(), String> {
        let query = format!("DROP TABLE `{table}`", table = self.table_path);
        let mut qc = self.idempotent_client();
        tokio::time::timeout(self.write_timeout, async move { qc.exec(query).await })
            .await
            .map_err(|_| "drop table timeout".to_string())?
            .map_err(|err| err.to_string())
    }

    async fn read(&self, id: RowID) -> Result<(TestRow, u64), String> {
        let attempts = Arc::new(AtomicUsize::new(0));
        let select_sql = format!(
            r#"
            DECLARE $id AS Uint64;

            SELECT id, payload_str, payload_double, payload_timestamp, payload_hash
            FROM `{table}`
            WHERE id = $id AND hash = Digest::NumericHash($id);
            "#,
            table = self.table_path
        );

        let attempts_for_op = attempts.clone();
        let mut qc = self.idempotent_client();
        let row = tokio::time::timeout(self.read_timeout, async move {
            attempts_for_op.fetch_add(1, Ordering::Relaxed);
            qc.query_row(select_sql).param("$id", id).await
        })
        .await
        .map_err(|_| "read timeout".to_string())?
        .map_err(|err| err.to_string())?;

        Ok((row_to_test_row(row)?, attempts.load(Ordering::Relaxed) as u64))
    }

    async fn write(&self, row: TestRow) -> Result<u64, String> {
        let attempts = Arc::new(AtomicUsize::new(0));
        let upsert_sql = format!(
            r#"
            DECLARE $id AS Uint64;
            DECLARE $payload_str AS Utf8;
            DECLARE $payload_double AS Double;
            DECLARE $payload_timestamp AS Timestamp;

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
        );

        let attempts_for_op = attempts.clone();
        let mut qc = self.idempotent_client();
        tokio::time::timeout(self.write_timeout, async move {
            attempts_for_op.fetch_add(1, Ordering::Relaxed);
            qc.exec(upsert_sql)
                .param("$id", row.id)
                .param("$payload_str", row.payload_str)
                .param("$payload_double", row.payload_double)
                .param("$payload_timestamp", row.payload_timestamp)
                .await
        })
        .await
        .map_err(|_| "write timeout".to_string())?
        .map_err(|err| err.to_string())?;

        Ok(attempts.load(Ordering::Relaxed) as u64)
    }

    async fn close(&self) -> Result<(), String> {
        Ok(())
    }
}

fn row_to_test_row(mut row: ydb::Row) -> Result<TestRow, String> {
    let id: u64 = row
        .remove_field_by_name("id")
        .map_err(|err: ydb::YdbError| err.to_string())?
        .try_into()
        .map_err(|err: ydb::YdbError| err.to_string())?;
    let payload_str: String = row
        .remove_field_by_name("payload_str")
        .map_err(|err: ydb::YdbError| err.to_string())?
        .try_into()
        .map_err(|err: ydb::YdbError| err.to_string())?;
    let payload_double: f64 = row
        .remove_field_by_name("payload_double")
        .map_err(|err: ydb::YdbError| err.to_string())?
        .try_into()
        .map_err(|err: ydb::YdbError| err.to_string())?;
    let payload_timestamp: std::time::SystemTime = row
        .remove_field_by_name("payload_timestamp")
        .map_err(|err: ydb::YdbError| err.to_string())?
        .try_into()
        .map_err(|err: ydb::YdbError| err.to_string())?;

    Ok(TestRow::new(
        id,
        payload_str,
        payload_double,
        payload_timestamp,
    ))
}

pub async fn new_workload(fw: Framework) -> Result<Box<dyn Workload>, String> {
    let params = slo_framework::kv::parse_params(&fw);
    let storage = Storage::new(&fw, &params).await?;
    Ok(Box::new(KvWorkload::new(fw, params, storage)))
}
