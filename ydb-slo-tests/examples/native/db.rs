use async_trait::async_trait;
use ydb::{
    ydb_params, ClientBuilder, Query, Row, TableClient, YdbResult, YdbResultWithCustomerErr,
};
use ydb_slo_tests::args::CreateArgs;
use ydb_slo_tests::cli::SloTestsCli;
use ydb_slo_tests::row::{RowID, TestRow};
use ydb_slo_tests::workers::ReadWriter;

#[derive(Clone)]
pub struct Database {
    db_table_client: TableClient,
    cli_args: SloTestsCli,
}

impl Database {
    pub async fn new(cli: SloTestsCli) -> YdbResult<Self> {
        let client = ClientBuilder::new_from_connection_string(&cli.endpoint)?
            .with_database(&cli.db)
            .client()?;

        client.wait().await?;

        let table_client = client.table_client();

        Ok(Self {
            db_table_client: table_client,
            cli_args: cli,
        })
    }

    pub async fn create_table(&self, create_args: &CreateArgs) -> YdbResult<()> {
        self.db_table_client
            .retry_execute_scheme_query(format!(
                "CREATE TABLE IF NOT EXISTS {table}
                    (
                        hash Uint64,
                        id Uint64,
                        payload_str Text?,
                        payload_double Double?,
                        payload_timestamp Timestamp?,
                        payload_hash Uint64?,
                        PRIMARY KEY (hash, id)
                    ) WITH (
                        UNIFORM_PARTITIONS = {min_partitions_count},
                        AUTO_PARTITIONING_BY_SIZE = ENABLED,
                        AUTO_PARTITIONING_PARTITION_SIZE_MB = {partition_size},
                        AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {min_partitions_count},
                        AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = {max_partitions_count}
                    )",
                table = self.cli_args.table_name,
                min_partitions_count = create_args.min_partitions_count,
                max_partitions_count = create_args.max_partitions_count,
                partition_size = create_args.partition_size,
            ))
            .await
    }

    pub async fn drop_table(&self) -> YdbResult<()> {
        self.db_table_client
            .retry_execute_scheme_query(format!("DROP TABLE {}", self.cli_args.table_name))
            .await
    }
}

#[async_trait]
impl ReadWriter for Database {
    async fn read(&self, row_id: RowID) -> YdbResult<Row> {
        let query = Query::from(format!(
            r#"
            DECLARE $id AS Uint64;

            SELECT id, payload_str, payload_double, payload_timestamp, payload_hash
            FROM {table}
            WHERE id = $id AND hash = Digest::NumericHash($id);
            "#,
            table = self.cli_args.table_name
        ))
        .with_params(ydb_params!("$id" => row_id));

        self.db_table_client
            .retry_transaction(|t| async {
                let mut t = t;
                let res = t.query(query.clone()).await?;
                Ok(res)
            })
            .await?
            .into_only_row()
    }

    async fn write(&self, row: TestRow) -> YdbResultWithCustomerErr<()> {
        let query = Query::from(format!(
            r#"
            DECLARE $id AS Uint64;
            DECLARE $payload_str AS Utf8;
            DECLARE $payload_double AS Double;
            DECLARE $payload_timestamp AS Timestamp;

            UPSERT INTO {table} (
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
            table = &self.cli_args.table_name,
        ))
        .with_params(ydb_params!(
                "$id" => row.id,
                "$payload_str" => row.payload_str,
                "$payload_double" => row.payload_double,
                "$payload_timestamp" => row.payload_timestamp,
        ));

        self.db_table_client
            .retry_transaction(|t| async {
                let mut t = t;
                t.query(query.clone()).await?;
                t.commit().await?;
                Ok(())
            })
            .await
    }
}
