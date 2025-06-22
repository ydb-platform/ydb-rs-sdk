use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::timeout;
use ydb::{ClientBuilder, TableClient, YdbOrCustomerError, YdbResult, YdbResultWithCustomerErr};
use ydb_slo_tests_lib::args::CreateArgs;
use ydb_slo_tests_lib::cli::SloTestsCli;
use ydb_slo_tests_lib::db::query::{
    generate_create_table_query, generate_drop_table_query, generate_read_query,
    generate_write_query,
};
use ydb_slo_tests_lib::db::row::{Row, RowID};
use ydb_slo_tests_lib::workers::ReadWriter;

pub type Attempts = usize;

#[derive(Clone)]
pub struct Database {
    db_table_client: TableClient,
    cli_args: SloTestsCli,
}

impl Database {
    pub async fn new(cli: SloTestsCli) -> YdbResultWithCustomerErr<Self> {
        let client = ClientBuilder::new_from_connection_string(&cli.endpoint)?
            .with_database(&cli.db)
            .client()?;

        match timeout(
            Duration::from_secs(cli.db_init_timeout_seconds),
            client.wait(),
        )
        .await
        {
            Ok(res) => res?,
            Err(elapsed) => {
                return Err(YdbOrCustomerError::from_err(elapsed));
            }
        }

        let table_client = client.table_client();

        Ok(Self {
            db_table_client: table_client,
            cli_args: cli,
        })
    }

    pub async fn create_table(&self, create_args: &CreateArgs) -> YdbResult<()> {
        let query = generate_create_table_query(
            &self.cli_args.table_name,
            create_args.min_partitions_count,
            create_args.max_partitions_count,
            create_args.partition_size_mb,
        );

        self.db_table_client.retry_execute_scheme_query(query).await
    }

    pub async fn drop_table(&self) -> YdbResult<()> {
        let query = generate_drop_table_query(self.cli_args.table_name.as_str());

        self.db_table_client.retry_execute_scheme_query(query).await
    }
}

#[async_trait]
impl ReadWriter for Database {
    async fn read(
        &self,
        row_id: RowID,
        timeout: Duration,
    ) -> (YdbResultWithCustomerErr<()>, Attempts) {
        let query = generate_read_query(self.cli_args.table_name.as_str(), row_id);
        let attempts = AtomicUsize::new(0);

        let result = match tokio::time::timeout(
            timeout,
            self.db_table_client.retry_transaction(|t| async {
                let mut t = t;
                attempts.fetch_add(1, Ordering::Relaxed);
                t.query(query.clone()).await?;
                Ok(())
            }),
        )
        .await
        {
            Ok(res) => res,
            Err(elapsed) => Err(YdbOrCustomerError::from_err(elapsed)),
        };

        if attempts.load(Ordering::Relaxed) > 0 {
            attempts.fetch_sub(1, Ordering::Relaxed);
            (result, attempts.load(Ordering::Relaxed))
        } else {
            (result, attempts.load(Ordering::Relaxed))
        }
    }

    async fn write(&self, row: Row, timeout: Duration) -> (YdbResultWithCustomerErr<()>, Attempts) {
        let query = generate_write_query(self.cli_args.table_name.as_str(), row);
        let attempts = AtomicUsize::new(0);

        let result = match tokio::time::timeout(
            timeout,
            self.db_table_client.retry_transaction(|t| async {
                let mut t = t;
                attempts.fetch_add(1, Ordering::Relaxed);
                t.query(query.clone()).await?;
                t.commit().await?;
                Ok(())
            }),
        )
        .await
        {
            Ok(res) => res,
            Err(elapsed) => Err(YdbOrCustomerError::from_err(elapsed)),
        };

        if attempts.load(Ordering::Relaxed) > 0 {
            attempts.fetch_sub(1, Ordering::Relaxed);
            (result, attempts.load(Ordering::Relaxed))
        } else {
            (result, attempts.load(Ordering::Relaxed))
        }
    }
}
