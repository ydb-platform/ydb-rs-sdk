mod queries;
mod transaction;
mod verification;

use anyhow::{Context, Result, bail};
use ydb::{
    Client, ClientBuilder, ConsumerBuilder, CreateTopicOptionsBuilder, PartitioningStrategy,
    QueryClient, SessionPoolSettings, TopicClient, TopicWriterMessage, TopicWriterOptions,
};

use slo_framework::topic_tx::{ChainEvent, Params, PartitionId, TopicOffset};
use slo_framework::{Framework, preserve_primary_error};

pub(super) use transaction::PartitionWorker;

pub(super) struct TopicTxStorage {
    client: Client,
    topic_client: TopicClient,
    query_client: QueryClient,
    params: Params,
}

impl TopicTxStorage {
    pub(super) async fn connect(framework: &Framework, params: Params) -> Result<Self> {
        let client = ClientBuilder::new_from_connection_string(&framework.config.connection_string)
            .context("parse YDB connection string")?
            .client()
            .context("create YDB client")?;
        client.wait().await.context("wait for YDB discovery")?;

        let client = client
            .with_session_pool(
                SessionPoolSettings::new()
                    .with_limit(params.session_pool_size)
                    .with_warm_up(params.session_pool_size)
                    .with_session_create_timeout(params.operation_timeout)
                    .with_session_delete_timeout(params.operation_timeout),
            )
            .await
            .context("initialize query session pool")?;

        Ok(Self {
            topic_client: client.topic_client(),
            query_client: client.query_client(),
            client,
            params,
        })
    }

    pub(super) async fn setup_resources(&self) -> Result<()> {
        self.create_table().await?;
        self.create_topic().await?;
        self.initialize_partition_chains().await
    }

    pub(super) async fn open_workers(&self) -> Result<Vec<PartitionWorker>> {
        let mut workers = Vec::with_capacity(self.params.partition_count);

        for raw_partition_id in 0..self.params.partition_count {
            let partition_id = PartitionId::new(raw_partition_id as i64);
            workers.push(
                PartitionWorker::open(
                    partition_id,
                    self.query_client.clone(),
                    self.topic_client.clone(),
                    &self.params,
                )
                .await?,
            );
        }

        Ok(workers)
    }

    pub(super) async fn cleanup_resources(&self) -> Result<()> {
        let mut topic_client = self.topic_client.clone();
        let topic_result = topic_client
            .drop_topic(self.params.topic_path.clone())
            .await
            .context("drop topic transaction topic");

        let mut query_client = self.query_client.clone();
        let table_result = query_client
            .exec(format!("DROP TABLE `{}`", self.params.table_path))
            .idempotent(true)
            .timeout(self.params.operation_timeout)
            .await
            .context("drop topic transaction table");

        preserve_primary_error(topic_result, table_result)
    }

    async fn create_table(&self) -> Result<()> {
        let query = format!(
            "CREATE TABLE `{table}` (
                partition_id Int64 NOT NULL,
                input_offset Int64 NOT NULL,
                input_generation Uint64 NOT NULL,
                output_generation Uint64 NOT NULL,
                PRIMARY KEY (partition_id, input_offset)
            )",
            table = self.params.table_path,
        );
        self.query_client
            .clone()
            .exec(query)
            .idempotent(true)
            .timeout(self.params.operation_timeout)
            .await
            .with_context(|| format!("create table {}", self.params.table_path))
    }

    async fn create_topic(&self) -> Result<()> {
        let consumer = ConsumerBuilder::default()
            .name(self.params.consumer_name.clone())
            .important(true)
            .build()
            .context("build topic transaction consumer")?;
        let partition_count = self.params.partition_count as i64;
        let options = CreateTopicOptionsBuilder::default()
            .min_active_partitions(partition_count)
            .partition_count_limit(partition_count)
            .consumers(vec![consumer])
            .build()
            .context("build topic transaction topic options")?;

        self.topic_client
            .clone()
            .create_topic(self.params.topic_path.clone(), options)
            .await
            .with_context(|| format!("create topic {}", self.params.topic_path))
    }

    /// Writes the single generation-zero event that starts each partition chain.
    async fn initialize_partition_chains(&self) -> Result<()> {
        for raw_partition_id in 0..self.params.partition_count {
            let partition_id = PartitionId::new(raw_partition_id as i64);
            let initial_event = ChainEvent::initial(partition_id)?;
            let options = TopicWriterOptions::builder()
                .topic_path(self.params.topic_path.clone())
                .partitioning(PartitioningStrategy::PartitionId(partition_id.value()))
                .build();
            let writer = self
                .topic_client
                .clone()
                .create_writer_with_params(options)
                .await
                .with_context(|| {
                    format!("open initialization writer for partition {partition_id}")
                })?;
            writer
                .write_with_ack(TopicWriterMessage::new(initial_event.encode()))
                .await
                .with_context(|| format!("write initial event to partition {partition_id}"))?;
            writer.stop().await.with_context(|| {
                format!("stop initialization writer for partition {partition_id}")
            })?;
        }

        let partitions = self.read_partition_offsets().await?;
        let expected_partitions = self.params.partition_count;
        if partitions.len() != expected_partitions {
            bail!(
                "expected {expected_partitions} initialized partitions, found {}",
                partitions.len(),
            );
        }
        for (raw_partition_id, partition) in partitions.into_iter().enumerate() {
            let expected_partition_id = PartitionId::new(raw_partition_id as i64);
            if partition.partition_id != expected_partition_id {
                bail!(
                    "expected initialized partition {expected_partition_id}, found {}",
                    partition.partition_id,
                );
            }
            if partition.committed_offset != TopicOffset::new(0)
                || partition.end_offset != TopicOffset::new(1)
            {
                bail!(
                    "partition {} was not initialized with exactly one event: committed offset {}, end offset {}",
                    partition.partition_id,
                    partition.committed_offset,
                    partition.end_offset,
                );
            }
        }
        Ok(())
    }
}
