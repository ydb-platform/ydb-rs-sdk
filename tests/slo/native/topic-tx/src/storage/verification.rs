use std::time::Duration;

use anyhow::{Context, Result, ensure};
use tokio::time::{Instant, sleep};
use ydb::{DescribeConsumerOptionsBuilder, Transaction, YdbOrCustomerError, closure};

use slo_framework::topic_tx::{ChainTransition, PartitionId, TopicOffset};

use super::TopicTxStorage;
use super::queries::{read_next_transition, required_field};
use super::transaction::{invalid_chain_state, reader_options};

const POOL_RELEASE_TIMEOUT: Duration = Duration::from_secs(5);
const POOL_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PartitionOffsets {
    pub(super) partition_id: PartitionId,
    pub(super) committed_offset: TopicOffset,
    pub(super) end_offset: TopicOffset,
}

impl TopicTxStorage {
    /// Verifies atomic chain state after workers stop, then checks that no query
    /// session remains in use or creation.
    pub(crate) async fn verify_shutdown_state(&self) -> Result<()> {
        self.verify_transactional_snapshots().await?;
        for partition in self.read_partition_offsets().await? {
            ensure!(
                partition.end_offset.value() - partition.committed_offset.value() == 1,
                "partition {} must have exactly one unconsumed event: committed offset {}, end offset {}",
                partition.partition_id,
                partition.committed_offset,
                partition.end_offset,
            );
        }
        self.wait_for_pool_release().await
    }

    async fn verify_transactional_snapshots(&self) -> Result<()> {
        for raw_partition_id in 0..self.params.partition_count {
            let partition_id = PartitionId::new(raw_partition_id as i64);
            self.verify_partition_snapshot(partition_id).await?;
        }
        Ok(())
    }

    async fn verify_partition_snapshot(&self, partition_id: PartitionId) -> Result<()> {
        let options = reader_options(partition_id, &self.params);
        let mut reader = self
            .topic_client
            .clone()
            .create_reader_with_params(options)
            .await
            .with_context(|| format!("open verification reader for partition {partition_id}"))?;
        let timeout = self.params.operation_timeout;
        let deadline = Instant::now() + timeout;
        let table_path = &self.params.table_path;

        self.query_client
            .retry_tx(closure!(
                [&mut reader, table_path, partition_id, &deadline],
                async |tx: &mut Transaction| {
                    // Observe both sides of the chain in one transaction, then
                    // roll it back so verification does not advance the consumer.
                    let transition =
                        read_next_transition(reader, tx, *partition_id, *deadline).await?;
                    verify_partition_table_state(tx, table_path, &transition).await?;
                    tx.rollback().await?;
                    Ok(())
                }
            ))
            .idempotent(true)
            .timeout(timeout)
            .await
            .map_err(anyhow::Error::new)
            .with_context(|| format!("verify transaction snapshot for partition {partition_id}"))
    }

    pub(super) async fn read_partition_offsets(&self) -> Result<Vec<PartitionOffsets>> {
        let options = DescribeConsumerOptionsBuilder::default()
            .include_stats(true)
            .build()
            .context("build topic transaction consumer description options")?;
        let description = self
            .topic_client
            .clone()
            .describe_consumer(
                self.params.topic_path.clone(),
                self.params.consumer_name.clone(),
                options,
            )
            .await
            .with_context(|| {
                format!(
                    "describe consumer {} on topic {}",
                    self.params.consumer_name, self.params.topic_path,
                )
            })?;
        let mut partitions = Vec::with_capacity(description.partitions.len());
        for partition in description.partitions {
            let partition_id = PartitionId::new(partition.partition_id);
            let committed_offset = partition.consumer_stats.committed_offset;
            let end_offset = partition.stats.end_offset;
            ensure!(
                committed_offset >= 0,
                "partition {partition_id} has negative committed offset {committed_offset}",
            );
            ensure!(
                end_offset >= 0,
                "partition {partition_id} has negative end offset {end_offset}",
            );
            partitions.push(PartitionOffsets {
                partition_id,
                committed_offset: TopicOffset::new(committed_offset),
                end_offset: TopicOffset::new(end_offset),
            });
        }
        partitions.sort_unstable_by_key(|partition| partition.partition_id);

        Ok(partitions)
    }

    async fn wait_for_pool_release(&self) -> Result<()> {
        let timeout = self.params.operation_timeout.min(POOL_RELEASE_TIMEOUT);
        let deadline = Instant::now() + timeout;
        loop {
            let stats = self.client.session_pool_stats();
            if stats.in_use == 0 && stats.create_in_progress == 0 {
                ensure!(
                    stats.size <= stats.limit,
                    "query session pool contains {} sessions above its limit {}",
                    stats.size,
                    stats.limit,
                );
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "query session pool did not release all work: {} in use, {} being created",
                stats.in_use,
                stats.create_in_progress,
            );
            sleep(POOL_POLL_INTERVAL).await;
        }
    }
}

async fn verify_partition_table_state(
    tx: &mut Transaction,
    table_path: &str,
    transition: &ChainTransition,
) -> Result<(), YdbOrCustomerError> {
    let partition_id = transition.coordinate.partition_id;
    let next_offset = transition.coordinate.offset.value();
    let query = format!(
        "SELECT
            COUNT(*) AS transition_count,
            COUNT_IF(
                input_offset >= 0
                AND input_offset < $next_offset
                AND input_generation = CAST(input_offset AS Uint64)
                AND output_generation = CAST(input_offset + 1 AS Uint64)
            ) AS valid_transition_count
         FROM `{table_path}`
         WHERE partition_id = $partition_id",
    );
    let mut row = tx
        .query_row(query)
        .param("$partition_id", partition_id.value())
        .param("$next_offset", next_offset)
        .await?;
    let transition_count: u64 =
        required_field(&mut row, "transition_count").map_err(invalid_chain_state)?;
    let valid_transition_count: u64 =
        required_field(&mut row, "valid_transition_count").map_err(invalid_chain_state)?;

    validate_partition_table_counts(
        partition_id,
        next_offset,
        transition_count,
        valid_transition_count,
    )
    .map_err(invalid_chain_state)
}

fn validate_partition_table_counts(
    partition_id: PartitionId,
    next_offset: i64,
    transition_count: u64,
    valid_transition_count: u64,
) -> Result<()> {
    ensure!(
        next_offset > 0,
        "partition {partition_id} made no transaction progress",
    );
    let expected_count = u64::try_from(next_offset).context("next topic offset is negative")?;
    ensure!(
        transition_count == expected_count,
        "partition {partition_id} has {transition_count} table transitions before live topic offset {next_offset}, expected {expected_count}",
    );
    ensure!(
        valid_transition_count == expected_count,
        "partition {partition_id} has {valid_transition_count} valid table transitions before live topic offset {next_offset}, expected {expected_count}",
    );
    Ok(())
}
