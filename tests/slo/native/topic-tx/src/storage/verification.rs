use std::time::Duration;

use anyhow::{Context, Result, ensure};
use tokio::time::{Instant, sleep, timeout};
use ydb::DescribeConsumerOptionsBuilder;

use slo_framework::topic_tx::{PartitionId, TopicOffset};

use super::TopicTxStorage;
use super::queries::required_field;

const STABLE_STATE_PERIOD: Duration = Duration::from_secs(1);
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
        self.verify_after_quiet_period().await?;
        self.wait_for_pool_release().await
    }

    async fn verify_after_quiet_period(&self) -> Result<()> {
        timeout(self.params.operation_timeout, async {
            loop {
                let partition_offsets = self.wait_for_quiet_partition_offsets().await?;
                // Delay a table failure until the second topic observation. If
                // offsets moved during the scan, it was not a coherent snapshot.
                let verification = self.verify_partitions(&partition_offsets).await;
                let partition_offsets_after = self.read_partition_offsets().await?;

                if partition_offsets == partition_offsets_after {
                    return verification;
                }
            }
        })
        .await
        .context("final transaction verification timed out")?
    }

    /// A late commit can advance a partition after its RPC fails. One unchanged
    /// observation reduces that shutdown race but cannot resolve commit ambiguity.
    async fn wait_for_quiet_partition_offsets(&self) -> Result<Vec<PartitionOffsets>> {
        let mut previous = self.read_partition_offsets().await?;
        loop {
            sleep(STABLE_STATE_PERIOD).await;
            let current = self.read_partition_offsets().await?;
            if current == previous {
                return Ok(current);
            }
            previous = current;
        }
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

    async fn verify_partitions(&self, partitions: &[PartitionOffsets]) -> Result<()> {
        ensure!(
            partitions.len() == self.params.partition_count,
            "expected {} topic partitions, found {}",
            self.params.partition_count,
            partitions.len(),
        );

        for (raw_partition_id, partition) in partitions.iter().enumerate() {
            let partition_id = PartitionId::new(raw_partition_id as i64);
            Self::verify_partition_topic_state(partition_id, partition)?;
            self.verify_partition_table_state(partition).await?;
        }
        Ok(())
    }

    fn verify_partition_topic_state(
        expected_partition_id: PartitionId,
        partition: &PartitionOffsets,
    ) -> Result<()> {
        ensure!(
            partition.partition_id == expected_partition_id,
            "expected partition {expected_partition_id}, found {}",
            partition.partition_id,
        );
        ensure!(
            partition.committed_offset.value() > 0,
            "partition {expected_partition_id} made no transaction progress",
        );
        let expected_end = TopicOffset::new(
            partition
                .committed_offset
                .value()
                .checked_add(1)
                .context("committed topic offset overflow")?,
        );
        ensure!(
            partition.end_offset == expected_end,
            "partition {expected_partition_id} must contain one live chain event: committed offset {}, end offset {}",
            partition.committed_offset,
            partition.end_offset,
        );
        Ok(())
    }

    async fn verify_partition_table_state(&self, partition: &PartitionOffsets) -> Result<()> {
        let partition_id = partition.partition_id;
        let query = format!(
            "SELECT
                COUNT(*) AS transition_count,
                COUNT_IF(
                    input_offset >= 0
                    AND input_offset < $committed_offset
                    AND input_generation = CAST(input_offset AS Uint64)
                    AND output_generation = CAST(input_offset + 1 AS Uint64)
                ) AS valid_transition_count
             FROM `{}`
             WHERE partition_id = $partition_id",
            self.params.table_path,
        );
        let mut row = self
            .query_client
            .clone()
            .query_row(query)
            .param("$partition_id", partition_id.value())
            .param("$committed_offset", partition.committed_offset.value())
            .idempotent(true)
            .timeout(self.params.operation_timeout)
            .await
            .with_context(|| {
                format!(
                    "read partition {partition_id} from table {}",
                    self.params.table_path,
                )
            })?;
        let transition_count: u64 = required_field(&mut row, "transition_count")?;
        let valid_transition_count: u64 = required_field(&mut row, "valid_transition_count")?;
        let expected_count = partition.committed_offset.value() as u64;

        ensure!(
            transition_count == expected_count,
            "partition {partition_id} has {transition_count} table transitions, expected {expected_count}",
        );
        ensure!(
            valid_transition_count == expected_count,
            "partition {partition_id} has {valid_transition_count} valid table transitions, expected {expected_count}",
        );
        Ok(())
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
