use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use tokio::time::{Instant, sleep};
use ydb::{DescribeConsumerOptionsBuilder, ResultSet};

use slo_framework::topic_tx::{ChainTransition, MessageCoordinate, PartitionId, TopicOffset};

use super::TopicTxStorage;
use super::queries::{required_field, transition_from_row};

const SHUTDOWN_VERIFICATION_TIMEOUT: Duration = Duration::from_secs(5);
const STATE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const STABLE_STATE_PERIOD: Duration = Duration::from_secs(1);
const POOL_POLL_INTERVAL: Duration = Duration::from_millis(10);

// A valid chain is uniquely determined by its committed partition offsets;
// verify_current_state rechecks the corresponding table rows on every poll.
struct StableObservation {
    partition_offsets: Vec<PartitionOffsets>,
    unchanged_since: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PartitionOffsets {
    pub(super) partition_id: PartitionId,
    pub(super) committed_offset: TopicOffset,
    pub(super) end_offset: TopicOffset,
}

impl TopicTxStorage {
    /// Verifies atomic chain state after workers stop, then checks that every
    /// query session was returned to the pool.
    pub(crate) async fn verify_shutdown_state(&self) -> Result<()> {
        self.wait_for_settled_state().await?;
        self.wait_for_pool_release().await
    }

    async fn wait_for_settled_state(&self) -> Result<()> {
        let timeout = self
            .params
            .operation_timeout
            .min(SHUTDOWN_VERIFICATION_TIMEOUT);
        let deadline = Instant::now() + timeout;
        // An ambiguous commit may finish on the server after its RPC has failed.
        // Require an unchanged valid observation before declaring shutdown clean.
        let mut stable_observation: Option<StableObservation> = None;

        loop {
            match self.verify_current_state().await {
                Ok(partition_offsets) => match &stable_observation {
                    Some(stable) if stable.partition_offsets == partition_offsets => {
                        if stable.unchanged_since.elapsed() >= STABLE_STATE_PERIOD {
                            return Ok(());
                        }
                    }
                    _ => {
                        stable_observation = Some(StableObservation {
                            partition_offsets,
                            unchanged_since: Instant::now(),
                        });
                    }
                },
                Err(error) => {
                    stable_observation = None;
                    if Instant::now() >= deadline {
                        return Err(error).context("final transaction state did not settle");
                    }
                }
            }

            if Instant::now() >= deadline {
                bail!(
                    "final transaction state did not remain unchanged for {} ms",
                    STABLE_STATE_PERIOD.as_millis(),
                );
            }
            sleep(STATE_POLL_INTERVAL).await;
        }
    }

    async fn verify_current_state(&self) -> Result<Vec<PartitionOffsets>> {
        // Topic and Query APIs cannot participate in one read snapshot. Equal
        // topic observations bracket table verification while offsets were stable.
        let partitions_before = self.read_partition_offsets().await?;
        self.verify_partitions(&partitions_before).await?;
        let partitions_after = self.read_partition_offsets().await?;
        ensure!(
            partitions_before == partitions_after,
            "topic transaction state changed while reading table transitions",
        );
        Ok(partitions_after)
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
            let transition_rows = self.read_partition_transition_rows(partition_id).await?;
            Self::verify_partition_table_state(partition, transition_rows)?;
        }

        let expected_transition_count = partitions
            .iter()
            .map(|partition| partition.committed_offset.value() as u64)
            .sum();
        self.verify_total_transition_count(expected_transition_count)
            .await
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

    async fn read_partition_transition_rows(&self, partition_id: PartitionId) -> Result<ResultSet> {
        // Reading one partition at a time keeps the standard SLO result below
        // the Query API result limit.
        let query = format!(
            "SELECT partition_id, input_offset, input_generation, output_generation
             FROM `{}`
             WHERE partition_id = $partition_id
             ORDER BY input_offset",
            self.params.table_path,
        );
        let result_set = self
            .query_client
            .clone()
            .query_result_set(query)
            .param("$partition_id", partition_id.value())
            .idempotent(true)
            .timeout(self.params.operation_timeout)
            .await
            .with_context(|| {
                format!(
                    "read partition {partition_id} from table {}",
                    self.params.table_path,
                )
            })?;
        ensure!(
            !result_set.is_truncated(),
            "table result for partition {partition_id} is truncated",
        );
        Ok(result_set)
    }

    fn verify_partition_table_state(
        partition: &PartitionOffsets,
        transition_rows: ResultSet,
    ) -> Result<()> {
        let partition_id = partition.partition_id;
        let mut transition_count = 0;
        for row in transition_rows.rows() {
            let expected = ChainTransition {
                coordinate: MessageCoordinate {
                    partition_id,
                    offset: TopicOffset::new(transition_count),
                },
                input_generation: transition_count as u64,
                output_generation: transition_count as u64 + 1,
            };
            let transition = transition_from_row(row)?;
            ensure!(
                transition == expected,
                "expected transition {expected:?}, found {transition:?}",
            );
            transition_count += 1;
        }
        ensure!(
            transition_count == partition.committed_offset.value(),
            "partition {partition_id} has {transition_count} table transitions, expected {}",
            partition.committed_offset,
        );
        Ok(())
    }

    async fn verify_total_transition_count(&self, expected: u64) -> Result<()> {
        let mut row = self
            .query_client
            .clone()
            .query_row(format!(
                "SELECT COUNT(*) AS transition_count FROM `{}`",
                self.params.table_path,
            ))
            .idempotent(true)
            .timeout(self.params.operation_timeout)
            .await
            .with_context(|| format!("count transitions in table {}", self.params.table_path))?;
        let actual: u64 = required_field(&mut row, "transition_count")?;
        ensure!(
            actual == expected,
            "table contains {actual} transitions, expected {expected}",
        );
        Ok(())
    }

    async fn wait_for_pool_release(&self) -> Result<()> {
        let timeout = self
            .params
            .operation_timeout
            .min(SHUTDOWN_VERIFICATION_TIMEOUT);
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
