use std::time::Duration;

use anyhow::{Context, Result, ensure};
use tokio::time::{Instant, sleep};
use ydb::{Transaction, YdbOrCustomerError, closure};

use slo_framework::topic_tx::{ChainTransition, PartitionId};

use super::TopicTxStorage;
use super::queries::{read_next_transition, required_field};
use super::transaction::{invalid_chain_state, reader_options};

const POOL_RELEASE_TIMEOUT: Duration = Duration::from_secs(5);
const STATE_POLL_INTERVAL: Duration = Duration::from_millis(10);

struct PartitionObservation {
    partition_id: PartitionId,
    next_offset: i64,
    transition_count: u64,
    valid_transition_count: u64,
}

impl TopicTxStorage {
    /// Verifies atomic chain state after workers stop, then checks that no query
    /// session remains in use or creation.
    pub(crate) async fn verify_shutdown_state(&self) -> Result<()> {
        self.verify_transactional_chains().await?;
        self.wait_for_pool_release().await
    }

    async fn verify_transactional_chains(&self) -> Result<()> {
        for raw_partition_id in 0..self.params.partition_count {
            let partition_id = PartitionId::new(raw_partition_id as i64);
            self.verify_partition_chain(partition_id).await?;
        }
        Ok(())
    }

    async fn verify_partition_chain(&self, partition_id: PartitionId) -> Result<()> {
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

        let observation = self
            .query_client
            .retry_tx(closure!(
                [&mut reader, table_path, partition_id, &deadline],
                async |tx: &mut Transaction| {
                    let transition =
                        read_next_transition(reader, tx, *partition_id, *deadline).await?;
                    read_partition_table_observation(tx, table_path, &transition).await
                }
            ))
            .idempotent(true)
            .timeout(timeout)
            .await
            .map_err(anyhow::Error::new)
            .with_context(|| {
                format!("commit verification transaction for partition {partition_id}")
            })?;

        // Topic payloads are read from the live reader; only their consumer-offset updates belong
        // to the Query transaction. Commit must succeed before comparing that live observation
        // with the transaction's table view.
        validate_partition_table_counts(
            observation.partition_id,
            observation.next_offset,
            observation.transition_count,
            observation.valid_transition_count,
        )
        .with_context(|| format!("verify committed transaction for partition {partition_id}"))
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
            sleep(STATE_POLL_INTERVAL).await;
        }
    }
}

async fn read_partition_table_observation(
    tx: &mut Transaction,
    table_path: &str,
    transition: &ChainTransition,
) -> Result<PartitionObservation, YdbOrCustomerError> {
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

    Ok(PartitionObservation {
        partition_id,
        next_offset,
        transition_count,
        valid_transition_count,
    })
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
