use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::time::{Instant, timeout_at};
use tokio_util::sync::CancellationToken;
use ydb::{
    PartitioningStrategy, QueryClient, TopicClient, TopicReader, TopicReaderOptions, TopicSelector,
    TopicWriter, TopicWriterMessage, TopicWriterOptions, Transaction, YdbError, YdbOrCustomerError,
    closure,
};

use slo_framework::topic_tx::{MessageCoordinate, Params, PartitionId};
use slo_framework::{Logger, Metrics};

use super::queries::{insert_transition, transition_from_batch};
use super::{ERROR_COMMIT_PHASE_FAILURE, ERROR_OPERATIONAL_FAILURE, OPERATION_TRANSACTION};

/// Owns one partition reader and advances its chain one transaction at a time.
pub(crate) struct PartitionWorker {
    partition_id: PartitionId,
    query_client: QueryClient,
    reader: TopicReader,
    writer: TopicWriter,
    table_path: String,
    operation_timeout: Duration,
}

enum ChainAdvanceOutcome {
    Committed,
    CommitPhaseFailure(MessageCoordinate, anyhow::Error),
    OperationalFailure(anyhow::Error),
    InvalidChainState(anyhow::Error),
}

/// Result of one logical chain advance, including all `retry_tx` attempts.
struct ChainAdvanceReport {
    attempts: u64,
    outcome: ChainAdvanceOutcome,
}

impl PartitionWorker {
    pub(super) async fn open(
        partition_id: PartitionId,
        query_client: QueryClient,
        mut topic_client: TopicClient,
        params: &Params,
    ) -> Result<Self> {
        let reader_options = reader_options(partition_id, params);
        let reader = topic_client
            .create_reader_with_params(reader_options)
            .await
            .with_context(|| format!("open transaction reader for partition {partition_id}"))?;
        let writer_options = TopicWriterOptions::builder()
            .topic_path(params.topic_path.clone())
            .producer_id(format!("slo-topic-tx-partition-{partition_id}"))
            .partitioning(PartitioningStrategy::PartitionId(partition_id.value()))
            .build();
        let writer = topic_client
            .create_writer_with_params(writer_options)
            .await
            .with_context(|| format!("open transaction writer for partition {partition_id}"))?;

        Ok(Self {
            partition_id,
            query_client,
            reader,
            writer,
            table_path: params.table_path.clone(),
            operation_timeout: params.operation_timeout,
        })
    }

    pub(crate) async fn run(
        mut self,
        cancel: CancellationToken,
        metrics: Metrics,
        logger: Arc<Logger>,
    ) -> Result<PartitionId> {
        // Let an in-flight transaction resolve; cancellation only prevents the
        // worker from starting the next one.
        while !cancel.is_cancelled() {
            let span = metrics.start(OPERATION_TRANSACTION);
            let ChainAdvanceReport { attempts, outcome } = self.advance_chain_once().await;

            // TopicReaderTx discards buffered state and reconnects its reader in
            // the background whenever the transaction aborts.
            //
            // Non-terminal outcomes use bounded metric names; full errors
            // stay in logs. Invalid state propagates as a fatal error.
            match outcome {
                ChainAdvanceOutcome::Committed => {
                    span.finish(None, attempts);
                }
                ChainAdvanceOutcome::CommitPhaseFailure(coordinate, error) => {
                    let message =
                        format!("transaction commit phase failed at {coordinate}: {error:#}");
                    span.finish(Some(ERROR_COMMIT_PHASE_FAILURE), attempts);
                    logger.errorf(message);
                }
                ChainAdvanceOutcome::OperationalFailure(error) => {
                    let message = format!("{error:#}");
                    span.finish(Some(ERROR_OPERATIONAL_FAILURE), attempts);
                    logger.errorf(message);
                }
                ChainAdvanceOutcome::InvalidChainState(error) => {
                    return Err(error).with_context(|| {
                        format!("partition {} transaction failed", self.partition_id)
                    });
                }
            }
        }

        Ok(self.partition_id)
    }

    async fn advance_chain_once(&mut self) -> ChainAdvanceReport {
        let timeout = self.operation_timeout;
        let deadline = Instant::now() + timeout;
        let query_client = &self.query_client;
        let mut attempts = 0;
        // Set after the callback has queued all transactional work. A later
        // failure comes from the implicit commit phase, which includes writer
        // flush hooks as well as the commit RPC; `retry_tx` does not expose
        // which of those stages failed.
        let mut commit_phase_coordinate = None;

        let result = query_client
            .retry_tx(closure!(
                [
                    &mut attempts,
                    &mut commit_phase_coordinate,
                    &mut reader = &mut self.reader,
                    &mut writer = &mut self.writer,
                    &table_path = &self.table_path,
                    partition_id = self.partition_id,
                    &deadline,
                ],
                async |tx: &mut Transaction| {
                    *attempts += 1;
                    *commit_phase_coordinate = None;

                    let batch = {
                        let mut reader_tx = reader.tx_reader(tx).await?;
                        timeout_at(*deadline, reader_tx.read_batch())
                            .await
                            .map_err(|_| {
                                YdbError::Custom("transaction topic read timed out".to_string())
                            })??
                    };
                    let transition = transition_from_batch(batch, *partition_id).await?;
                    insert_transition(tx, table_path.as_str(), &transition).await?;
                    let writer_tx = writer.transactional(tx).await?;
                    writer_tx
                        .write(TopicWriterMessage::new(transition.successor().encode()))
                        .await?;

                    *commit_phase_coordinate = Some(transition.coordinate);
                    Ok(ChainAdvanceOutcome::Committed)
                }
            ))
            .idempotent(true)
            .timeout(timeout)
            .await;
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(error) => match commit_phase_coordinate {
                Some(coordinate) => ChainAdvanceOutcome::CommitPhaseFailure(
                    coordinate,
                    anyhow::Error::new(error).context("execute topic transaction commit phase"),
                ),
                None => classify_transaction_error(error),
            },
        };

        ChainAdvanceReport { attempts, outcome }
    }
}

pub(super) fn reader_options(partition_id: PartitionId, params: &Params) -> TopicReaderOptions {
    let selector = TopicSelector::builder()
        .path(params.topic_path.clone())
        .partition_ids(vec![partition_id.value()])
        .build();
    TopicReaderOptions::builder()
        .consumer(params.consumer_name.clone())
        .topic(selector)
        .batch_size(1)
        .build()
}

/// YDB failures reduce availability but do not stop the long-running SLO.
/// Customer failures originate from `invalid_chain_state` and are fatal because
/// they mean the persisted chain no longer satisfies the workload model.
fn classify_transaction_error(error: YdbOrCustomerError) -> ChainAdvanceOutcome {
    match error {
        YdbOrCustomerError::YDB(error) => ChainAdvanceOutcome::OperationalFailure(
            anyhow::Error::new(error).context("execute topic transaction"),
        ),
        YdbOrCustomerError::Customer(error) => ChainAdvanceOutcome::InvalidChainState(
            anyhow::Error::new(error).context("validate topic transaction state"),
        ),
    }
}

/// Prevents `retry_tx` from retrying a workload-state validation failure.
pub(super) fn invalid_chain_state(error: anyhow::Error) -> YdbOrCustomerError {
    YdbOrCustomerError::from_err(std::io::Error::other(format!("{error:#}")))
}
