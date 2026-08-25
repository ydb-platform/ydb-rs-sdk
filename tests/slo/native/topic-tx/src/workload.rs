use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use slo_framework::topic_tx::PartitionId;
use slo_framework::{Framework, Logger, Metrics, Workload, preserve_primary_error};

use crate::storage::{
    ERROR_COMMIT_PHASE_FAILURE, ERROR_OPERATIONAL_FAILURE, OPERATION_TRANSACTION, TopicTxStorage,
};

pub(super) struct TopicTxWorkload {
    metrics: Metrics,
    logger: Arc<Logger>,
    storage: TopicTxStorage,
}

impl TopicTxWorkload {
    fn new(metrics: Metrics, logger: Arc<Logger>, storage: TopicTxStorage) -> Self {
        Self {
            metrics,
            logger,
            storage,
        }
    }
}

#[async_trait]
impl Workload for TopicTxWorkload {
    async fn setup(&self, _ctx: &CancellationToken) -> Result<()> {
        self.storage
            .setup_resources()
            .await
            .context("setup topic transaction resources")
    }

    async fn run(&self, ctx: &CancellationToken) -> Result<()> {
        let workers = self
            .storage
            .open_workers()
            .await
            .context("open topic transaction workers")?;
        let worker_cancel = ctx.child_token();
        let mut worker_tasks = JoinSet::new();
        for worker in workers {
            worker_tasks.spawn(worker.run(
                worker_cancel.clone(),
                self.metrics.clone(),
                self.logger.clone(),
            ));
        }

        let run_result = monitor_workers(ctx, &mut worker_tasks).await;
        worker_cancel.cancel();
        let shutdown_result = join_workers(&mut worker_tasks).await;
        let run_result = preserve_primary_error(run_result, shutdown_result);
        sleep(Duration::from_secs(1)).await;
        preserve_primary_error(run_result, self.storage.verify_shutdown_state().await)
    }

    async fn teardown(&self, _ctx: &CancellationToken) -> Result<()> {
        self.storage
            .cleanup_resources()
            .await
            .context("cleanup topic transaction resources")
    }
}

pub(super) async fn new_workload(
    framework: Framework,
) -> std::result::Result<Box<dyn Workload>, String> {
    framework
        .metrics
        .initialize_operation_series(OPERATION_TRANSACTION);
    framework
        .metrics
        .initialize_error_series(OPERATION_TRANSACTION, ERROR_COMMIT_PHASE_FAILURE);
    framework
        .metrics
        .initialize_error_series(OPERATION_TRANSACTION, ERROR_OPERATIONAL_FAILURE);
    let params = slo_framework::topic_tx::parse_params(&framework);
    let storage = TopicTxStorage::connect(&framework, params)
        .await
        .map_err(|error| format!("{error:#}"))?;
    Ok(Box::new(TopicTxWorkload::new(
        framework.metrics,
        framework.logger,
        storage,
    )))
}

/// Keeps the run alive until cancellation; an earlier worker exit is a failure.
async fn monitor_workers(
    ctx: &CancellationToken,
    worker_tasks: &mut JoinSet<Result<PartitionId>>,
) -> Result<()> {
    tokio::select! {
        biased;
        _ = ctx.cancelled() => Ok(()),
        worker_result = worker_tasks.join_next() => {
            let worker_result = worker_result.context("partition worker set is empty")?;
            let partition_id = worker_result
                .context("partition worker panicked")?
                .context("partition worker failed")?;
            bail!("partition worker {partition_id} stopped before workload cancellation")
        }
    }
}

async fn join_workers(worker_tasks: &mut JoinSet<Result<PartitionId>>) -> Result<()> {
    let mut result = Ok(());
    while let Some(task) = worker_tasks.join_next().await {
        let worker_result = task
            .context("partition worker panicked")
            .and_then(|result| result.map(|_| ()));
        result = preserve_primary_error(result, worker_result);
    }
    result.context("partition workers failed during shutdown")
}
