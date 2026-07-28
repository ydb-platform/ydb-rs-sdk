use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::task::JoinSet;
use tokio::time::timeout_at;
use ydb::{Bytes, QueryClient};

use crate::config::{QueryWorkload, Scenario};
use crate::connection;
use crate::metrics::LatencyRecorder;
use crate::payload::FILL_BYTE;
use crate::result::{BenchmarkResult, QueryMetrics};
use crate::schedule::BenchmarkSchedule;

const QUERY: &str = r#"DECLARE $row_count AS Uint64;
DECLARE $payload AS String;

$rows = SELECT ListFromRange(0ul, $row_count) AS row_ids;

SELECT
    row_id,
    $payload AS payload,
    CAST(row_id AS Double) AS value
FROM $rows
FLATTEN LIST BY row_ids AS row_id;
"#;

pub(crate) async fn run(scenario: &Scenario, workload: &QueryWorkload) -> Result<BenchmarkResult> {
    let client = connection::connect()
        .session_pool_size(workload.concurrent_requests)
        .call()
        .await?;
    let request_payload = Arc::new(vec![FILL_BYTE; workload.payload_size_bytes]);
    let query_clients = (0..workload.concurrent_requests)
        .map(|_| client.query_client())
        .collect::<Vec<_>>();

    let measurement_duration = Duration::from_secs(scenario.execution.measurement_seconds);
    let schedule = BenchmarkSchedule::new(
        Duration::from_secs(scenario.execution.warmup_seconds),
        measurement_duration,
        Duration::from_secs(scenario.execution.drain_timeout_seconds),
    )?;

    let worker_run = run_workers(query_clients, request_payload, schedule, workload);
    let metrics = timeout_at(schedule.completion_deadline.into(), worker_run)
        .await
        .context("Query benchmark did not complete before drain deadline")??;

    let seconds = measurement_duration.as_secs_f64();
    Ok(BenchmarkResult::query(
        scenario.clone(),
        QueryMetrics {
            execute: metrics.execute.summary(),
            queries_per_second: metrics.execute.count() as f64 / seconds,
            rows_per_second: metrics.rows as f64 / seconds,
            payload_bytes_per_second: metrics.payload_bytes as f64 / seconds,
        },
    ))
}

struct WorkerMetrics {
    execute: LatencyRecorder,
    rows: u64,
    payload_bytes: u64,
}

impl WorkerMetrics {
    fn new() -> Result<Self> {
        Ok(Self {
            execute: LatencyRecorder::new()?,
            rows: 0,
            payload_bytes: 0,
        })
    }

    fn record(&mut self, started_at: Instant, execution: QueryExecution) -> Result<()> {
        self.execute.record(started_at.elapsed())?;
        self.rows = self
            .rows
            .checked_add(execution.rows)
            .context("measured Query row count overflowed")?;
        self.payload_bytes = self
            .payload_bytes
            .checked_add(execution.payload_bytes)
            .context("measured Query payload byte count overflowed")?;
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        self.execute.merge(&other.execute)?;
        self.rows = self
            .rows
            .checked_add(other.rows)
            .context("combined Query row count overflowed")?;
        self.payload_bytes = self
            .payload_bytes
            .checked_add(other.payload_bytes)
            .context("combined Query payload byte count overflowed")?;
        Ok(())
    }
}

struct QueryExecution {
    rows: u64,
    payload_bytes: u64,
}

async fn run_workers(
    query_clients: Vec<QueryClient>,
    request_payload: Arc<Vec<u8>>,
    schedule: BenchmarkSchedule,
    workload: &QueryWorkload,
) -> Result<WorkerMetrics> {
    let mut tasks = JoinSet::new();
    for query_client in query_clients {
        tasks.spawn(run_worker(
            query_client,
            Arc::clone(&request_payload),
            schedule,
            workload.row_count,
        ));
    }

    let mut combined = WorkerMetrics::new()?;
    while let Some(joined) = tasks.join_next().await {
        let worker_metrics = match joined {
            Ok(Ok(metrics)) => metrics,
            Ok(Err(error)) => {
                tasks.shutdown().await;
                return Err(error);
            }
            Err(error) => {
                tasks.shutdown().await;
                return Err(error).context("Query worker task panicked or was cancelled");
            }
        };
        if let Err(error) = combined.merge(&worker_metrics) {
            tasks.shutdown().await;
            return Err(error);
        }
    }
    Ok(combined)
}

async fn run_worker(
    mut query_client: QueryClient,
    request_payload: Arc<Vec<u8>>,
    schedule: BenchmarkSchedule,
    row_count: u64,
) -> Result<WorkerMetrics> {
    let mut metrics = WorkerMetrics::new()?;

    // Keep one continuous query loop across warm-up and measurement.
    loop {
        let started_at = Instant::now();
        if started_at >= schedule.measurement_end {
            break;
        }

        let execution =
            execute_query(&mut query_client, request_payload.as_ref(), row_count).await?;
        if schedule.is_measurement_instant(started_at) {
            metrics.record(started_at, execution)?;
        }
    }

    Ok(metrics)
}

async fn execute_query(
    query_client: &mut QueryClient,
    request_payload: &[u8],
    row_count: u64,
) -> Result<QueryExecution> {
    let mut stream = query_client
        .query(QUERY)
        .param("$row_count", row_count)
        .param("$payload", Bytes::from(request_payload.to_vec()))
        .idempotent(true)
        .await
        .context("failed to execute generated-row Query")?;

    let mut rows = 0_u64;
    let mut payload_bytes = 0_u64;

    while let Some(result_set) = stream
        .next_result_set()
        .await
        .context("failed to consume Query result stream")?
    {
        for mut row in result_set {
            let _: u64 = row
                .remove_field_by_name("row_id")
                .context("failed to extract Query column row_id")?
                .try_into()
                .context("failed to decode Query column row_id as u64")?;
            let row_payload: Bytes = row
                .remove_field_by_name("payload")
                .context("failed to extract Query column payload")?
                .try_into()
                .context("failed to decode Query column payload as binary")?;
            let _: f64 = row
                .remove_field_by_name("value")
                .context("failed to extract Query column value")?
                .try_into()
                .context("failed to decode Query column value as f64")?;

            let row_payload: Vec<u8> = row_payload.into();
            let row_payload_bytes = u64::try_from(row_payload.len())
                .context("row payload size does not fit into u64")?;
            rows = rows
                .checked_add(1)
                .context("decoded row count overflowed")?;
            payload_bytes = payload_bytes
                .checked_add(row_payload_bytes)
                .context("decoded payload byte count overflowed")?;
        }
    }

    stream
        .close()
        .await
        .context("failed to close Query result stream")?;

    Ok(QueryExecution {
        rows,
        payload_bytes,
    })
}
