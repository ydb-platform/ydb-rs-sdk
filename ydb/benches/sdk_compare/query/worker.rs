use std::time::Instant;

use anyhow::{Context, Result};
use tokio::task::JoinSet;
use ydb::{Bytes, QueryClient, Row, Value, YdbError};

use crate::config::QueryWorkload;
use crate::metrics::LatencyRecorder;
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

pub(super) struct WorkerMetrics {
    pub(super) execute: LatencyRecorder,
    pub(super) rows: u64,
    pub(super) payload_bytes: u64,
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

pub(super) async fn run(
    query_clients: Vec<QueryClient>,
    request_payload: Vec<u8>,
    schedule: BenchmarkSchedule,
    workload: &QueryWorkload,
) -> Result<WorkerMetrics> {
    let mut tasks = JoinSet::new();
    for query_client in query_clients {
        tasks.spawn(run_worker(
            query_client,
            request_payload.clone(),
            schedule,
            workload.row_count,
        ));
    }

    let mut combined = WorkerMetrics::new()?;
    while let Some(joined) = tasks.join_next().await {
        let worker_metrics = joined.context("Query worker task panicked or was cancelled")??;
        combined.merge(&worker_metrics)?;
    }
    Ok(combined)
}

async fn run_worker(
    mut query_client: QueryClient,
    request_payload: Vec<u8>,
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

        let execution = execute_query(&mut query_client, &request_payload, row_count).await?;
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
            let _: u64 = decode_field(&mut row, "row_id")?;
            let row_payload: Vec<u8> = decode_field::<Bytes>(&mut row, "payload")?.into();
            let _: f64 = decode_field(&mut row, "value")?;

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

fn decode_field<T>(row: &mut Row, name: &'static str) -> Result<T>
where
    T: TryFrom<Value, Error = YdbError>,
{
    row.remove_field_by_name(name)
        .and_then(T::try_from)
        .with_context(|| format!("failed to decode Query column {name}"))
}
