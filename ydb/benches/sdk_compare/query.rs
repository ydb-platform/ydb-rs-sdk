mod worker;

use anyhow::{Context, Result};
use tokio::time::timeout_at;

use crate::config::{QueryWorkload, Scenario};
use crate::connection;
use crate::payload::FILL_BYTE;
use crate::result::{BenchmarkResult, QueryMetrics};
use crate::schedule::BenchmarkSchedule;

pub(crate) async fn run(scenario: &Scenario, workload: &QueryWorkload) -> Result<BenchmarkResult> {
    let client = connection::connect()
        .session_pool_size(workload.concurrent_requests)
        .call()
        .await?;
    let request_payload = vec![FILL_BYTE; workload.payload_size_bytes];
    let query_clients = (0..workload.concurrent_requests)
        .map(|_| client.query_client())
        .collect::<Vec<_>>();

    let schedule = BenchmarkSchedule::from_execution(&scenario.execution)?;

    let worker_run = worker::run(query_clients, request_payload, schedule, workload);
    let metrics = timeout_at(schedule.completion_deadline.into(), worker_run)
        .await
        .context("Query benchmark did not complete before drain deadline")??;

    let measurement_seconds = schedule.measurement_seconds();
    Ok(BenchmarkResult::query(
        scenario.clone(),
        QueryMetrics {
            execute: metrics.execute.summary(),
            queries_per_second: metrics.execute.count() as f64 / measurement_seconds,
            rows_per_second: metrics.rows as f64 / measurement_seconds,
            payload_bytes_per_second: metrics.payload_bytes as f64 / measurement_seconds,
        },
    ))
}
