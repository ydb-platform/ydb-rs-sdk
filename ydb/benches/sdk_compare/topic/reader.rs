use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::task::JoinSet;
use tokio::time::sleep_until;
use ydb::{TopicClient, TopicReader, TopicReaderMessage, TopicReaderOptions, TopicSelector};

use super::BenchmarkSchedule;
use crate::config::TopicWorkload;
use crate::metrics::LatencyRecorder;
use crate::payload;

pub(super) struct ReaderMetrics {
    pub(super) end_to_end: LatencyRecorder,
    pub(super) commit_ack: LatencyRecorder,
}

impl ReaderMetrics {
    fn new() -> Result<Self> {
        Ok(Self {
            end_to_end: LatencyRecorder::new()?,
            commit_ack: LatencyRecorder::new()?,
        })
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        self.end_to_end.merge(&other.end_to_end)?;
        self.commit_ack.merge(&other.commit_ack)?;
        Ok(())
    }

    fn record_commit(&mut self, latency: Option<Duration>) -> Result<()> {
        if let Some(latency) = latency {
            self.commit_ack.record(latency)?;
        }
        Ok(())
    }
}

pub(super) async fn open(
    topic_client: &mut TopicClient,
    topic_path: &str,
    workload: &TopicWorkload,
) -> Result<Vec<TopicReader>> {
    let mut readers = Vec::with_capacity(workload.reader_count);
    for reader_id in 0..workload.reader_count {
        let options = TopicReaderOptions::builder()
            .consumer(workload.consumer_name.clone())
            .topic(TopicSelector::new(topic_path))
            .build();
        let reader = topic_client
            .create_reader_with_params(options)
            .await
            .with_context(|| format!("failed to create reader {reader_id}"))?;
        readers.push(reader);
    }
    Ok(readers)
}

pub(super) async fn run(
    readers: Vec<TopicReader>,
    schedule: BenchmarkSchedule,
) -> Result<ReaderMetrics> {
    let mut tasks = JoinSet::new();
    for reader in readers {
        tasks.spawn(run_reader(reader, schedule));
    }

    let mut combined_metrics = ReaderMetrics::new()?;
    while let Some(joined) = tasks.join_next().await {
        let worker_metrics = joined.context("reader task panicked or was cancelled")??;
        combined_metrics.merge(&worker_metrics)?;
    }

    Ok(combined_metrics)
}

async fn run_reader(mut reader: TopicReader, schedule: BenchmarkSchedule) -> Result<ReaderMetrics> {
    let mut metrics = ReaderMetrics::new()?;
    let mut commits = FuturesUnordered::new();
    let measurement_start_ns = schedule.ns_at(schedule.measurement_start)?;
    let measurement_end = schedule.measurement_end;

    loop {
        // Keep reading while completed commit acknowledgements are recorded in parallel.
        let batch = tokio::select! {
            () = sleep_until(measurement_end.into()) => break,
            Some(completion) = commits.next(), if !commits.is_empty() => {
                metrics.record_commit(completion?)?;
                continue;
            }
            batch = reader.read_batch() => batch.context("reader failed")?,
        };

        let delivered_at_ns = schedule.now_ns()?;
        let marker = batch.get_commit_marker();
        let measured_batch = process_batch(
            batch.messages,
            delivered_at_ns,
            measurement_start_ns,
            &mut metrics,
        )
        .await?;

        let started = Instant::now();
        let ack = reader.commit_with_ack(marker);

        commits.push(async move {
            ack.await.context("commit acknowledgement failed")?;
            anyhow::Ok(measured_batch.then(|| started.elapsed()))
        });
    }

    // Stop reading at the measurement boundary, then finish commits already started.
    while let Some(completion) = commits.next().await {
        metrics.record_commit(completion?)?;
    }
    Ok(metrics)
}

async fn process_batch(
    messages: Vec<TopicReaderMessage>,
    delivered_at_ns: u64,
    measurement_start_ns: u64,
    metrics: &mut ReaderMetrics,
) -> Result<bool> {
    let mut measured_batch = false;
    for mut message in messages {
        let data = message
            .read_and_take()
            .await?
            .context("reader message has no payload")?;
        let sent_at_ns =
            payload::read_timestamp(&data).context("failed to decode benchmark payload")?;
        if sent_at_ns < measurement_start_ns {
            continue;
        }

        measured_batch = true;
        let latency_ns = delivered_at_ns
            .checked_sub(sent_at_ns)
            .context("payload timestamp is ahead of the benchmark clock")?;
        metrics
            .end_to_end
            .record(Duration::from_nanos(latency_ns))?;
    }
    Ok(measured_batch)
}
