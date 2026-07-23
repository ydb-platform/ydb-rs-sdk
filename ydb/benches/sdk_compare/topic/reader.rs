use std::time::{Duration, Instant};

use anyhow::{Context, Result};
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
    let measurement_start_ns = schedule.ns_at(schedule.measurement_start)?;
    let measurement_end = schedule.measurement_end;

    let (pending_acks_tx, mut pending_acks_rx) = tokio::sync::mpsc::unbounded_channel();

    let ack_recorder_task = tokio::spawn(async move {
        let mut ack_latency = LatencyRecorder::new()?;

        while let Some(pending_ack) = pending_acks_rx.recv().await {
            if let Some(elapsed) = pending_ack.await? {
                ack_latency.record(elapsed)?;
            }
        }

        anyhow::Ok(ack_latency)
    });

    let mut end_to_end = LatencyRecorder::new()?;

    loop {
        // Keep reading while completed commit acknowledgements are recorded in parallel.
        let batch = tokio::select! {
            () = sleep_until(measurement_end.into()) => break,
            batch = reader.read_batch() => batch.context("reader failed")?,
        };

        let delivered_at_ns = schedule.now_ns()?;
        let marker = batch.get_commit_marker();
        let measured_batch = process_batch(
            batch.messages,
            delivered_at_ns,
            measurement_start_ns,
            &mut end_to_end,
        )
        .await?;

        let started = Instant::now();
        let ack = reader.commit_with_ack(marker);

        pending_acks_tx.send(async move {
            ack.await?;
            anyhow::Ok(measured_batch.then_some(started.elapsed()))
        })?;
    }

    // Close the channel so the acknowledgement recorder can finish.
    drop(pending_acks_tx);

    let commit_ack = ack_recorder_task
        .await
        .context("commit acknowledgement recorder panicked or was cancelled")??;

    Ok(ReaderMetrics {
        end_to_end,
        commit_ack,
    })
}

async fn process_batch(
    messages: Vec<TopicReaderMessage>,
    delivered_at_ns: u64,
    measurement_start_ns: u64,
    end_to_end: &mut LatencyRecorder,
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
        end_to_end.record(Duration::from_nanos(latency_ns))?;
    }
    Ok(measured_batch)
}
