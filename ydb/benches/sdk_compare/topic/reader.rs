use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::task::JoinSet;
use tokio::time::sleep_until;
use ydb::{
    TopicClient, TopicReader, TopicReaderCommitAckFuture, TopicReaderMessage, TopicReaderOptions,
    TopicSelector,
};

use super::AckLatencyStart;
use crate::config::TopicWorkload;
use crate::metrics::LatencyRecorder;
use crate::payload;
use crate::schedule::BenchmarkSchedule;

pub(super) struct ReaderMetrics {
    pub(super) end_to_end: LatencyRecorder,
    pub(super) commit_ack: LatencyRecorder,
}

struct PendingCommit {
    acknowledgement: TopicReaderCommitAckFuture,
    latency_start: AckLatencyStart,
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

    // Ordered commit acknowledgements must not backpressure message delivery.
    let (pending_commits_tx, mut pending_commits_rx) =
        tokio::sync::mpsc::unbounded_channel::<PendingCommit>();

    let ack_recorder_task = tokio::spawn(async move {
        let mut ack_latency = LatencyRecorder::new()?;

        while let Some(pending_commit) = pending_commits_rx.recv().await {
            pending_commit
                .acknowledgement
                .await
                .context("commit acknowledgement failed")?;

            if let AckLatencyStart::Measured(started_at) = pending_commit.latency_start {
                ack_latency.record(started_at.elapsed())?;
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
        let contains_measured_messages = record_end_to_end_latencies(
            batch.messages,
            delivered_at_ns,
            measurement_start_ns,
            &mut end_to_end,
        )
        .await?;

        let commit_started_at = Instant::now();
        let acknowledgement = reader.commit_with_ack(marker);
        let latency_start = if contains_measured_messages {
            AckLatencyStart::Measured(commit_started_at)
        } else {
            AckLatencyStart::Warmup
        };

        pending_commits_tx
            .send(PendingCommit {
                acknowledgement,
                latency_start,
            })
            .context("commit acknowledgement recorder stopped")?;
    }

    // Close the channel so the acknowledgement recorder can finish.
    drop(pending_commits_tx);

    let commit_ack = ack_recorder_task
        .await
        .context("commit acknowledgement recorder panicked or was cancelled")??;

    Ok(ReaderMetrics {
        end_to_end,
        commit_ack,
    })
}

async fn record_end_to_end_latencies(
    messages: Vec<TopicReaderMessage>,
    delivered_at_ns: u64,
    measurement_start_ns: u64,
    end_to_end: &mut LatencyRecorder,
) -> Result<bool> {
    let mut contains_measured_messages = false;
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

        contains_measured_messages = true;
        let latency_ns = delivered_at_ns
            .checked_sub(sent_at_ns)
            .context("payload timestamp is ahead of the benchmark clock")?;
        end_to_end.record(Duration::from_nanos(latency_ns))?;
    }
    Ok(contains_measured_messages)
}
