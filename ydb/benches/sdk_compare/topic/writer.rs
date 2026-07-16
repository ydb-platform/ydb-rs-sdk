use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::task::JoinSet;
use ydb::{PartitioningStrategy, TopicClient, TopicWriter, TopicWriterMessage, TopicWriterOptions};

use super::BenchmarkSchedule;
use crate::config::TopicWorkload;
use crate::metrics::LatencyRecorder;
use crate::payload;

#[derive(Clone, Copy)]
struct WriterSettings {
    message_size_bytes: usize,
    max_in_flight: usize,
}

impl WriterSettings {
    fn new(workload: &TopicWorkload) -> Self {
        Self {
            message_size_bytes: workload.message_size_bytes,
            max_in_flight: workload.max_in_flight_per_writer,
        }
    }
}

pub(super) struct WriterMetrics {
    pub(super) write_ack: LatencyRecorder,
}

impl WriterMetrics {
    fn new() -> Result<Self> {
        Ok(Self {
            write_ack: LatencyRecorder::new()?,
        })
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        self.write_ack.merge(&other.write_ack)?;
        Ok(())
    }

    fn record_acknowledgement(&mut self, latency: Option<Duration>) -> Result<()> {
        if let Some(latency) = latency {
            self.write_ack.record(latency)?;
        }
        Ok(())
    }
}

pub(super) async fn open(
    topic_client: &mut TopicClient,
    topic_path: &str,
    workload: &TopicWorkload,
) -> Result<Vec<TopicWriter>> {
    let mut writers = Vec::with_capacity(workload.writer_count);

    for writer_id in 0..workload.writer_count {
        let options = TopicWriterOptions::builder()
            .topic_path(topic_path)
            .producer_id(format!("sdk-compare-writer-{writer_id}"))
            .partitioning(PartitioningStrategy::ByProducerId)
            .build();

        let writer = topic_client
            .create_writer_with_params(options)
            .await
            .with_context(|| format!("failed to create writer {writer_id}"))?;

        writers.push(writer);
    }

    Ok(writers)
}

pub(super) async fn run(
    writers: Vec<TopicWriter>,
    schedule: BenchmarkSchedule,
    workload: &TopicWorkload,
) -> Result<WriterMetrics> {
    let settings = WriterSettings::new(workload);
    let mut tasks = JoinSet::new();
    for writer in writers {
        tasks.spawn(run_writer(writer, schedule, settings));
    }

    let mut combined_metrics = WriterMetrics::new()?;
    while let Some(joined) = tasks.join_next().await {
        let worker_metrics = joined.context("writer task panicked or was cancelled")??;
        combined_metrics.merge(&worker_metrics)?;
    }

    Ok(combined_metrics)
}

async fn run_writer(
    writer: TopicWriter,
    schedule: BenchmarkSchedule,
    settings: WriterSettings,
) -> Result<WriterMetrics> {
    let mut in_flight = FuturesUnordered::new();
    let mut metrics = WriterMetrics::new()?;

    // Keep one continuous pipeline across warm-up and measurement.
    loop {
        if in_flight.len() < settings.max_in_flight {
            let Some(message) = prepare_next_message(&schedule, settings.message_size_bytes)?
            else {
                break;
            };
            let ack = writer
                .write_with_ack_future(message.message)
                .await
                .context("writer submission failed")?;
            let measured = schedule.is_measurement_instant(message.started);
            in_flight.push(async move {
                ack.await.context("writer acknowledgement failed")?;
                anyhow::Ok(measured.then(|| message.started.elapsed()))
            });
        } else {
            let completion = in_flight
                .next()
                .await
                .context("writer acknowledgement stream ended")?;
            metrics.record_acknowledgement(completion?)?;
        }
    }

    // Stop submitting at the measurement boundary, then finish acknowledgements already started.
    while let Some(completion) = in_flight.next().await {
        metrics.record_acknowledgement(completion?)?;
    }

    writer.stop().await.context("failed to stop writer")?;
    Ok(metrics)
}

fn prepare_next_message(
    schedule: &BenchmarkSchedule,
    message_size_bytes: usize,
) -> Result<Option<PreparedMessage>> {
    // Allocate before starting the latency timer, then check that submissions remain open.
    let mut data = payload::allocate(message_size_bytes)?;
    let started = Instant::now();
    if started >= schedule.measurement_end {
        return Ok(None);
    }
    payload::write_timestamp(&mut data, schedule.ns_at(started)?)?;

    Ok(Some(PreparedMessage {
        message: TopicWriterMessage::builder().data(data).build(),
        started,
    }))
}

struct PreparedMessage {
    message: TopicWriterMessage,
    started: Instant,
}
