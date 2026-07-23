use std::time::Instant;

use anyhow::{Context, Result};
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
    let (pending_acks_tx, mut pending_acks_rx) = tokio::sync::mpsc::channel(settings.max_in_flight);

    let ack_recorder_task = tokio::spawn(async move {
        let mut ack_latency = LatencyRecorder::new()?;

        while let Some(pending_ack) = pending_acks_rx.recv().await {
            if let Some(elapsed) = pending_ack.await? {
                ack_latency.record(elapsed)?;
            }
        }

        anyhow::Ok(ack_latency)
    });

    // Keep one continuous pipeline across warm-up and measurement.
    loop {
        // Allocate before starting the latency timer.
        let mut data = payload::allocate(settings.message_size_bytes)?;
        let started = Instant::now();
        if started >= schedule.measurement_end {
            break;
        }
        payload::write_timestamp(&mut data, schedule.ns_at(started)?)?;

        let message = TopicWriterMessage::builder().data(data).build();
        let ack = writer
            .write_with_ack_future(message)
            .await
            .context("writer submission failed")?;
        let measured = schedule.is_measurement_instant(started);

        pending_acks_tx
            .send(async move {
                ack.await.context("writer ack failed")?;
                anyhow::Ok(measured.then_some(started.elapsed()))
            })
            .await?;
    }

    // Close the channel so the acknowledgement recorder can finish.
    drop(pending_acks_tx);

    let metrics = WriterMetrics {
        write_ack: ack_recorder_task.await??,
    };

    writer.stop().await.context("failed to stop writer")?;
    Ok(metrics)
}
