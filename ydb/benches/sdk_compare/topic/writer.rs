use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::sleep_until;
use ydb::{
    MessageSkipReason, MessageWriteStatus, PartitioningStrategy, TopicClient, TopicWriter,
    TopicWriterAckFuture, TopicWriterMessage, TopicWriterOptions,
};

use super::{AckLatencyStart, BenchmarkSchedule};
use crate::config::TopicWorkload;
use crate::metrics::LatencyRecorder;
use crate::payload;

#[derive(Clone, Copy)]
struct WriterTaskSettings {
    message_size_bytes: usize,
    max_in_flight: usize,
}

impl WriterTaskSettings {
    fn new(workload: &TopicWorkload) -> Self {
        Self {
            message_size_bytes: workload.message_size_bytes,
            max_in_flight: workload.max_in_flight_per_writer,
        }
    }
}

struct PendingWrite {
    acknowledgement: TopicWriterAckFuture,
    latency_start: AckLatencyStart,
    _concurrency_permit: OwnedSemaphorePermit,
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
    let partition_count = usize::try_from(workload.partition_count)
        .context("partition count does not fit into usize")?;
    let writer_count = partition_count
        .checked_mul(workload.writers_per_partition)
        .context("total writer count overflowed")?;
    let mut writers = Vec::with_capacity(writer_count);

    for partition_id in 0..workload.partition_count {
        for writer_index in 0..workload.writers_per_partition {
            let options = TopicWriterOptions::builder()
                .topic_path(topic_path)
                .producer_id(format!("sdk-compare-writer-{partition_id}-{writer_index}"))
                .partitioning(PartitioningStrategy::PartitionId(i64::from(partition_id)))
                .write_request_messages_chunk_size(workload.write_batch_max_messages)
                .write_request_send_messages_period(Duration::from_millis(
                    workload.write_batch_max_delay_ms,
                ))
                .build();

            let writer = topic_client
                .create_writer_with_params(options)
                .await
                .with_context(|| {
                    format!("failed to create writer {writer_index} for partition {partition_id}")
                })?;

            writers.push(writer);
        }
    }

    Ok(writers)
}

pub(super) async fn run(
    writers: Vec<TopicWriter>,
    schedule: BenchmarkSchedule,
    workload: &TopicWorkload,
) -> Result<WriterMetrics> {
    let settings = WriterTaskSettings::new(workload);
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
    settings: WriterTaskSettings,
) -> Result<WriterMetrics> {
    let write_slots = Arc::new(Semaphore::new(settings.max_in_flight));
    // The semaphore limits submitted writes; the channel only transfers ownership.
    let (pending_writes_tx, mut pending_writes_rx) =
        tokio::sync::mpsc::channel::<PendingWrite>(settings.max_in_flight);

    let ack_recorder_task = tokio::spawn(async move {
        let mut ack_latency = LatencyRecorder::new()?;

        while let Some(pending_write) = pending_writes_rx.recv().await {
            let status = pending_write
                .acknowledgement
                .await
                .context("writer ack failed")?;
            ensure_message_persisted(status)?;

            if let AckLatencyStart::Measured(started_at) = pending_write.latency_start {
                ack_latency.record(started_at.elapsed())?;
            }
        }

        anyhow::Ok(ack_latency)
    });

    // Keep one continuous pipeline across warm-up and measurement.
    loop {
        let concurrency_permit = tokio::select! {
            permit = Arc::clone(&write_slots).acquire_owned() => {
                permit.context("write concurrency limiter was closed")?
            }
            () = sleep_until(schedule.measurement_end.into()) => break,
        };

        // Allocate before starting the latency timer.
        let mut data = payload::allocate(settings.message_size_bytes)?;
        let write_started_at = Instant::now();
        if write_started_at >= schedule.measurement_end {
            break;
        }
        payload::write_timestamp(&mut data, schedule.ns_at(write_started_at)?)?;

        let message = TopicWriterMessage::builder().data(data).build();
        let acknowledgement = writer
            .write_with_ack_future(message)
            .await
            .context("writer submission failed")?;
        let latency_start = if schedule.is_measurement_instant(write_started_at) {
            AckLatencyStart::Measured(write_started_at)
        } else {
            AckLatencyStart::Warmup
        };

        pending_writes_tx
            .try_send(PendingWrite {
                acknowledgement,
                latency_start,
                _concurrency_permit: concurrency_permit,
            })
            .context("failed to hand write to acknowledgement recorder")?;
    }

    // Close the channel so the acknowledgement recorder can finish.
    drop(pending_writes_tx);

    let write_ack = ack_recorder_task
        .await
        .context("write acknowledgement recorder panicked or was cancelled")??;
    let metrics = WriterMetrics { write_ack };

    writer.stop().await.context("failed to stop writer")?;
    Ok(metrics)
}

fn ensure_message_persisted(status: MessageWriteStatus) -> Result<()> {
    match status {
        MessageWriteStatus::Written(_)
        | MessageWriteStatus::Skipped(MessageSkipReason::AlreadyWritten) => Ok(()),
        other => bail!("message was not persisted: server returned {other:?}"),
    }
}
