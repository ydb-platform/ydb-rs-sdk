mod reader;
mod writer;

use std::time::Instant;

use anyhow::{Context, Result};
use tokio::time::timeout_at;
use ydb::{Codec, ConsumerBuilder, CreateTopicOptionsBuilder, TopicClient};

use crate::config::{Scenario, TopicWorkload};
use crate::connection;
use crate::result::{BenchmarkResult, TopicMetrics};
use crate::schedule::BenchmarkSchedule;

pub(crate) async fn run(scenario: &Scenario, workload: &TopicWorkload) -> Result<BenchmarkResult> {
    let client = connection::connect().call().await?;
    let topic_path = format!(
        "{}/{}",
        client.database().trim_end_matches('/'),
        workload.topic_name
    );
    let mut topic_client = client.topic_client();
    create_topic(&mut topic_client, &topic_path, workload).await?;

    let result = run_workload(&mut topic_client, &topic_path, scenario, workload).await;
    if let Err(error) = topic_client.drop_topic(topic_path.clone()).await {
        eprintln!("warning: failed to drop benchmark topic {topic_path}: {error}");
    }
    result
}

async fn create_topic(
    topic_client: &mut TopicClient,
    topic_path: &str,
    workload: &TopicWorkload,
) -> Result<()> {
    let partition_count = i64::from(workload.partition_count);
    let consumer = ConsumerBuilder::default()
        .name(workload.consumer_name.clone())
        .important(true)
        .build()
        .context("failed to build benchmark consumer")?;

    let options = CreateTopicOptionsBuilder::default()
        .min_active_partitions(partition_count)
        .partition_count_limit(partition_count)
        .supported_codecs(vec![Codec::RAW])
        .partition_write_speed_bytes_per_second(workload.partition_write_speed_bytes_per_second)
        .consumers(vec![consumer])
        .build()
        .context("failed to build topic options")?;

    topic_client
        .create_topic(topic_path.to_owned(), options)
        .await
        .with_context(|| format!("failed to create topic {topic_path}"))
}

async fn run_workload(
    topic_client: &mut TopicClient,
    topic_path: &str,
    scenario: &Scenario,
    workload: &TopicWorkload,
) -> Result<BenchmarkResult> {
    // Open every SDK session before the benchmark clock starts.
    let writers = writer::open(topic_client, topic_path, workload).await?;
    let readers = reader::open(topic_client, topic_path, workload).await?;
    let schedule = BenchmarkSchedule::from_execution(&scenario.execution)?;

    // Run readers and writers continuously across the warm-up/measurement boundary.
    let worker_run = async {
        tokio::try_join!(
            writer::run(writers, schedule, workload),
            reader::run(readers, schedule),
        )
    };
    let (writer_metrics, reader_metrics) =
        timeout_at(schedule.completion_deadline.into(), worker_run)
            .await
            .context("benchmark drain timed out")??;

    let measurement_seconds = schedule.measurement_seconds();
    let message_size = workload.message_size_bytes as f64;
    let write_messages_per_second = writer_metrics.write_ack.count() as f64 / measurement_seconds;
    let read_messages_per_second = reader_metrics.end_to_end.count() as f64 / measurement_seconds;

    Ok(BenchmarkResult::topic(
        scenario.clone(),
        TopicMetrics {
            write_ack: writer_metrics.write_ack.summary(),
            end_to_end: reader_metrics.end_to_end.summary(),
            commit_ack: reader_metrics.commit_ack.summary(),
            write_messages_per_second,
            write_bytes_per_second: write_messages_per_second * message_size,
            read_messages_per_second,
            read_bytes_per_second: read_messages_per_second * message_size,
        },
    ))
}

#[derive(Clone, Copy)]
pub(super) enum AckLatencyStart {
    Warmup,
    Measured(Instant),
}
