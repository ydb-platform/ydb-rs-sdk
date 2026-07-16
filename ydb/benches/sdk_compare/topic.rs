mod reader;
mod writer;

use std::env;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::time::timeout_at;
use ydb::{Client, ClientBuilder, Codec, ConsumerBuilder, CreateTopicOptionsBuilder, TopicClient};

use crate::config::{Scenario, TopicWorkload};
use crate::result::{BenchmarkResult, TopicMetrics};

const CONNECTION_STRING_ENV: &str = "YDB_CONNECTION_STRING";

pub(crate) async fn run(scenario: &Scenario, workload: &TopicWorkload) -> Result<BenchmarkResult> {
    let client = connect().await?;
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

async fn connect() -> Result<Client> {
    let connection_string = env::var(CONNECTION_STRING_ENV)
        .with_context(|| format!("{CONNECTION_STRING_ENV} is not set"))?;

    let client = ClientBuilder::new_from_connection_string(&connection_string)
        .context("failed to parse YDB connection string")?
        .client()
        .context("failed to create YDB client")?;

    client
        .wait()
        .await
        .context("failed to initialize YDB client")?;

    Ok(client)
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
    let drain_timeout = Duration::from_secs(scenario.execution.drain_timeout_seconds);
    let measurement_duration = Duration::from_secs(scenario.execution.measurement_seconds);

    // Open every SDK session before the benchmark clock starts.
    let writers = writer::open(topic_client, topic_path, workload).await?;
    let readers = reader::open(topic_client, topic_path, workload).await?;
    let schedule = BenchmarkSchedule::new(
        Duration::from_secs(scenario.execution.warmup_seconds),
        measurement_duration,
        drain_timeout,
    )?;

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

    let seconds = measurement_duration.as_secs_f64();
    let message_size = workload.message_size_bytes as f64;
    let write_messages_per_second = writer_metrics.write_ack.count() as f64 / seconds;
    let read_messages_per_second = reader_metrics.end_to_end.count() as f64 / seconds;

    Ok(BenchmarkResult::new(
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
pub(super) struct BenchmarkSchedule {
    origin: Instant,
    pub(super) measurement_start: Instant,
    pub(super) measurement_end: Instant,
    pub(super) completion_deadline: Instant,
}

impl BenchmarkSchedule {
    fn new(
        warmup_duration: Duration,
        measurement_duration: Duration,
        drain_timeout: Duration,
    ) -> Result<Self> {
        let origin = Instant::now();
        let measurement_start = origin
            .checked_add(warmup_duration)
            .context("warm-up deadline overflowed")?;
        let measurement_end = measurement_start
            .checked_add(measurement_duration)
            .context("measurement deadline overflowed")?;
        let completion_deadline = measurement_end
            .checked_add(drain_timeout)
            .context("measurement drain deadline overflowed")?;

        Ok(Self {
            origin,
            measurement_start,
            measurement_end,
            completion_deadline,
        })
    }

    pub(super) fn is_measurement_instant(&self, instant: Instant) -> bool {
        instant >= self.measurement_start && instant < self.measurement_end
    }

    pub(super) fn ns_at(&self, instant: Instant) -> Result<u64> {
        let elapsed = instant
            .checked_duration_since(self.origin)
            .context("benchmark instant is before schedule origin")?;
        u64::try_from(elapsed.as_nanos())
            .context("benchmark schedule does not fit into u64 nanoseconds")
    }

    pub(super) fn now_ns(&self) -> Result<u64> {
        self.ns_at(Instant::now())
    }
}
