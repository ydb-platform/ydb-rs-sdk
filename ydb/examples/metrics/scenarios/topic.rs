//! Topic scenario: create a topic, produce messages, consume them with
//! auto-commit, clean up.
//!
//! This is the only scenario that moves the gRPC stream series in both roles
//! that exist today:
//! - `ydb_grpc_stream_messages_total{direction="received"}` — every
//!   `StreamRead` part the reader receives (and the writer's init response);
//! - the writer's sent messages would be
//!   `ydb_grpc_stream_messages_total{direction="sent"}` but are **never
//!   emitted** — a known SDK gap: all stream senders use `clone_sender()`,
//!   bypassing the counting wrapper.
//!
//! Also moves: `ydb_new_topic_client_counter`, `ydb_new_scheme_client_counter`
//! (the topic lives in a created directory), scheme/topic gRPC RPCs in
//! `ydb_grpc_requests_total`, and the session pool series via the unary
//! scheme/topic RPCs.

use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tracing::warn;
use ydb::{
    ConsumerBuilder, CreateTopicOptionsBuilder, TopicWriterMessage, TopicWriterOptions, YdbResult,
};

use crate::SharedClient;
use crate::scenarios::sleep_jittered;

const CONSUMER: &str = "metrics-example-consumer";
const PRODUCER: &str = "metrics-example-producer";
const DIRECTORY: &str = "metrics-example";
const TOPIC: &str = "metrics-topic";
const MESSAGES: usize = 5;
const READ_DEADLINE: Duration = Duration::from_secs(60);
const MIN_INTERVAL: Duration = Duration::from_secs(90);
const MAX_INTERVAL: Duration = Duration::from_secs(180);

pub(crate) async fn run(client: SharedClient, cancel: CancellationToken) {
    loop {
        if !sleep_jittered(&cancel, MIN_INTERVAL, MAX_INTERVAL).await {
            return;
        }
        let client = client.read().await.clone();
        if let Err(err) = tick(&client).await {
            warn!(?err, "topic scenario failed");
        }
    }
}

async fn tick(client: &ydb::Client) -> YdbResult<()> {
    let database = client.database();
    let directory = format!("{database}/{DIRECTORY}");
    let topic_path = format!("{directory}/{TOPIC}");

    let mut scheme_client = client.scheme_client();
    let mut topic_client = client.topic_client();

    // Idempotent cleanup: a previous tick may have died mid-scenario.
    if let Err(err) = topic_client.drop_topic(topic_path.clone()).await {
        tracing::debug!(?err, "pre-cleanup drop_topic ignored");
    }

    // make_directory of an existing directory errors — ignore either way.
    if let Err(err) = scheme_client.make_directory(directory.clone()).await {
        tracing::debug!(?err, "make_directory ignored");
    }

    let consumer = ConsumerBuilder::default()
        .name(CONSUMER.to_string())
        .build()?;
    let options = CreateTopicOptionsBuilder::default()
        .consumers(vec![consumer])
        .build()?;
    topic_client
        .create_topic(topic_path.clone(), options)
        .await?;

    // Produce. Sent stream messages are not counted (known gap, see module docs).
    let writer = topic_client
        .create_writer_with_params(
            TopicWriterOptions::builder()
                .topic_path(topic_path.clone())
                .producer_id(PRODUCER.to_string())
                .build(),
        )
        .await?;
    for i in 0..MESSAGES {
        writer
            .write(
                TopicWriterMessage::builder()
                    .data(format!("message {i}").into_bytes())
                    .build(),
            )
            .await?;
    }
    writer.stop().await?;

    // Consume until every produced message is confirmed, with a deadline.
    let mut reader = topic_client
        .create_reader(CONSUMER, topic_path.clone())
        .await?;
    let deadline = Instant::now() + READ_DEADLINE;
    let mut confirmed = 0_usize;
    while confirmed < MESSAGES {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            warn!(
                confirmed,
                MESSAGES, "topic read deadline reached; leaving scenario early"
            );
            break;
        }
        let batch = tokio::time::timeout(remaining, reader.read_batch())
            .await
            .map_err(|_| ydb::YdbError::Custom("topic read deadline exceeded".into()))??;
        confirmed += batch.messages.len();
        reader.commit(batch.get_commit_marker())?;
    }

    // Cleanup: close the reader (drops its partition sessions), drop the topic.
    drop(reader);
    topic_client.drop_topic(topic_path).await?;
    Ok(())
}
