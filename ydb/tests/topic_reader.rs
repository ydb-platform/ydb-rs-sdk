mod mock_server;

use crate::mock_server::FakeTopic;

use std::time::Duration;
use tokio::sync::oneshot::error::TryRecvError;
use tracing_test::traced_test;
use ydb::{TopicReader, TopicReaderBatch, TopicReaderCommitMarker, YdbError, YdbResult};

const DATABASE: &str = "/local";
const TOPIC_PATH: &str = "/local/topic";
const CONSUMER: &str = "consumer";

#[tokio::test]
#[traced_test]
async fn reads_message() -> YdbResult<()> {
    let (mut reader, mut topic) = reader_and_topic().await?;

    read_one(&mut reader, &mut topic, 0, b"hello").await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn commits_message_after_server_ack() -> YdbResult<()> {
    let (mut reader, mut topic) = reader_and_topic().await?;

    let commit_marker = read_one(&mut reader, &mut topic, 0, b"hello").await?;

    let commit = reader.commit(commit_marker);
    topic.ack_next_commit(0, 1).await;

    commit.await.expect("commit handler must be acknowledged");
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn acknowledges_cumulative_commit_offset() -> YdbResult<()> {
    let (mut reader, mut topic) = reader_and_topic().await?;

    let first_marker = read_one(&mut reader, &mut topic, 0, b"first").await?;
    let second_marker = read_one(&mut reader, &mut topic, 1, b"second").await?;

    let first_commit = reader.commit(first_marker);
    let second_commit = reader.commit(second_marker);
    topic.expect_next_commit(0, 1).await;
    topic.expect_next_commit(1, 2).await;
    topic.ack_committed_offset(2).await;

    first_commit
        .await
        .expect("first commit must be covered by cumulative ack");
    second_commit
        .await
        .expect("second commit must be covered by cumulative ack");
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn acknowledges_only_commits_covered_by_server_offset() -> YdbResult<()> {
    let (mut reader, mut topic) = reader_and_topic().await?;

    let first_marker = read_one(&mut reader, &mut topic, 0, b"first").await?;
    let second_marker = read_one(&mut reader, &mut topic, 1, b"second").await?;

    let first_commit = reader.commit(first_marker);
    let mut second_commit = reader.commit(second_marker);
    topic.expect_next_commit(0, 1).await;
    topic.expect_next_commit(1, 2).await;

    topic.ack_committed_offset(1).await;

    first_commit
        .await
        .expect("first commit must be covered by server offset 1");
    assert_eq!(second_commit.try_recv(), Err(TryRecvError::Empty));

    topic.ack_committed_offset(2).await;
    second_commit
        .await
        .expect("second commit must be covered by server offset 2");
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn idempotent_retry() -> YdbResult<()> {
    let (mut reader, mut topic) = reader_and_topic().await?;

    read_one(&mut reader, &mut topic, 0, b"before").await?;

    topic.fail_idempotent().await;
    topic.redeliver_uncommitted().await;

    let batch = reader.read_batch().await?;
    assert_single_message_batch(batch, 0, b"before").await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn reconnects_after_server_restart() -> YdbResult<()> {
    let (mut reader, mut topic) = reader_and_topic().await?;

    read_one(&mut reader, &mut topic, 0, b"before").await?;

    topic.restart_server().await;
    read_one(&mut reader, &mut topic, 1, b"after").await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn discards_buffered_messages_after_stream_failure() -> YdbResult<()> {
    let (mut reader, mut topic) = reader_and_topic().await?;

    topic.deliver(0, b"stale").await;
    topic.fail_retriable().await;
    topic.deliver(0, b"fresh").await;

    let batch = reader.read_batch().await?;
    assert_single_message_batch(batch, 0, b"fresh").await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn retryable_retry() -> YdbResult<()> {
    let (mut reader, mut topic) = reader_and_topic().await?;

    let old_marker = read_one(&mut reader, &mut topic, 0, b"hello").await?;

    let old_commit = reader.commit(old_marker.clone());
    topic.expect_next_commit(0, 1).await;
    topic.fail_retriable().await;
    old_commit
        .await
        .expect_err("pending commit from failed stream must be cancelled");

    topic.redeliver_uncommitted().await;
    let batch = reader.read_batch().await?;
    let new_marker = assert_single_message_batch(batch, 0, b"hello")
        .await?
        .get_commit_marker();

    reader
        .commit(old_marker)
        .await
        .expect_err("commit marker from old reader epoch must be cancelled");

    let new_commit = reader.commit(new_marker);
    topic.ack_next_commit(0, 1).await;
    new_commit
        .await
        .expect("commit from the reconnected stream must be acknowledged");
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn stop_partition_cancels_unacknowledged_commit() -> YdbResult<()> {
    let (mut reader, mut topic) = reader_and_topic().await?;

    let marker = read_one(&mut reader, &mut topic, 0, b"hello").await?;

    let commit = reader.commit(marker);
    topic.expect_next_commit(0, 1).await;
    topic.stop_partition_without_commit().await;

    commit
        .await
        .expect_err("stop partition without committed offset must cancel pending commit");
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn non_retriable_fail() -> YdbResult<()> {
    let (mut reader, mut topic) = reader_and_topic().await?;

    read_one(&mut reader, &mut topic, 0, b"hello").await?;

    topic.fail_non_retriable().await;

    let err = tokio::time::timeout(Duration::from_secs(5), reader.read_batch())
        .await
        .expect("timed out waiting for non-retriable stream error")
        .expect_err("non-retriable stream error must be returned to caller");
    assert_grpc_code(err, tonic::Code::InvalidArgument);
    topic.assert_no_reconnect(Duration::from_millis(200)).await;

    Ok(())
}

async fn reader_and_topic() -> YdbResult<(TopicReader, FakeTopic)> {
    FakeTopic::new(DATABASE, TOPIC_PATH, CONSUMER).await
}

async fn assert_single_message_batch(
    mut batch: TopicReaderBatch,
    offset: i64,
    payload: &[u8],
) -> YdbResult<TopicReaderBatch> {
    assert_eq!(batch.messages.len(), 1);
    assert_eq!(batch.messages[0].offset, offset);
    assert_eq!(batch.messages[0].get_topic(), TOPIC_PATH);
    assert_eq!(batch.messages[0].get_partition_id(), 0);
    assert_eq!(
        batch.messages[0].read_and_take().await?.as_deref(),
        Some(payload)
    );
    Ok(batch)
}

async fn read_one(
    reader: &mut TopicReader,
    topic: &mut FakeTopic,
    offset: i64,
    payload: &[u8],
) -> YdbResult<TopicReaderCommitMarker> {
    topic.deliver(offset, payload).await;
    let batch = reader.read_batch().await?;
    Ok(assert_single_message_batch(batch, offset, payload)
        .await?
        .get_commit_marker())
}

fn assert_grpc_code(err: YdbError, expected_code: tonic::Code) {
    match err {
        YdbError::TransportGRPCStatus(status) => assert_eq!(status.code(), expected_code),
        other => panic!("expected TransportGRPCStatus({expected_code:?}), got {other:?}"),
    }
}
