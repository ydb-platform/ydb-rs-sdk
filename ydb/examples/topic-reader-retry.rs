use std::{collections::HashSet, time::Duration};
use tokio::time::timeout;
use tracing::info;
use ydb::{ClientBuilder, TopicReaderMessage, YdbError, YdbResult};

const READ_CYCLES: usize = 5;
const SLOW: Duration = Duration::from_secs(20);
const FAST: Duration = Duration::from_secs(1);

type MessageSeqNos = HashSet<i64>;

fn mark_batch_read(
    batch_messages: &[TopicReaderMessage],
    read: &mut MessageSeqNos,
    committed: &MessageSeqNos,
) {
    info!("Process messages");
    for TopicReaderMessage { seq_no, .. } in batch_messages.iter() {
        assert!(!committed.contains(seq_no));
        assert!(read.insert(*seq_no));
    }
}

fn mark_batch_commit_result(
    batch_messages: &[TopicReaderMessage],
    read: &mut MessageSeqNos,
    committed: &mut MessageSeqNos,
    is_committed: bool,
) {
    for TopicReaderMessage { seq_no, .. } in batch_messages.iter() {
        if is_committed {
            assert!(committed.insert(*seq_no));
        } else {
            read.remove(seq_no);
        }
    }
}

#[tokio::main]
async fn main() -> YdbResult<()> {
    tracing_subscriber::fmt().init();

    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
        .client()?;

    if let Ok(res) = timeout(Duration::from_secs(3), client.wait()).await {
        res?
    } else {
        return Err(YdbError::from("Connection timeout"));
    };

    info!("Connected to YDB");

    let mut topic_client = client.topic_client();

    let mut reader = topic_client
        .create_reader("test".into(), "/local/topic")
        .await?;

    let mut read = MessageSeqNos::new();
    let mut committed = MessageSeqNos::new();

    info!("Read -> Wait -> Commit");
    for iter in 0..READ_CYCLES {
        let batch = reader.read_batch().await?;
        info!(?batch, iter);

        mark_batch_read(&batch.messages, &mut read, &committed);

        info!(?SLOW, "Sleep!");
        tokio::time::sleep(SLOW).await;

        info!("Commit!");
        let commit_handler = reader.commit(batch.get_commit_marker());

        info!(?SLOW, "Sleep!");
        tokio::time::sleep(SLOW).await;

        let is_committed = match tokio::time::timeout(FAST, commit_handler).await {
            Ok(result) => {
                info!(?result, "commit result");
                result.is_ok()
            }
            Err(_) => {
                info!("Commit no response");
                false
            }
        };

        mark_batch_commit_result(&batch.messages, &mut read, &mut committed, is_committed);
    }

    //info!("Read -> Commit -> Wait");
    //for iter in 0..READ_CYCLES {
    //    let batch = reader.read_batch().await?;
    //    info!(?batch, iter);

    //    mark_batch_read(&batch.messages, &mut read, &committed);

    //    info!("Commit!");
    //    let commit_handler = reader.commit(batch.get_commit_marker());

    //    info!(?SLOW, "Sleep!");
    //    tokio::time::sleep(SLOW).await;

    //    let is_committed = match tokio::time::timeout(FAST, commit_handler).await {
    //        Ok(result) => {
    //            info!(?result, "commit result");
    //            result.is_ok()
    //        }
    //        Err(_) => {
    //            info!("Commit no response");
    //            false
    //        }
    //    };

    //    mark_batch_commit_result(&batch.messages, &mut read, &mut committed, is_committed);
    //}

    Ok(())
}
