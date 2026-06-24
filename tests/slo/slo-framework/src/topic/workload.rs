use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::framework::Workload;
use crate::helpers::{new_rate_limiter, run_workers_for};
use crate::metrics::{OPERATION_READ, OPERATION_WRITE};
use crate::Framework;

use super::{verification, Params, Topic};

pub struct TopicWorkload<T: Topic> {
    fw: Arc<Framework>,
    topic: Arc<T>,
    params: Params,
}

impl<T: Topic + 'static> TopicWorkload<T> {
    pub fn new(fw: Framework, params: Params, topic: T) -> Self {
        Self {
            fw: Arc::new(fw),
            topic: Arc::new(topic),
            params,
        }
    }
}

#[async_trait::async_trait]
impl<T: Topic + 'static> Workload for TopicWorkload<T> {
    async fn setup(&self, _ctx: &CancellationToken) -> Result<(), String> {
        self.topic.create_topic().await?;
        self.fw.logger.printf("create topic ok");

        Ok(())
    }

    async fn run(&self, ctx: &CancellationToken) -> Result<(), String> {
        let writers = self.topic.open_writers().await?;
        let readers = self.topic.open_readers().await?;

        self.fw.logger.printf(format!(
            "opened {} writer(s), {} reader(s)",
            writers.len(),
            readers.len()
        ));

        let write_handle = spawn_writer_workers(
            ctx.clone(),
            self.fw.clone(),
            writers,
            self.params.write_rps,
            self.params.write_timeout,
        );

        let read_handle = spawn_reader_workers(
            ctx.clone(),
            self.fw.clone(),
            readers,
            self.params.read_rps,
            self.params.read_timeout,
            self.params.commit_delay,
        );

        let _ = tokio::join!(write_handle, read_handle);

        Ok(())
    }

    async fn teardown(&self, _ctx: &CancellationToken) -> Result<(), String> {
        let drop_result = self.topic.drop_topic().await;
        let _ = self.topic.close().await;

        drop_result?;
        self.fw.logger.printf("cleanup topic ok");

        Ok(())
    }
}

fn spawn_writer_workers(
    ctx: CancellationToken,
    fw: Arc<Framework>,
    writers: Vec<ydb::TopicWriter>,
    rps: u32,
    timeout: Duration,
) -> tokio::task::JoinHandle<()> {
    let limiter = Arc::new(new_rate_limiter(rps));

    tokio::spawn(run_workers_for(writers.into_iter().map(move |writer| {
        let ctx = ctx.clone();
        let fw = fw.clone();
        let limiter = limiter.clone();

        move || async move {
            let mut seq_no: i64 = 1;

            while !ctx.is_cancelled() {
                if let Err(wait) = limiter.try_wait() {
                    tokio::time::sleep(wait).await;
                    continue;
                }

                let payload = format!("{seq_no}").into_bytes();
                seq_no = seq_no.wrapping_add(1);

                let message = match ydb::TopicWriterMessageBuilder::default()
                    .data(payload)
                    .build()
                {
                    Ok(m) => m,
                    Err(err) => {
                        fw.logger.errorf(format!("build message failed: {err}"));
                        continue;
                    }
                };

                let span = fw.metrics.start(OPERATION_WRITE);
                let result = tokio::time::timeout(timeout, writer.write(message)).await;

                match result {
                    Ok(Ok(())) => span.finish(None, 1),
                    Ok(Err(err)) => {
                        let msg = err.to_string();
                        span.finish(Some(&msg), 1);
                        fw.logger.errorf(format!("write failed: {msg}"));
                    }
                    Err(_) => {
                        span.finish(Some("write timeout"), 1);
                        fw.logger.errorf("write failed: timeout");
                    }
                }
            }
        }
    })))
}

fn spawn_reader_workers(
    ctx: CancellationToken,
    fw: Arc<Framework>,
    readers: Vec<ydb::TopicReader>,
    rps: u32,
    timeout: Duration,
    commit_delay: Duration,
) -> tokio::task::JoinHandle<()> {
    let limiter = Arc::new(new_rate_limiter(rps));
    let messages_order = Arc::new(verification::MessagesOrder::default());
    let offsets_order = Arc::new(verification::OffsetOrder::default());

    tokio::spawn(run_workers_for(readers.into_iter().map(move |reader| {
        let ctx = ctx.clone();
        let fw = fw.clone();
        let limiter = limiter.clone();

        let reader = Arc::new(tokio::sync::Mutex::new(reader));
        let messages_order = messages_order.clone();
        let offset_order = offsets_order.clone();

        move || async move {
            while !ctx.is_cancelled() {
                if let Err(wait) = limiter.try_wait() {
                    tokio::time::sleep(wait).await;
                    continue;
                }

                let span = fw.metrics.start(OPERATION_READ);
                let result = tokio::time::timeout(timeout, reader.lock().await.read_batch()).await;

                match result {
                    Ok(Ok(mut batch)) => {
                        if let Err(err) =
                            process_batch(&messages_order, &offset_order, &mut batch).await
                        {
                            fw.logger.errorf(format!("invariant violated: {err}"));
                            span.finish(Some(&err), 1);
                            continue;
                        }

                        spawn_commit(
                            fw.clone(),
                            reader.clone(),
                            offset_order.clone(),
                            batch.partition_id(),
                            batch.offset(),
                            batch.get_commit_marker(),
                            commit_delay,
                        );

                        span.finish(None, 1)
                    }
                    Ok(Err(err)) => {
                        let msg = err.to_string();
                        span.finish(Some(&msg), 1);
                        fw.logger.errorf(format!("read failed: {msg}"));
                    }
                    Err(_) => {
                        span.cancel();
                        fw.logger.printf("read failed: timeout");
                    }
                }
            }
        }
    })))
}

async fn process_batch(
    messages_order: &verification::MessagesOrder,
    offset_order: &verification::OffsetOrder,
    batch: &mut ydb::TopicReaderBatch,
) -> Result<(), String> {
    for message in batch.messages.iter_mut() {
        let payload = message
            .read_and_take()
            .await
            .map_err(|e| e.to_string())?
            .ok_or("message payload violated: no payload".to_string())?;

        if payload != message.seq_no.to_string().into_bytes() {
            return Err(format!(
                "message payload violated: expected: {}, got: {}",
                message.seq_no,
                String::from_utf8_lossy(&payload)
            ));
        }

        messages_order.insert(message)?;
        offset_order.ack_message(message)?;
    }

    Ok(())
}

fn spawn_commit(
    fw: Arc<Framework>,
    reader: Arc<tokio::sync::Mutex<ydb::TopicReader>>,
    offset_order: Arc<verification::OffsetOrder>,

    partition_id: i64,
    offset: i64,

    commit_marker: ydb::TopicReaderCommitMarker,
    commit_delay: Duration,
) {
    tokio::spawn(async move {
        tokio::time::sleep(commit_delay).await;
        let handle = reader.lock().await.commit_with_ack(commit_marker);

        match handle.await {
            Ok(()) => offset_order.insert(partition_id, offset),
            Err(err) => fw.logger.printf(format!("commit not acked: {err}")),
        }
    });
}
