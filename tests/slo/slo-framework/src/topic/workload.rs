use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::Framework;
use crate::framework::Workload;
use crate::helpers::{TimeoutOutcome, new_rate_limiter, run_workers_for, timeout_or_cancel};
use crate::metrics::{OPERATION_READ, OPERATION_WRITE};

use super::{Params, TopicService, verification};

pub struct TopicWorkload<T: TopicService> {
    fw: Arc<Framework>,
    topic: Arc<T>,
    params: Params,
}

impl<T: TopicService + 'static> TopicWorkload<T> {
    pub fn new(fw: Framework, params: Params, topic: T) -> Self {
        Self {
            fw: Arc::new(fw),
            topic: Arc::new(topic),
            params,
        }
    }
}

#[async_trait::async_trait]
impl<T: TopicService + 'static> Workload for TopicWorkload<T> {
    async fn setup(&self, _ctx: &CancellationToken) -> Result<(), String> {
        self.topic.create_topic().await?;
        self.fw.logger.printf("create topic ok");

        Ok(())
    }

    async fn run(&self, ctx: &CancellationToken) -> Result<(), String> {
        let writers = self.topic.open_writers().await?;
        let readers = self.topic.open_readers().await?;

        self.fw.logger.printf(format!(
            "opened {} partition(s), {} writer(s), {} reader(s)",
            self.params.partition_count,
            writers.len(),
            readers.len(),
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
            self.params.delivery_timeout,
            self.params.commit_timeout,
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

                let message = ydb::TopicWriterMessage::builder().data(payload).build();

                let span = fw.metrics.start(OPERATION_WRITE);
                match timeout_or_cancel(&ctx, timeout, writer.write_with_ack(message)).await {
                    TimeoutOutcome::Completed(Ok(_)) => span.finish(None, 1),
                    TimeoutOutcome::Completed(Err(err)) => {
                        let msg = err.to_string();
                        span.finish(Some(&msg), 1);
                        fw.logger.errorf(format!("write failed: {msg}"));
                    }
                    TimeoutOutcome::TimedOut => {
                        span.finish(Some("write timeout"), 1);
                        fw.logger.errorf("write failed: timeout");
                    }
                    TimeoutOutcome::Cancelled => {
                        span.cancel();
                        break;
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
    delivery_timeout: Duration,
    commit_timeout: Duration,
    commit_delay: Duration,
) -> tokio::task::JoinHandle<()> {
    let messages_order = Arc::new(verification::MessagesOrder::default());
    let offsets_order = Arc::new(verification::OffsetOrder::default());

    tokio::spawn(run_workers_for(readers.into_iter().map(
        move |mut reader| {
            let ctx = ctx.clone();
            let fw = fw.clone();

            let messages_order = messages_order.clone();
            let offset_order = offsets_order.clone();

            move || async move {
                while !ctx.is_cancelled() {
                    let delivery_span = fw.metrics.start(OPERATION_READ);
                    let mut batch = match timeout_or_cancel(
                        &ctx,
                        delivery_timeout,
                        reader.read_batch(),
                    )
                    .await
                    {
                        TimeoutOutcome::Completed(Ok(batch)) => batch,
                        TimeoutOutcome::Completed(Err(err)) => {
                            let msg = err.to_string();
                            delivery_span.finish(Some(&msg), 1);
                            fw.logger.errorf(format!("read failed: {msg}"));
                            continue;
                        }
                        TimeoutOutcome::TimedOut => {
                            delivery_span.finish(Some("message delivery timeout"), 1);
                            fw.logger.errorf("read failed: message delivery timeout");
                            continue;
                        }
                        TimeoutOutcome::Cancelled => {
                            delivery_span.cancel();
                            break;
                        }
                    };

                    if let Err(err) =
                        process_batch(&fw, &messages_order, &offset_order, &mut batch).await
                    {
                        delivery_span.finish(Some(&err), 1);
                        fw.logger.errorf(format!("invariant violated: {err}"));
                        continue;
                    }

                    // A delivered batch is not a completed read operation until
                    // its offset commit is acknowledged. The commit span below
                    // records that single successful operation.
                    delivery_span.cancel();

                    // This simulates application processing and is intentionally
                    // outside the SDK commit latency and deadline.
                    tokio::select! {
                        biased;
                        _ = ctx.cancelled() => break,
                        _ = tokio::time::sleep(commit_delay) => {}
                    }

                    let partition_id = batch.partition_id();
                    let end_offset = batch.offset();
                    let commit_marker = batch.get_commit_marker();
                    let span = fw.metrics.start(OPERATION_READ);

                    match timeout_or_cancel(
                        &ctx,
                        commit_timeout,
                        reader.commit_with_ack(commit_marker),
                    )
                    .await
                    {
                        TimeoutOutcome::Completed(Ok(())) => {
                            offset_order.insert(partition_id, end_offset);
                            span.finish(None, 1);
                        }
                        TimeoutOutcome::Completed(Err(err)) => {
                            let msg = err.to_string();
                            span.finish(Some(&msg), 1);
                            fw.logger
                                .errorf(format!("commit acknowledgement failed: {msg}"));
                        }
                        TimeoutOutcome::TimedOut => {
                            span.finish(Some("commit acknowledgement timeout"), 1);
                            fw.logger.errorf("commit acknowledgement failed: timeout");
                        }
                        TimeoutOutcome::Cancelled => {
                            span.cancel();
                            break;
                        }
                    }
                }
            }
        },
    )))
}

async fn process_batch(
    fw: &Framework,
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
        record_topic_e2e_latency(fw, message)?;
    }

    Ok(())
}

fn record_topic_e2e_latency(
    fw: &Framework,
    message: &ydb::TopicReaderMessage,
) -> Result<(), String> {
    let created_at = message
        .created_at
        .ok_or_else(|| "message has no timestamp".to_string())?;

    let latency = std::time::SystemTime::now()
        .duration_since(created_at)
        .map_err(|e| format!("message timestamp in the future: {e}"))?;

    fw.metrics.record_topic_e2e_latency(latency);

    Ok(())
}
