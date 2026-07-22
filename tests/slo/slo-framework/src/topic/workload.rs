use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::{JoinError, JoinSet};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::Framework;
use crate::framework::Workload;
use crate::helpers::{RateLimiter, new_rate_limiter};
use crate::metrics::{OPERATION_READ, OPERATION_WRITE};

use super::{Params, TopicService, verification};

const WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

type WorkerResult = Result<Infallible, String>;

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

        let mut workers = JoinSet::new();

        spawn_writer_workers(
            &mut workers,
            self.fw.clone(),
            writers,
            self.params.write_rps,
            self.params.write_timeout,
        );

        spawn_reader_workers(
            &mut workers,
            self.fw.clone(),
            readers,
            self.params.delivery_timeout,
            self.params.commit_timeout,
        );

        supervise_workers(ctx, workers).await
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
    workers: &mut JoinSet<WorkerResult>,
    fw: Arc<Framework>,
    writers: Vec<ydb::TopicWriter>,
    rps: u32,
    timeout: Duration,
) {
    let limiter = Arc::new(new_rate_limiter(rps));

    for writer in writers {
        workers.spawn(writer_worker(fw.clone(), writer, limiter.clone(), timeout));
    }
}

fn spawn_reader_workers(
    workers: &mut JoinSet<WorkerResult>,
    fw: Arc<Framework>,
    readers: Vec<ydb::TopicReader>,
    delivery_timeout: Duration,
    commit_timeout: Duration,
) {
    let messages_order = Arc::new(verification::MessagesOrder::default());
    let offsets_order = Arc::new(verification::OffsetOrder::default());

    for (worker_id, reader) in readers.into_iter().enumerate() {
        workers.spawn(reader_worker(
            worker_id,
            fw.clone(),
            reader,
            messages_order.clone(),
            offsets_order.clone(),
            delivery_timeout,
            commit_timeout,
        ));
    }
}

async fn supervise_workers(
    ctx: &CancellationToken,
    mut workers: JoinSet<WorkerResult>,
) -> Result<(), String> {
    tokio::select! {
        _ = ctx.cancelled() => {
            workers.abort_all();
            timeout(WORKER_SHUTDOWN_TIMEOUT, workers.shutdown())
                .await
                .map_err(|_| "topic workers did not stop after cancellation".to_string())?;
            Ok(())
        }
        joined = workers.join_next() => unexpected_worker_exit(joined),
    }
}

fn unexpected_worker_exit(joined: Option<Result<WorkerResult, JoinError>>) -> Result<(), String> {
    match joined {
        Some(Ok(Err(err))) => Err(err),
        Some(Ok(Ok(never))) => match never {},
        Some(Err(err)) => Err(format!("topic worker task failed: {err}")),
        None => Err("topic worker set is empty".to_string()),
    }
}

async fn writer_worker(
    fw: Arc<Framework>,
    writer: ydb::TopicWriter,
    limiter: Arc<RateLimiter>,
    operation_timeout: Duration,
) -> WorkerResult {
    let mut seq_no: i64 = 1;

    loop {
        limiter.wait().await;

        let payload = format!("{seq_no}").into_bytes();
        seq_no = seq_no.wrapping_add(1);

        let message = ydb::TopicWriterMessage::builder().data(payload).build();
        let span = fw.metrics.start(OPERATION_WRITE);

        match timeout(operation_timeout, writer.write_with_ack(message)).await {
            Ok(Ok(_)) => span.finish(None, 1),
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

async fn reader_worker(
    worker_id: usize,
    fw: Arc<Framework>,
    mut reader: ydb::TopicReader,
    messages_order: Arc<verification::MessagesOrder>,
    offset_order: Arc<verification::OffsetOrder>,
    delivery_timeout: Duration,
    commit_timeout: Duration,
) -> WorkerResult {
    loop {
        let delivery_span = fw.metrics.start(OPERATION_READ);
        let mut batch = match timeout(delivery_timeout, reader.read_batch()).await {
            Ok(Ok(batch)) => batch,
            Ok(Err(err)) => {
                let msg = err.to_string();
                delivery_span.finish(Some(&msg), 1);
                fw.logger.errorf(format!("read failed: {msg}"));
                return Err(format!("reader {worker_id} failed: {msg}"));
            }
            Err(_) => {
                delivery_span.finish(Some("message delivery timeout"), 1);
                fw.logger.errorf("read failed: message delivery timeout");
                continue;
            }
        };

        if let Err(err) = process_batch(&fw, &messages_order, &offset_order, &mut batch).await {
            delivery_span.finish(Some(&err), 1);
            return Err(format!("reader {worker_id} invariant violated: {err}"));
        }

        // A delivered batch is not a completed read operation until
        // its offset commit is acknowledged. The commit span below
        // records that single successful operation.
        delivery_span.cancel();

        let partition_id = batch.partition_id();
        let end_offset = batch.offset();
        let commit_marker = batch.get_commit_marker();
        let span = fw.metrics.start(OPERATION_READ);

        match timeout(commit_timeout, reader.commit_with_ack(commit_marker)).await {
            Ok(Ok(())) => {
                offset_order.insert(partition_id, end_offset);
                span.finish(None, 1);
            }
            Ok(Err(err)) => {
                let msg = err.to_string();
                span.finish(Some(&msg), 1);
                fw.logger
                    .errorf(format!("commit acknowledgement failed: {msg}"));
            }
            Err(_) => {
                span.finish(Some("commit acknowledgement timeout"), 1);
                fw.logger.errorf("commit acknowledgement failed: timeout");
            }
        }
    }
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
