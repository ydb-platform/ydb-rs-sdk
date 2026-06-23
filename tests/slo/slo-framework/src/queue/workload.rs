use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::framework::Workload;
use crate::helpers::{new_rate_limiter, run_workers};
use crate::metrics::{OPERATION_READ, OPERATION_WRITE};
use crate::partition::{MessageReader, MessageWriter, WriterHandle};
use crate::Framework;

use super::{Params, Queue};

type LockedHandles<T> = Arc<Vec<Arc<Mutex<Box<T>>>>>;

pub struct QueueWorkload<Q: Queue> {
    fw: Arc<Framework>,
    queue: Arc<Q>,
    params: Params,
    // TODO: local_store: DashMap<BucketID, VecDeque<Vec<u8>>>
    // TODO: replay_queue: DashMap<BucketID, VecDeque<Vec<u8>>>
}

impl<Q: Queue + 'static> QueueWorkload<Q> {
    pub fn new(fw: Framework, params: Params, queue: Q) -> Self {
        Self {
            fw: Arc::new(fw),
            queue: Arc::new(queue),
            params,
        }
    }
}

#[async_trait::async_trait]
impl<Q: Queue + 'static> Workload for QueueWorkload<Q> {
    async fn setup(&self, _ctx: &CancellationToken) -> Result<(), String> {
        self.queue.create_topic().await?;
        self.fw.logger.printf("create topic ok");

        Ok(())
    }

    async fn run(&self, ctx: &CancellationToken) -> Result<(), String> {
        let writers = self.queue.open_writers().await?;
        let readers = self.queue.open_readers().await?;

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
        );

        let _ = tokio::join!(write_handle, read_handle);

        Ok(())
    }

    async fn teardown(&self, _ctx: &CancellationToken) -> Result<(), String> {
        let drop_result = self.queue.drop_topic().await;
        let _ = self.queue.close().await;

        drop_result?;
        self.fw.logger.printf("cleanup topic ok");

        Ok(())
    }
}

fn spawn_writer_workers(
    ctx: CancellationToken,
    fw: Arc<Framework>,
    writers: Vec<WriterHandle>,
    rps: u32,
    timeout: Duration,
) -> tokio::task::JoinHandle<()> {
    let worker_count = writers.len();

    // Skeleton drops handle.buckets; real workload will mirror each payload to
    // every bucket in handle.buckets before/with the acknowledged write.
    let shared: LockedHandles<dyn MessageWriter> = Arc::new(
        writers
            .into_iter()
            .map(|handle| Arc::new(Mutex::new(handle.writer)))
            .collect(),
    );

    let limiter = new_rate_limiter(rps);

    tokio::spawn(async move {
        let counter = Arc::new(AtomicUsize::new(0));

        run_workers(&ctx, worker_count, limiter, move || {
            let fw = fw.clone();
            let shared = shared.clone();
            let counter = counter.clone();

            async move {
                if shared.is_empty() {
                    return;
                }

                let idx = counter.fetch_add(1, Ordering::Relaxed) % shared.len();
                let writer = shared[idx].clone();

                let span = fw.metrics.start(OPERATION_WRITE);
                // TODO: monotonic payload generator
                // TODO: push to local_store per WriterHandle.buckets before write
                let payload = b"placeholder".to_vec();

                let work = async move { writer.lock().await.write(payload).await };
                let result = tokio::time::timeout(timeout, work).await;

                match result {
                    Ok(Ok(())) => span.finish(None, 1),
                    Ok(Err(err)) => {
                        span.finish(Some(&err), 1);
                        fw.logger.errorf(format!("write failed: {err}"));
                    }
                    Err(_) => {
                        span.finish(Some("write timeout"), 1);
                        fw.logger.errorf("write failed: timeout");
                    }
                }
            }
        })
        .await;
    })
}

fn spawn_reader_workers(
    ctx: CancellationToken,
    fw: Arc<Framework>,
    readers: Vec<Box<dyn MessageReader>>,
    rps: u32,
    timeout: Duration,
) -> tokio::task::JoinHandle<()> {
    let worker_count = readers.len();

    let shared: LockedHandles<dyn MessageReader> = Arc::new(
        readers
            .into_iter()
            .map(|r| Arc::new(Mutex::new(r)))
            .collect(),
    );

    let limiter = new_rate_limiter(rps);

    tokio::spawn(async move {
        let counter = Arc::new(AtomicUsize::new(0));

        run_workers(&ctx, worker_count, limiter, move || {
            let fw = fw.clone();
            let shared = shared.clone();
            let counter = counter.clone();

            async move {
                if shared.is_empty() {
                    return;
                }

                let idx = counter.fetch_add(1, Ordering::Relaxed) % shared.len();
                let reader = shared[idx].clone();

                let span = fw.metrics.start(OPERATION_READ);

                let work = async move { reader.lock().await.read_batch().await };
                let result = tokio::time::timeout(timeout, work).await;

                match result {
                    Ok(Ok(_batch)) => {
                        // TODO: pop replay_queue[bucket_id] then local_store[bucket_id], assert match
                        // TODO: commit under commit_timeout — batch.marker (even handle idx) or msg.commit_marker (odd)
                        // TODO: on transient commit err, push batch payloads to replay_queue[bucket_id]
                        span.finish(None, 1)
                    }
                    Ok(Err(err)) => {
                        span.finish(Some(&err), 1);
                        fw.logger.errorf(format!("read failed: {err}"));
                    }
                    Err(_) => {
                        span.finish(Some("read timeout"), 1);
                        fw.logger.errorf("read failed: timeout");
                    }
                }
            }
        })
        .await;
    })
}
