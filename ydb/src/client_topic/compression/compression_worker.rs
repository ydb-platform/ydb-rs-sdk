use super::ordered_task_queue::{self, OrderedTaskQueue};
use crate::client_topic::compression::codec_registry::CodecRegistry;
use crate::client_topic::compression::codec_selector::{CodecSelection, CodecSelector};
use crate::client_topic::compression::executor::Executor;
use crate::client_topic::list_types::Codec;
use crate::{YdbError, YdbResult};
use std::{num::NonZeroUsize, sync::Arc};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use ydb_grpc::ydb_proto::topic::stream_write_message::WriteRequest;
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

type ChunkResult = YdbResult<WriteRequest>;
type InputRx = mpsc::Receiver<Vec<MessageData>>;
type OutputTx = mpsc::Sender<ChunkResult>;

pub(crate) struct CompressionWorker {
    codec_selector: CodecSelector,
    codec_registry: Arc<CodecRegistry>,
    queue: OrderedTaskQueue<WriteRequest>,
    results_rx: ordered_task_queue::TaskResultRx<WriteRequest>,
    parallelism: NonZeroUsize,
}

impl CompressionWorker {
    pub(crate) fn new(
        selection: CodecSelection,
        codec_registry: Arc<CodecRegistry>,
        executor: Arc<dyn Executor>,
        server_codecs: Vec<Codec>,
    ) -> YdbResult<Self> {
        let codec_selector = CodecSelector::new(
            selection,
            server_codecs,
            codec_registry.clone(),
            executor.clone(),
        )?;
        let parallelism = executor.available_parallelism();
        let output_backlog = parallelism.saturating_mul(super::OUTPUT_BACKLOG_PER_TASK);
        let (queue, results_rx) = OrderedTaskQueue::new(executor, parallelism, output_backlog);

        Ok(Self {
            codec_selector,
            codec_registry,
            queue,
            results_rx,
            parallelism,
        })
    }

    pub(crate) fn spawn_into(
        self,
        tasks: &mut JoinSet<()>,
        mut rx: InputRx,
        tx: OutputTx,
        cancellation_token: CancellationToken,
    ) {
        let CompressionWorker {
            mut codec_selector,
            codec_registry,
            queue,
            mut results_rx,
            parallelism,
        } = self;

        let schedule_cancellation = cancellation_token.clone();
        tasks.spawn(async move {
            loop {
                let Some(mut batch) = (tokio::select! {
                    _ = schedule_cancellation.cancelled() => return,
                    batch = rx.recv() => batch,
                }) else {
                    return;
                };

                tokio::select! {
                    _ = schedule_cancellation.cancelled() => return,
                    _ = codec_selector.step(&batch) => {}
                }
                let codec = codec_selector.codec();
                let chunk_size =
                    (batch.len() / parallelism).clamp(1, super::MAX_MESSAGES_PER_CHUNK);

                while !batch.is_empty() {
                    let chunk: Vec<MessageData> =
                        batch.drain(..chunk_size.min(batch.len())).collect();

                    let registry = codec_registry.clone();

                    tokio::select! {
                        _ = schedule_cancellation.cancelled() => return,
                        _ = queue.submit(Box::new(move || compress_batch(chunk, codec, registry))) => {}
                    }
                }
            }
        });

        tasks.spawn(async move {
            loop {
                let Some(result_rx) = (tokio::select! {
                    _ = cancellation_token.cancelled() => return,
                    result_rx = results_rx.recv() => result_rx,
                }) else {
                    return;
                };

                let result = tokio::select! {
                    _ = cancellation_token.cancelled() => return,
                    result = result_rx => result.unwrap_or(Err(YdbError::custom(
                        "executor compression task panicked",
                    ))),
                };

                let sent = tokio::select! {
                    _ = cancellation_token.cancelled() => return,
                    sent = tx.send(result) => sent,
                };
                if sent.is_err() {
                    return;
                }
            }
        });
    }
}

fn compress_batch(
    mut batch: Vec<MessageData>,
    codec: Codec,
    registry: Arc<CodecRegistry>,
) -> ChunkResult {
    if codec != Codec::RAW {
        let Some(encoder) = registry.get_encoder(codec) else {
            return Err(YdbError::custom(format!(
                "no encoder found for codec {}",
                codec.code
            )));
        };

        for message in batch.iter_mut() {
            message.data = encoder.encode(message.data.as_slice()).map_err(|err| {
                YdbError::custom(format!(
                    "{encoder:?} failed to encode: {err}, message seq_no: {}",
                    message.seq_no,
                ))
            })?;
        }
    }

    Ok(WriteRequest {
        messages: batch,
        codec: codec.code,
        tx: None,
    })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Mutex;
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;
    use crate::client_topic::compression::Executor;

    struct HoldingExecutor {
        tasks: Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>,
    }

    impl HoldingExecutor {
        fn new() -> Self {
            Self {
                tasks: Mutex::new(Vec::new()),
            }
        }

        fn task_count(&self) -> usize {
            self.tasks.lock().expect("holding executor lock").len()
        }
    }

    impl Executor for HoldingExecutor {
        fn available_parallelism(&self) -> NonZeroUsize {
            NonZeroUsize::MIN
        }

        fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
            self.tasks.lock().expect("holding executor lock").push(task);
        }
    }

    fn message(seq_no: i64) -> MessageData {
        MessageData {
            seq_no,
            created_at: None,
            data: Vec::new(),
            uncompressed_size: 0,
            metadata_items: Vec::new(),
            partitioning: None,
        }
    }

    #[tokio::test]
    async fn cancellation_stops_scheduler_waiting_for_a_worker_slot() {
        let executor = Arc::new(HoldingExecutor::new());
        let worker = CompressionWorker::new(
            CodecSelection::Fixed(Codec::RAW),
            Arc::new(CodecRegistry::new()),
            executor.clone(),
            vec![Codec::RAW],
        )
        .expect("raw codec should be supported");
        let (input_tx, input_rx) = mpsc::channel(2);
        let (output_tx, _output_rx) = mpsc::channel(1);
        let cancellation = CancellationToken::new();
        let mut tasks = JoinSet::new();

        worker.spawn_into(&mut tasks, input_rx, output_tx, cancellation.clone());
        input_tx
            .send(vec![message(1)])
            .await
            .expect("worker input should be open");
        input_tx
            .send(vec![message(2)])
            .await
            .expect("worker input should be open");

        timeout(Duration::from_millis(50), async {
            while executor.task_count() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first compression task should occupy the only worker slot");

        cancellation.cancel();
        timeout(Duration::from_millis(50), tasks.join_next())
            .await
            .expect("cancellation should stop the compression worker");
    }
}
