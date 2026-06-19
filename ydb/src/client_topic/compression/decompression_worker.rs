use super::ordered_task_queue::{self, OrderedTaskQueue};
use crate::client_topic::compression::codec_registry::CodecRegistry;
use crate::client_topic::compression::error_strategy::ErrorHandlingStrategy;
use crate::client_topic::compression::executor::Executor;
use crate::client_topic::list_types::Codec;
use crate::{TopicReaderMessage, YdbError, YdbResult};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::warn;

type BatchRx = mpsc::UnboundedReceiver<(Vec<TopicReaderMessage>, Codec)>;

type DecompressedBatchTx = mpsc::UnboundedSender<YdbResult<Vec<TopicReaderMessage>>>;

pub(crate) struct DecompressionWorker {
    codec_registry: Arc<CodecRegistry>,
    error_strategy: ErrorHandlingStrategy,
    queue: OrderedTaskQueue<Vec<TopicReaderMessage>>,
    results_rx: ordered_task_queue::TaskResultRx<Vec<TopicReaderMessage>>,
    parallelism: usize,
}

impl DecompressionWorker {
    pub(crate) fn new(
        codec_registry: Arc<CodecRegistry>,
        error_strategy: ErrorHandlingStrategy,
        executor: Arc<dyn Executor>,
    ) -> Self {
        let parallelism = executor.available_parallelism().max(1);
        let (queue, results_rx) = OrderedTaskQueue::new(executor, parallelism);

        Self {
            codec_registry,
            error_strategy,
            queue,
            results_rx,
            parallelism,
        }
    }

    pub(crate) fn spawn_into(
        self,
        tasks: &mut JoinSet<()>,
        mut rx: BatchRx,
        tx: DecompressedBatchTx,
        cancellation_token: CancellationToken,
    ) {
        let DecompressionWorker {
            codec_registry,
            error_strategy,
            queue,
            mut results_rx,
            parallelism,
        } = self;

        let schedule_cancellation_token = cancellation_token.clone();
        tasks.spawn(async move {
            loop {
                let Some((batch, codec)) = (tokio::select! {
                    _ = schedule_cancellation_token.cancelled() => return,
                    batch = rx.recv() => batch,
                }) else {
                    return;
                };

                let chunk_size = (batch.len() / parallelism).max(1);

                let mut batch_iter = batch.into_iter();
                loop {
                    let chunk: Vec<TopicReaderMessage> =
                        batch_iter.by_ref().take(chunk_size).collect();
                    if chunk.is_empty() {
                        break;
                    }

                    let registry = codec_registry.clone();
                    let strategy = error_strategy;

                    tokio::select! {
                        _ = schedule_cancellation_token.cancelled() => return,
                        _ = queue.submit(Box::new(move || {
                            decompress_batch(chunk, codec, registry, strategy)
                        })) => {}
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
                        "executor decompression task panicked",
                    ))),
                };

                if tx.send(result).is_err() {
                    break;
                }
            }
        });
    }
}

fn decompress_batch(
    mut batch: Vec<TopicReaderMessage>,
    codec: Codec,
    registry: Arc<CodecRegistry>,
    strategy: ErrorHandlingStrategy,
) -> YdbResult<Vec<TopicReaderMessage>> {
    if codec == Codec::RAW {
        return Ok(batch);
    }

    for message in batch.iter_mut() {
        let Some(raw_data) = message.raw_data.take() else {
            continue;
        };

        let data = prost::bytes::Bytes::from(raw_data);
        match registry.decompress(&data, &codec) {
            Ok(decompressed) => {
                message.raw_data = Some(decompressed.into());
            }
            Err(err) => match strategy {
                ErrorHandlingStrategy::FailFast => return Err(err),
                ErrorHandlingStrategy::Skip => {
                    warn!(
                        message.offset,
                        message.seq_no,
                        ?err,
                        "decompression failed; keeping original payload"
                    );
                    message.raw_data = Some(data.to_vec());
                    message.decompression_failed = true;
                }
            },
        }
    }

    Ok(batch)
}
