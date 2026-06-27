use super::ordered_task_queue::{self, OrderedTaskQueue};
use crate::client_topic::compression::codec_registry::CodecRegistry;
use crate::client_topic::compression::executor::Executor;
use crate::client_topic::list_types::Codec;
use crate::{TopicReaderMessage, YdbError, YdbResult};
use std::{num::NonZeroUsize, sync::Arc};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

type BatchRx = mpsc::UnboundedReceiver<(Vec<TopicReaderMessage>, Codec)>;

type DecompressedBatchTx = mpsc::UnboundedSender<YdbResult<Vec<TopicReaderMessage>>>;

pub(crate) struct DecompressionWorker {
    codec_registry: Arc<CodecRegistry>,
    queue: OrderedTaskQueue<Vec<TopicReaderMessage>>,
    results_rx: ordered_task_queue::TaskResultRx<Vec<TopicReaderMessage>>,
    parallelism: NonZeroUsize,
}

impl DecompressionWorker {
    pub(crate) fn new(codec_registry: Arc<CodecRegistry>, executor: Arc<dyn Executor>) -> Self {
        let parallelism = executor.available_parallelism();
        let output_backlog = parallelism.saturating_mul(super::OUTPUT_BACKLOG_PER_TASK);
        let (queue, results_rx) = OrderedTaskQueue::new(executor, parallelism, output_backlog);

        Self {
            codec_registry,
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

                let chunk_size =
                    (batch.len() / parallelism.get()).clamp(1, super::MAX_MESSAGES_PER_CHUNK);

                let mut batch_iter = batch.into_iter();
                loop {
                    let chunk: Vec<TopicReaderMessage> =
                        batch_iter.by_ref().take(chunk_size).collect();
                    if chunk.is_empty() {
                        break;
                    }

                    let registry = codec_registry.clone();

                    tokio::select! {
                        _ = schedule_cancellation_token.cancelled() => return,
                        _ = queue.submit(Box::new(move || {
                            decompress_batch(chunk, codec, registry)
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
) -> YdbResult<Vec<TopicReaderMessage>> {
    if codec == Codec::RAW {
        return Ok(batch);
    }

    let Some(decoder) = registry.get_decoder(codec) else {
        return Err(YdbError::custom(format!(
            "no decoder found for codec {}",
            codec.code
        )));
    };

    for message in batch.iter_mut() {
        let Some(raw_data) = message.raw_data.as_ref() else {
            continue;
        };

        message.raw_data = Some(decoder.decode(raw_data.as_slice()).map_err(|err| {
            YdbError::custom(format!(
                "{decoder:?} failed to decode: {err}, message seq_no: {}, message offset: {}",
                message.seq_no, message.offset,
            ))
        })?);
    }

    Ok(batch)
}
