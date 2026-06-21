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

    let Some(decoder) = registry.get_decoder(codec) else {
        return process_missing_decoder(batch, codec, strategy);
    };

    for message in batch.iter_mut() {
        let Some(raw_data) = message.raw_data.as_ref() else {
            continue;
        };

        match (decoder.decode(raw_data.as_slice()), strategy) {
            (Ok(decompressed), _) => {
                message.raw_data = Some(decompressed);
            }

            (Err(err), ErrorHandlingStrategy::Skip) => {
                warn!(
                    ?decoder,
                    ?err,
                    message.seq_no,
                    message.offset,
                    "decoder failed, keep original payload"
                );
                message.decompression_failed = true;
            }

            (Err(err), ErrorHandlingStrategy::FailFast) => {
                return Err(err);
            }
        }
    }

    Ok(batch)
}

fn process_missing_decoder(
    batch: Vec<TopicReaderMessage>,
    codec: Codec,
    strategy: ErrorHandlingStrategy,
) -> YdbResult<Vec<TopicReaderMessage>> {
    match strategy {
        ErrorHandlingStrategy::FailFast => Err(YdbError::custom(format!(
            "no decoder found for codec {}",
            codec.code
        ))),
        ErrorHandlingStrategy::Skip => {
            warn!(
                "no decoder found for codec {}, passing raw messages",
                codec.code
            );
            Ok(batch)
        }
    }
}
