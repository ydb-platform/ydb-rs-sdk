use super::ordered_task_queue::OrderedTaskQueue;
use crate::client_topic::compression::codec_registry::CodecRegistry;
use crate::client_topic::compression::error_strategy::ErrorHandlingStrategy;
use crate::client_topic::compression::executor::Executor;
use crate::client_topic::list_types::Codec;
use crate::{TopicReaderMessage, YdbResult};
use prost::bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::warn;

pub struct DecompressionWorker {
    codec_registry: Arc<CodecRegistry>,
    error_strategy: ErrorHandlingStrategy,
    parallelism: usize,
    queue: OrderedTaskQueue<Vec<TopicReaderMessage>>,
}

impl DecompressionWorker {
    pub fn new(
        codec_registry: Arc<CodecRegistry>,
        error_strategy: ErrorHandlingStrategy,
        executor: Arc<dyn Executor>,
    ) -> (
        Self,
        mpsc::UnboundedReceiver<YdbResult<Vec<TopicReaderMessage>>>,
    ) {
        let parallelism = executor.available_parallelism();
        let (queue, receiver) = OrderedTaskQueue::new(executor);
        (
            Self {
                codec_registry,
                error_strategy,
                parallelism,
                queue,
            },
            receiver,
        )
    }

    pub fn process_batch(&self, mut batch: Vec<TopicReaderMessage>, codec: Codec) {
        let chunk_size = (batch.len() / self.parallelism).max(1);

        while !batch.is_empty() {
            let chunk: Vec<TopicReaderMessage> =
                batch.drain(..chunk_size.min(batch.len())).collect();
            let registry = self.codec_registry.clone();
            let strategy = self.error_strategy.clone();

            self.queue.submit(Box::new(move || {
                decompress_batch(chunk, codec, &registry, &strategy)
            }));
        }
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
        let Some(raw_data) = message.raw_data.as_mut() else {
            continue;
        };

        let raw_data = std::mem::take(raw_data);

        match registry.decompress(&raw_data.into(), &codec) {
            Ok(decompressed) => {
                message.raw_data = Some(decompressed.into());
            }
            Err(err) => {
                handle_error(strategy, err, message)?;
            }
        }
    }

    Ok(batch)
}

fn handle_error(
    strategy: ErrorHandlingStrategy,
    err: YdbError,
    message: &mut TopicReaderMessage,
) -> YdbResult<()> {
    match strategy {
        ErrorHandlingStrategy::FailFast => {
            error!(
                "decompression failed for message (offset: {}, seq_no: {}), \
                    dropping message: {}",
                message.offset, message.seq_no, err
            );

            Err(err)
        }
        ErrorHandlingStrategy::Skip => {
            warn!(
                "decompression failed for message (offset: {}, seq_no: {}), \
                    dropping message: {}",
                message.offset, message.seq_no, err
            );
            message.decompression_failed = true;

            Ok(())
        }
    }
}
