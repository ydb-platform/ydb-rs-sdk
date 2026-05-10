use super::ordered_task_queue::OrderedTaskQueue;
use crate::client_topic::compression::codec_registry::CodecRegistry;
use crate::client_topic::compression::error_strategy::ErrorHandlingStrategy;
use crate::client_topic::list_types::Codec;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::{
    DecompressedBatch, RawBatchWithId,
};
use crate::YdbResult;
use prost::bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::warn;

pub struct DecompressionWorker {
    codec_registry: Arc<CodecRegistry>,
    error_strategy: ErrorHandlingStrategy,
    queue: OrderedTaskQueue<DecompressedBatch>,
}

impl DecompressionWorker {
    pub fn new(
        codec_registry: Arc<CodecRegistry>,
        error_strategy: ErrorHandlingStrategy,
    ) -> (Self, mpsc::UnboundedReceiver<YdbResult<DecompressedBatch>>) {
        let (queue, receiver) = OrderedTaskQueue::new();
        (
            Self {
                codec_registry,
                error_strategy,
                queue,
            },
            receiver,
        )
    }

    pub async fn process_batch(&self, batch: RawBatchWithId) -> YdbResult<()> {
        let registry = self.codec_registry.clone();
        let strategy = self.error_strategy.clone();

        self.queue
            .submit(Box::new(move || {
                decompress_batch(batch, &registry, &strategy)
            }))
            .await
    }
}

fn decompress_batch(
    mut batch_with_id: RawBatchWithId,
    registry: &CodecRegistry,
    strategy: &ErrorHandlingStrategy,
) -> YdbResult<DecompressedBatch> {
    let read_session_size_bytes = batch_with_id.batch.get_read_session_size();
    let partition_session_id = batch_with_id.partition_session_id;
    let batch = &mut batch_with_id.batch;
    let codec = Codec {
        code: batch.codec.code,
    };
    if codec == Codec::RAW {
        return Ok(DecompressedBatch {
            partition_session_id,
            batch: batch_with_id.batch,
            read_session_size_bytes,
        });
    }

    let mut failed_offsets = Vec::new();

    for message in batch.message_data.iter_mut() {
        let data_bytes = Bytes::from(std::mem::take(&mut message.data));
        match registry.decompress(&data_bytes, &codec) {
            Ok(decompressed) => {
                message.data = decompressed.to_vec();
            }
            Err(err) => match strategy {
                ErrorHandlingStrategy::FailFast => {
                    return Err(err);
                }
                ErrorHandlingStrategy::Skip => {
                    warn!(
                        "decompression failed for message (offset: {}, seq_no: {}), \
                         dropping message: {}",
                        message.offset, message.seq_no, err
                    );
                    failed_offsets.push(message.offset);
                }
            },
        }
    }

    if !failed_offsets.is_empty() {
        batch
            .message_data
            .retain(|m| !failed_offsets.contains(&m.offset));
    }

    Ok(DecompressedBatch {
        partition_session_id,
        batch: batch_with_id.batch,
        read_session_size_bytes,
    })
}
