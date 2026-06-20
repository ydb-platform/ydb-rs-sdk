use super::ordered_task_queue::{self, OrderedTaskQueue};
use crate::client_topic::compression::codec_registry::CodecRegistry;
use crate::client_topic::compression::codec_selector::{CodecSelection, CodecSelector};
use crate::client_topic::compression::error_strategy::ErrorHandlingStrategy;
use crate::client_topic::compression::executor::Executor;
use crate::client_topic::list_types::Codec;
use crate::{YdbError, YdbResult};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::warn;
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

/// One chunk may produce multiple sub-batches when `Skip` falls back to RAW for
/// individual messages — each group is one wire-level `WriteRequest`.
pub(crate) type CompressedGroups = Vec<(Codec, Vec<MessageData>)>;
type ChunkResult = YdbResult<CompressedGroups>;
type InputRx = mpsc::UnboundedReceiver<Vec<MessageData>>;
type OutputTx = mpsc::UnboundedSender<ChunkResult>;

pub(crate) struct CompressionWorker {
    codec_selector: CodecSelector,
    codec_registry: Arc<CodecRegistry>,
    error_strategy: ErrorHandlingStrategy,
    queue: OrderedTaskQueue<CompressedGroups>,
    results_rx: ordered_task_queue::TaskResultRx<CompressedGroups>,
    parallelism: usize,
}

impl CompressionWorker {
    pub(crate) fn new(
        selection: CodecSelection,
        codec_registry: Arc<CodecRegistry>,
        error_strategy: ErrorHandlingStrategy,
        executor: Arc<dyn Executor>,
        server_codecs: Vec<Codec>,
    ) -> YdbResult<Self> {
        let codec_selector = CodecSelector::new(selection, server_codecs, codec_registry.clone())?;
        let parallelism = executor.available_parallelism();
        let (queue, results_rx) = OrderedTaskQueue::new(executor, parallelism);

        Ok(Self {
            codec_selector,
            codec_registry,
            error_strategy,
            queue,
            results_rx,
            parallelism,
        })
    }

    pub(crate) fn spawn_into(self, tasks: &mut JoinSet<()>, mut rx: InputRx, tx: OutputTx) {
        let CompressionWorker {
            mut codec_selector,
            codec_registry,
            error_strategy,
            queue,
            mut results_rx,
            parallelism,
        } = self;

        tasks.spawn(async move {
            while let Some(mut batch) = rx.recv().await {
                codec_selector.step(&batch);
                let codec = codec_selector.codec();
                let chunk_size = (batch.len() / parallelism).max(1);

                while !batch.is_empty() {
                    let chunk: Vec<MessageData> =
                        batch.drain(..chunk_size.min(batch.len())).collect();

                    let registry = codec_registry.clone();
                    let strategy = error_strategy;

                    queue
                        .submit(Box::new(move || {
                            compress_batch(chunk, codec, registry, strategy)
                        }))
                        .await;
                }
            }
        });

        tasks.spawn(async move {
            while let Some(result_tx) = results_rx.recv().await {
                let result = result_tx
                    .await
                    .unwrap_or(Err(YdbError::custom("executor compression task panicked")));

                if tx.send(result).is_err() {
                    break;
                }
            }
        });
    }
}

fn compress_batch(
    batch: Vec<MessageData>,
    codec: Codec,
    registry: Arc<CodecRegistry>,
    strategy: ErrorHandlingStrategy,
) -> ChunkResult {
    if codec == Codec::RAW {
        return Ok(vec![(codec, batch)]);
    }

    let Some(encoder) = registry.get_encoder(codec) else {
        return process_missing_encoder(batch, codec, strategy);
    };

    let mut groups: CompressedGroups = Vec::new();

    for mut message in batch.into_iter() {
        match (encoder.encode(message.data.as_slice()), strategy) {
            (Ok(compressed), _) => {
                message.data = compressed;
                push_to_group(&mut groups, codec, message);
            }

            (Err(err), ErrorHandlingStrategy::Skip) => {
                warn!(
                    ?encoder,
                    ?err,
                    message.seq_no,
                    "failed to encode, pass as RAW message"
                );
                push_to_group(&mut groups, Codec::RAW, message);
            }

            (Err(err), ErrorHandlingStrategy::FailFast) => {
                return Err(err);
            }
        };
    }

    Ok(groups)
}

fn process_missing_encoder(
    batch: Vec<MessageData>,
    codec: Codec,
    strategy: ErrorHandlingStrategy,
) -> ChunkResult {
    match strategy {
        ErrorHandlingStrategy::FailFast => Err(YdbError::custom(format!(
            "no encoder found for codec {}",
            codec.code
        ))),
        ErrorHandlingStrategy::Skip => {
            warn!(
                "no encoder found for codec {}, passing raw messages",
                codec.code
            );
            Ok(vec![(Codec::RAW, batch)])
        }
    }
}

fn push_to_group(groups: &mut CompressedGroups, codec: Codec, message: MessageData) {
    match groups.last_mut() {
        Some((last_codec, last_messages)) if *last_codec == codec => {
            last_messages.push(message);
        }

        _ => {
            groups.push((codec, vec![message]));
        }
    }
}
