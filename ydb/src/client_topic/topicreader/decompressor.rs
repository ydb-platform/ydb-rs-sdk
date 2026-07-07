use std::collections::VecDeque;
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::client_topic::compression::{
    CodecRegistry, CompressionDecoder, Executor, OrderedTaskQueue, TaskResultRx,
    MAX_MESSAGES_PER_CHUNK, OUTPUT_BACKLOG_PER_TASK,
};
use crate::client_topic::list_types::Codec;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::{
    RawBatch, RawFromServer, RawPartitionData, RawReadResponse,
};
use crate::{YdbError, YdbResult};

use super::reconnector;
use super::runtime::RuntimeHandle;
use super::task_supervisor::wait_child_tasks;

type EventRx = mpsc::UnboundedReceiver<RawFromServer>;

pub(super) struct Decompressor {
    codec_registry: Arc<CodecRegistry>,
    executor: Arc<dyn Executor>,
    rx: EventRx,
    runtime: RuntimeHandle,
    cancellation: CancellationToken,
}

impl Decompressor {
    pub(super) fn new(
        attempt: &reconnector::ConnectionAttempt,
        rx: EventRx,
        runtime: RuntimeHandle,
    ) -> Self {
        let mut codec_registry = CodecRegistry::new();
        for dec in &attempt.options.extra_decoders {
            codec_registry.register_decoder(dec.clone());
        }

        Self {
            codec_registry: Arc::new(codec_registry),
            executor: attempt.compression_executor.clone(),
            rx,
            runtime,
            cancellation: attempt.cancellation_token.clone(),
        }
    }

    pub(super) async fn run(self) -> YdbResult<()> {
        let Self {
            codec_registry,
            executor,
            rx,
            runtime,
            cancellation,
        } = self;

        let parallelism = executor.available_parallelism();
        let output_backlog = parallelism.saturating_mul(OUTPUT_BACKLOG_PER_TASK);
        let (queue, results_rx) = OrderedTaskQueue::new(executor, parallelism, output_backlog);
        let decompressor_cancellation = cancellation.child_token();

        let schedule = schedule_loop(
            rx,
            queue,
            codec_registry,
            parallelism,
            decompressor_cancellation.clone(),
        );
        let forward = forward_loop(results_rx, runtime, decompressor_cancellation.clone());

        let mut tasks: JoinSet<YdbResult<()>> = JoinSet::new();
        tasks.spawn(schedule);
        tasks.spawn(forward);

        wait_child_tasks(&decompressor_cancellation, tasks, "decompressor").await
    }
}

async fn schedule_loop(
    rx: EventRx,
    queue: OrderedTaskQueue<RawFromServer>,
    codec_registry: Arc<CodecRegistry>,
    parallelism: NonZeroUsize,
    cancellation: CancellationToken,
) -> YdbResult<()> {
    select! {
        _ = cancellation.cancelled() => {
            debug!("decompressor schedule cancelled, stopping");
            Ok(())
        }
        result = schedule_events(rx, queue, codec_registry, parallelism) => {
            let Err(e) = result;
            Err(e)
        }
    }
}

async fn schedule_events(
    mut rx: EventRx,
    queue: OrderedTaskQueue<RawFromServer>,
    codec_registry: Arc<CodecRegistry>,
    parallelism: NonZeroUsize,
) -> YdbResult<Infallible> {
    loop {
        let Some(msg) = rx.recv().await else {
            return Err(YdbError::Transport(
                "decompressor input channel closed".into(),
            ));
        };

        match msg {
            RawFromServer::ReadResponse(resp) => {
                for (partition_session_id, batch) in split_into_batches(resp, parallelism) {
                    let decoder = decoder_for_batch(&codec_registry, &batch)?;
                    queue
                        .submit(Box::new(move || {
                            let batch = decompress_batch(batch, decoder)?;
                            Ok(RawFromServer::ReadResponse(RawReadResponse {
                                bytes_size: batch.get_read_session_size(),
                                partition_data: vec![RawPartitionData {
                                    partition_session_id,
                                    batches: VecDeque::from([batch]),
                                }],
                            }))
                        }))
                        .await;
                }
            }
            other => {
                queue.submit(Box::new(move || Ok(other))).await;
            }
        }
    }
}

fn decoder_for_batch(
    codec_registry: &Arc<CodecRegistry>,
    batch: &RawBatch,
) -> YdbResult<Option<Arc<dyn CompressionDecoder>>> {
    let codec: Codec = batch.codec.into();
    if codec == Codec::RAW {
        return Ok(None);
    }

    codec_registry
        .get_decoder(codec)
        .map(Some)
        .ok_or_else(|| YdbError::custom(format!("no decoder found for codec {}", codec.code)))
}

async fn forward_loop(
    results_rx: TaskResultRx<RawFromServer>,
    runtime: RuntimeHandle,
    cancellation: CancellationToken,
) -> YdbResult<()> {
    select! {
        _ = cancellation.cancelled() => {
            debug!("decompressor forward cancelled, stopping");
            Ok(())
        }
        result = forward_events(results_rx, runtime) => {
            let Err(e) = result;
            Err(e)
        }
    }
}

async fn forward_events(
    mut results_rx: TaskResultRx<RawFromServer>,
    runtime: RuntimeHandle,
) -> YdbResult<Infallible> {
    loop {
        let Some(result_rx) = results_rx.recv().await else {
            return Err(YdbError::Transport(
                "decompressor results channel closed".into(),
            ));
        };
        let msg = result_rx
            .await
            .unwrap_or_else(|_| Err(YdbError::custom("executor decompression task panicked")))?;

        runtime.handle_from_server(msg)?;
    }
}

fn split_into_batches(resp: RawReadResponse, parallelism: NonZeroUsize) -> Vec<(i64, RawBatch)> {
    let total_messages: usize = resp
        .partition_data
        .iter()
        .flat_map(|partition_data| partition_data.batches.iter())
        .map(|batch| batch.message_data.len())
        .sum();
    let chunk_size = (total_messages / parallelism.get()).clamp(1, MAX_MESSAGES_PER_CHUNK);

    let mut batches = Vec::new();
    for partition_data in resp.partition_data {
        for batch in partition_data.batches {
            let RawBatch {
                producer_id,
                write_session_meta,
                codec,
                written_at,
                message_data,
            } = batch;

            let mut iter = message_data.into_iter();
            loop {
                let chunk: Vec<_> = iter.by_ref().take(chunk_size).collect();
                if chunk.is_empty() {
                    break;
                }

                batches.push((
                    partition_data.partition_session_id,
                    RawBatch {
                        producer_id: producer_id.clone(),
                        write_session_meta: write_session_meta.clone(),
                        codec,
                        written_at: written_at.clone(),
                        message_data: chunk,
                    },
                ));
            }
        }
    }

    batches
}

fn decompress_batch(
    mut batch: RawBatch,
    decoder: Option<Arc<dyn CompressionDecoder>>,
) -> YdbResult<RawBatch> {
    let Some(decoder) = decoder else {
        return Ok(batch);
    };

    for message in &mut batch.message_data {
        message.data = decoder.decode(&message.data).map_err(|err| {
            YdbError::custom(format!(
                "{decoder:?} failed to decode: {err}, message seq_no: {}, message offset: {}",
                message.seq_no, message.offset,
            ))
        })?;
    }

    Ok(batch)
}
