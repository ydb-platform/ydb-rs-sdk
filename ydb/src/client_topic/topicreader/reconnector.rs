use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::client_common::TokenCache;
use crate::client_topic::compression::Executor;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::retry::{Retry, RetryParams};
use crate::{YdbError, YdbResult};

use super::decompressor::Decompressor;
use super::grpc_streamer::GrpcStreamer;
use super::messages::MessageBatch;
use super::reader_options::TopicReaderOptions;
use super::storage::SharedStorage;
use super::task_supervisor::{is_retriable, wait_child_tasks};
use super::tokenizer::Tokenizer;

pub(super) struct Context {
    pub(super) manager: GrpcConnectionManager,
    pub(super) options: TopicReaderOptions,
    pub(super) token_cache: TokenCache,
    pub(super) compression_executor: Arc<dyn Executor>,

    pub(super) cancellation: CancellationToken,

    pub(super) connection_epoch: usize,
    pub(super) reader_id: usize,
}

/// Manages the topic reader connection loop in a background task.
///
/// Each connection runs three sibling tasks for its lifetime:
/// ```text
/// connection (one epoch)
///   |- GrpcStreamer   (receive_loop, send_loop)
///   |- Decompressor   (schedule_loop, forward_loop)
///   `- Tokenizer
/// ```
///
/// Errors bubble up from tasks to the reconnector:
/// - Retriable: cancel the current connection's tasks, bump the epoch, establish a new connection.
/// - Non-retriable: write the error into [`SharedStorage`] (so the next
///   [`pop_batch`](SharedStorage::pop_batch) call returns it) and stop.
///
/// Dropping [`TopicReader`](super::reader::TopicReader) cancels the outer token, which makes the loop
/// return immediately without waiting for in-flight tasks. Cancelling
/// [`pop_batch`](SharedStorage::pop_batch) mid-flight is always safe: after
/// reconnect the server redelivers all messages since the last committed offset.
pub(super) struct Reconnector {
    manager: GrpcConnectionManager,
    reader_options: TopicReaderOptions,
    token_cache: TokenCache,
    compression_executor: Arc<dyn Executor>,
    pub(super) shared_storage: SharedStorage,
    cancellation: CancellationToken,
    reader_id: usize,
}

pub(super) struct RunningReconnector {
    pub(super) handle: JoinHandle<YdbResult<()>>,
    pub(super) shared_storage: SharedStorage,
    pub(super) cancellation: CancellationToken,
}

impl Reconnector {
    pub(super) fn new(
        manager: GrpcConnectionManager,
        reader_options: TopicReaderOptions,
        token_cache: TokenCache,
        compression_executor: Arc<dyn Executor>,
        cancellation: CancellationToken,
        reader_id: usize,
    ) -> Self {
        let shared_storage = SharedStorage::connecting();

        Self {
            manager,
            reader_options,
            token_cache,
            compression_executor,
            shared_storage,
            cancellation,
            reader_id,
        }
    }

    pub(super) fn run(self) -> RunningReconnector {
        let shared_storage = self.shared_storage.clone();
        let cancellation = self.cancellation.clone();
        let handle = tokio::spawn(self.run_task());

        RunningReconnector {
            handle,
            shared_storage,
            cancellation,
        }
    }

    async fn run_task(self) -> YdbResult<()> {
        let shared_storage = self.shared_storage.clone();
        let cancellation = self.cancellation.clone();

        tokio::select! {
            _ = cancellation.cancelled() => {
                Ok(())
            }

            err = self.reconnect_loop() => {
                let Err(err) = err;
                let _ = shared_storage.fail(&err);
                Err(err)
            }
        }
    }

    async fn reconnect_loop(self) -> YdbResult<Infallible> {
        let Self {
            manager,
            reader_options,
            token_cache,
            compression_executor,
            shared_storage,
            cancellation,
            reader_id,
        } = self;

        let mut ctx = Context {
            manager,
            options: reader_options,
            token_cache,
            compression_executor,
            cancellation: cancellation.child_token(),
            connection_epoch: 0,
            reader_id,
        };

        loop {
            info!(
                reader_id = ctx.reader_id,
                connection_epoch = ctx.connection_epoch,
                "topic reader reconnector starting connection"
            );

            let start_time = Instant::now();
            let mut attempt = 0;

            let tasks = loop {
                match Self::establish(&ctx, &shared_storage).await {
                    Ok(tasks) => break tasks,

                    Err(err) if is_retriable(&err) => {
                        warn!(
                            error = %err,
                            reader_id = ctx.reader_id,
                            epoch = ctx.connection_epoch,
                            connect_attempt = attempt,
                            "topic reader connection setup failed, will retry"
                        );

                        wait_or_fail(
                            err,
                            ctx.options.retrier.as_ref(),
                            attempt,
                            start_time.elapsed(),
                        )
                        .await?;
                        attempt += 1;
                    }

                    Err(err) => {
                        error!(error = %err, "non-retriable error, exiting");
                        return Err(err);
                    }
                }
            };

            info!(
                connect_attempts = attempt,
                reader_id = ctx.reader_id,
                time = ?start_time.elapsed(),
                "topic reader connected"
            );

            match Self::run_connection(&ctx, tasks).await {
                Err(err) if is_retriable(&err) => {
                    warn!(
                        error = %err,
                        reader_id = ctx.reader_id,
                        epoch = ctx.connection_epoch,
                        "topic reader connection failed, will reconnect"
                    );
                    shared_storage.begin_connecting(YdbError::Transport(format!(
                        "topic reader reconnect, dropping connection epoch {}: {err}",
                        ctx.connection_epoch
                    )))?;
                }

                Err(err) => {
                    error!(error = %err, "non-retriable error, exiting");
                    return Err(err);
                }
            }

            ctx.cancellation = cancellation.child_token();
            ctx.connection_epoch += 1;
        }
    }

    async fn establish(
        ctx: &Context,
        shared_storage: &SharedStorage,
    ) -> YdbResult<tokio::task::JoinSet<YdbResult<()>>> {
        let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel();
        let (decomp_input_tx, decomp_input_rx) = mpsc::unbounded_channel::<MessageBatch>();

        let grpc =
            GrpcStreamer::new(ctx, decomp_input_tx, outgoing_rx, shared_storage.clone()).await?;

        shared_storage.recreate(
            outgoing_tx.clone(),
            ctx.connection_epoch,
            YdbError::Transport(format!(
                "topic reader switching to connection epoch {}",
                ctx.connection_epoch
            )),
        )?;

        let decompressor = Decompressor::new(ctx, decomp_input_rx, shared_storage.clone());
        let tokenizer = Tokenizer::new(ctx, outgoing_tx);

        let mut tasks: JoinSet<YdbResult<()>> = JoinSet::new();
        tasks.spawn(grpc.run());
        tasks.spawn(decompressor.run());
        tasks.spawn(tokenizer.run());

        Ok(tasks)
    }

    async fn run_connection(ctx: &Context, tasks: JoinSet<YdbResult<()>>) -> YdbResult<Infallible> {
        match wait_child_tasks(&ctx.cancellation, tasks, "topic reader connection").await {
            Ok(()) => Err(YdbError::custom("topic reader connection cancelled")),
            Err(err) => Err(err),
        }
    }
}

/// Fetches [`Retry::wait_duration`]. If retries are not allowed, returns `Err(err)`. Otherwise, waits
/// for requested duration and returns `Ok(())`.
async fn wait_or_fail(
    err: YdbError,
    retrier: &dyn Retry,
    attempt: usize,
    time_from_start: Duration,
) -> YdbResult<()> {
    let decision = retrier.wait_duration(RetryParams {
        attempt,
        time_from_start,
    });

    if !decision.allow_retry {
        error!(error = %err, attempt, ?time_from_start, "retry budget exhausted");
        return Err(err);
    }

    tokio::time::sleep(decision.wait_timeout).await;
    Ok(())
}
