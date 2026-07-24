use std::convert::Infallible;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument, warn};

use crate::client_common::TokenCache;
use crate::client_topic::compression::Executor;
use crate::errors::Idempotency;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawFromServer;
use crate::retry_budget::RetryState;
use crate::{YdbError, YdbResult, closure};

use super::auth_token_sender::AuthTokenSender;
use super::decompressor::Decompressor;
use super::grpc_streamer::GrpcStreamer;
use super::reader_options::TopicReaderOptions;
use super::runtime;
use super::task_supervisor::wait_child_tasks;

pub(super) struct ConnectionAttempt {
    pub(super) manager: GrpcConnectionManager,
    pub(super) options: TopicReaderOptions,
    pub(super) token_cache: TokenCache,
    pub(super) compression_executor: Arc<dyn Executor>,

    pub(super) cancellation_token: CancellationToken,

    pub(super) epoch: usize,
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
/// - Non-retriable: write the error into [`RuntimeHandle`](runtime::RuntimeHandle) (so the next
///   [`pop_batch`](runtime::RuntimeHandle::pop_batch) call returns it) and stop.
///
/// Dropping [`TopicReader`](super::reader::TopicReader) cancels the outer token, which makes the loop
/// return immediately without waiting for in-flight tasks. Cancelling
/// [`pop_batch`](runtime::RuntimeHandle::pop_batch) mid-flight is always safe: after
/// reconnect the server redelivers all messages since the last committed offset.
pub(super) struct Reconnector {
    manager: GrpcConnectionManager,
    reader_options: TopicReaderOptions,
    token_cache: TokenCache,
    compression_executor: Arc<dyn Executor>,
    pub(super) runtime: runtime::RuntimeHandle,
    cancellation_token: CancellationToken,
    reader_id: usize,
}

pub(super) struct ReconnectorTask {
    pub(super) join_handle: JoinHandle<YdbResult<()>>,
    pub(super) runtime: runtime::RuntimeHandle,
    pub(super) cancellation_token: CancellationToken,
}

impl Reconnector {
    pub(super) fn new(
        manager: GrpcConnectionManager,
        reader_options: TopicReaderOptions,
        token_cache: TokenCache,
        compression_executor: Arc<dyn Executor>,
        cancellation_token: CancellationToken,
        reader_id: usize,
    ) -> Self {
        let runtime = runtime::RuntimeHandle::new(reader_id);

        Self {
            manager,
            reader_options,
            token_cache,
            compression_executor,
            runtime,
            cancellation_token,
            reader_id,
        }
    }

    pub(super) fn run(self) -> ReconnectorTask {
        let runtime = self.runtime.clone();
        let cancellation_token = self.cancellation_token.clone();
        let join_handle = tokio::spawn(self.run_task());

        ReconnectorTask {
            join_handle,
            runtime,
            cancellation_token,
        }
    }

    async fn run_task(self) -> YdbResult<()> {
        let runtime = self.runtime.clone();
        let cancellation_token = self.cancellation_token.clone();

        tokio::select! {
            _ = cancellation_token.cancelled() => {
                Ok(())
            }

            err = self.reconnect_loop() => {
                let Err(err) = err;
                let _ = runtime.fail(&err);
                Err(err)
            }
        }
    }

    #[instrument(skip_all, err)]
    async fn reconnect_loop(self) -> YdbResult<Infallible> {
        let Self {
            manager,
            reader_options,
            token_cache,
            compression_executor,
            runtime,
            cancellation_token,
            reader_id,
        } = self;

        let mut attempt_ctx = ConnectionAttempt {
            manager,
            options: reader_options,
            token_cache,
            compression_executor,
            cancellation_token: cancellation_token.child_token(),
            epoch: 0,
            reader_id,
        };

        loop {
            info!(
                reader_id = attempt_ctx.reader_id,
                epoch = attempt_ctx.epoch,
                "topic reader reconnector starting connection"
            );

            let mut final_retry = RetryState::init();

            let tasks = attempt_ctx
                .options
                .retry_settings
                .retry_on_retriable_errors(
                    Idempotency::Idempotent,
                    closure!(
                        [&attempt_ctx, &runtime, &mut final_retry],
                        async |retry: &RetryState| {
                            *final_retry = *retry;
                            Self::establish(attempt_ctx, runtime).await
                        }
                    ),
                )
                .await?;

            info!(
                connect_attempts = final_retry.attempt,
                reader_id = attempt_ctx.reader_id,
                time = ?final_retry.start_time.elapsed(),
                "topic reader connected"
            );

            tokio::select! {
                _ = runtime.reconnection_notifier() => {
                    info!(
                        reader_id = attempt_ctx.reader_id,
                        epoch = attempt_ctx.epoch,
                        "topic reader forced reconnect requested"
                    );
                }

                err = Self::run_connection(&attempt_ctx, tasks) => {
                    match err {
                        Err(err) if err.is_retriable(Idempotency::Idempotent) => {
                            warn!(
                                error = %err,
                                reader_id = attempt_ctx.reader_id,
                                epoch = attempt_ctx.epoch,
                                "topic reader connection failed, will reconnect"
                            );
                            runtime.enter_reconnecting(YdbError::Transport(format!(
                                "topic reader reconnect, dropping connection epoch {}: {err}",
                                attempt_ctx.epoch
                            )))?;
                        }

                        Err(err) => {
                            error!(error = %err, "non-retriable error, exiting");
                            return Err(err);
                        }
                    }
                }
            }

            attempt_ctx.cancellation_token = cancellation_token.child_token();
            attempt_ctx.epoch += 1;
        }
    }

    async fn establish(
        attempt_ctx: &ConnectionAttempt,
        runtime: &runtime::RuntimeHandle,
    ) -> YdbResult<tokio::task::JoinSet<YdbResult<()>>> {
        let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel();
        let (decomp_input_tx, decomp_input_rx) = mpsc::unbounded_channel::<RawFromServer>();

        let grpc = GrpcStreamer::new(attempt_ctx, decomp_input_tx, outgoing_rx).await?;

        runtime.install_connection(
            runtime::Connection::new(outgoing_tx.clone(), attempt_ctx.epoch),
            YdbError::Transport(format!(
                "topic reader switching to connection epoch {}",
                attempt_ctx.epoch
            )),
        )?;

        let decompressor = Decompressor::new(attempt_ctx, decomp_input_rx, runtime.clone());
        let auth_token_sender = AuthTokenSender::new(attempt_ctx, outgoing_tx);

        let mut tasks: JoinSet<YdbResult<()>> = JoinSet::new();
        tasks.spawn(grpc.run());
        tasks.spawn(decompressor.run());
        tasks.spawn(auth_token_sender.run());

        Ok(tasks)
    }

    async fn run_connection(
        attempt_ctx: &ConnectionAttempt,
        tasks: JoinSet<YdbResult<()>>,
    ) -> YdbResult<Infallible> {
        match wait_child_tasks(
            &attempt_ctx.cancellation_token,
            tasks,
            "topic reader connection",
        )
        .await
        {
            Ok(()) => Err(YdbError::custom("topic reader connection cancelled")),
            Err(err) => Err(err),
        }
    }
}
