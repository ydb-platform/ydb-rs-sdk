use std::convert::Infallible;
use std::sync::Arc;

use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::trace;
use ydb_grpc::ydb_proto::topic::stream_write_message;

use crate::client_topic::compression::Executor;
use crate::client_topic::task_supervisor::{select_error, task_error};
use crate::client_topic::topicwriter::connection::ConnectionInfo;
use crate::client_topic::topicwriter::state::WriterState;
use crate::client_topic::topicwriter::stream_writer;
use crate::client_topic::topicwriter::write_request::WriteRequestSettings;
use crate::client_topic::topicwriter::writer_options::{TopicWriterOptions, WriterFlowControl};
use crate::errors::Idempotency;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::grpc_wrapper::raw_topic_service::client::RawTopicClient;
use crate::grpc_wrapper::raw_topic_service::stream_write::RawServerMessage;
use crate::{YdbError, YdbResult, closure};

pub(crate) struct ReconnectorParams {
    pub(crate) writer_options: TopicWriterOptions,
    pub(crate) producer_id: String,
    pub(crate) connection_manager: GrpcConnectionManager,
    pub(crate) shutdown_token: CancellationToken,
    pub(crate) executor: Arc<dyn Executor>,
}

/// Owns the transport-attempt lifecycle while [`WriterState`] owns accepted messages.
pub(crate) struct Reconnector {
    shutdown_token: CancellationToken,
    reconnection_task: JoinHandle<YdbResult<()>>,
    state: WriterState,
}

impl Reconnector {
    pub(crate) async fn new(params: ReconnectorParams) -> YdbResult<Self> {
        let ReconnectorParams {
            writer_options,
            producer_id,
            connection_manager,
            shutdown_token,
            executor,
        } = params;

        let flow_control = WriterFlowControl::try_from(&writer_options)?;
        let write_request_settings =
            WriteRequestSettings::new(connection_manager.max_message_size())?;
        let state = WriterState::new(writer_options.auto_seq_no, flow_control)?;

        let reconnection_loop = ReconnectionLoop {
            state: state.clone(),
            writer_options,
            connection_manager,
            shutdown_token: shutdown_token.clone(),
            producer_id,
            executor,
            write_request_settings,
        };
        let initial_connection = reconnection_loop
            .writer_options
            .retry_settings
            .retry_on_retriable_errors(
                Idempotency::Idempotent,
                closure!([&loop_state = &reconnection_loop], |_| loop_state
                    .establish_connection()),
            )
            .await?;
        state.initialize_last_seq_no(initial_connection.connection_info.last_seq_no_assigned)?;
        let reconnection_task = reconnection_loop.spawn(initial_connection)?;

        Ok(Reconnector {
            shutdown_token,
            reconnection_task,
            state,
        })
    }

    pub(crate) fn state(&self) -> WriterState {
        self.state.clone()
    }

    pub(crate) async fn stop(self) -> YdbResult<()> {
        let flush_result = self.state.flush().await;
        let state_failure_result = match &flush_result {
            Ok(()) => Ok(()),
            Err(error) => self.state.fail(error.clone()),
        };

        self.shutdown_token.cancel();

        let reconnection_result = self
            .reconnection_task
            .await
            .map_err(|err| {
                YdbError::custom(format!(
                    "stop: error while waiting for topic writer reconnection task: {err}"
                ))
            })
            .and_then(|result| result);

        state_failure_result?;
        flush_result?;
        reconnection_result?;
        self.state.ensure_not_failed()
    }
}

struct ReconnectionLoop {
    state: WriterState,
    writer_options: TopicWriterOptions,
    connection_manager: GrpcConnectionManager,
    shutdown_token: CancellationToken,
    producer_id: String,
    executor: Arc<dyn Executor>,
    write_request_settings: WriteRequestSettings,
}

impl ReconnectionLoop {
    fn spawn(
        self,
        initial_connection: EstablishedConnection,
    ) -> YdbResult<JoinHandle<YdbResult<()>>> {
        let epoch = self.state.epoch()?;
        let background_tasks = self.spawn_connection_tasks(initial_connection, epoch)?;
        Ok(tokio::spawn(self.run(background_tasks, epoch)))
    }

    async fn run(
        self,
        initial_background_tasks: JoinSet<YdbResult<Infallible>>,
        initial_epoch: usize,
    ) -> YdbResult<()> {
        let state = self.state.clone();
        let shutdown_token = self.shutdown_token.clone();

        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => Ok(()),
            result = self.reconnect_loop(initial_background_tasks, initial_epoch) => {
                let Err(error) = result;
                state.fail(error.clone())?;
                Err(error)
            }
        }
    }

    async fn reconnect_loop(
        self,
        mut background_tasks: JoinSet<YdbResult<Infallible>>,
        mut epoch: usize,
    ) -> YdbResult<Infallible> {
        loop {
            let Err(error) = wait_for_failure(background_tasks).await;
            trace!("topic writer connection failed: {error}");
            let retry_error = self.state.handle_connection_failure(epoch, error)?;
            self.state.wait_for_transaction_finish().await?;

            let established_connection = match retry_error {
                Some(error) => self.retry_after_failure(error).await?,
                None => self.establish_connection_with_retry().await?,
            };
            epoch = self.state.epoch()?;
            background_tasks = self.spawn_connection_tasks(established_connection, epoch)?;
        }
    }

    fn retry_after_failure(
        &self,
        error: YdbError,
    ) -> impl std::future::Future<Output = YdbResult<EstablishedConnection>> + '_ {
        // The retry helper performs an operation before its first wait. Feed the connection
        // failure through that operation so error classification and backoff remain owned by
        // RetrySettings; subsequent operations establish a new connection.
        let unclassified_error = Some(error);
        self.writer_options
            .retry_settings
            .retry_on_retriable_errors(
                Idempotency::Idempotent,
                closure!(
                    [&reconnection_loop = self, unclassified_error],
                    async |_| {
                        match unclassified_error.take() {
                            Some(error) => Err(error),
                            None => reconnection_loop.establish_connection().await,
                        }
                    }
                ),
            )
    }

    fn establish_connection_with_retry(
        &self,
    ) -> impl std::future::Future<Output = YdbResult<EstablishedConnection>> + '_ {
        self.writer_options
            .retry_settings
            .retry_on_retriable_errors(
                Idempotency::Idempotent,
                closure!([&reconnection_loop = self], |_| reconnection_loop
                    .establish_connection()),
            )
    }

    async fn establish_connection(&self) -> YdbResult<EstablishedConnection> {
        let init_request_body = stream_write_message::InitRequest {
            path: self.writer_options.topic_path.clone(),
            producer_id: self.producer_id.clone(),
            write_session_meta: self
                .writer_options
                .session_metadata
                .clone()
                .unwrap_or_default(),
            get_last_seq_no: self.writer_options.auto_seq_no,
            partitioning: Some(
                self.writer_options
                    .partitioning
                    .to_grpc_init_partitioning(self.producer_id.clone()),
            ),
        };
        let mut topic_service = self
            .connection_manager
            .get_auth_service(RawTopicClient::new)
            .await?;
        let mut stream = topic_service.stream_write(init_request_body).await?;
        let connection_info =
            ConnectionInfo::try_from(stream.receive::<RawServerMessage>().await?)?;
        Ok(EstablishedConnection {
            stream,
            connection_info,
        })
    }

    fn spawn_connection_tasks(
        &self,
        established: EstablishedConnection,
        epoch: usize,
    ) -> YdbResult<JoinSet<YdbResult<Infallible>>> {
        let EstablishedConnection {
            stream,
            connection_info,
        } = established;
        let server_codecs = connection_info.codecs_from_server.into();
        stream_writer::spawn_connection_tasks(
            self.writer_options.clone(),
            stream,
            self.state.clone(),
            epoch,
            server_codecs,
            self.executor.clone(),
            self.write_request_settings.clone(),
        )
    }
}

async fn drain_connection_failure(
    tasks: &mut JoinSet<YdbResult<Infallible>>,
    first_joined: Option<Result<YdbResult<Infallible>, tokio::task::JoinError>>,
) -> YdbError {
    let mut selected_error =
        first_joined.and_then(|joined| task_error(joined, "topic writer connection"));

    drain_connection_tasks(tasks, &mut selected_error).await;

    selected_error.unwrap_or_else(|| {
        YdbError::custom("topic writer connection tasks stopped without an error")
    })
}

async fn drain_connection_tasks(
    tasks: &mut JoinSet<YdbResult<Infallible>>,
    selected_error: &mut Option<YdbError>,
) {
    tasks.abort_all();

    while let Some(joined) = tasks.join_next().await {
        if matches!(&joined, Err(join_error) if join_error.is_cancelled()) {
            continue;
        }
        select_error(selected_error, joined, "topic writer connection");
    }
}

async fn wait_for_failure(mut tasks: JoinSet<YdbResult<Infallible>>) -> YdbResult<Infallible> {
    let first_joined = tasks.join_next().await;
    Err(drain_connection_failure(&mut tasks, first_joined).await)
}

struct EstablishedConnection {
    stream:
        AsyncGrpcStreamWrapper<stream_write_message::FromClient, stream_write_message::FromServer>,
    connection_info: ConnectionInfo,
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use tokio::sync::{mpsc, oneshot};

    use super::*;

    #[tokio::test]
    async fn aborts_and_drains_remaining_connection_tasks() {
        struct DropSignal(Option<oneshot::Sender<()>>);

        impl Drop for DropSignal {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let (dropped_tx, dropped_rx) = oneshot::channel();
        let drop_signal = DropSignal(Some(dropped_tx));
        let mut tasks: JoinSet<YdbResult<Infallible>> = JoinSet::new();
        tasks.spawn(async { Err(YdbError::custom("root task error")) });
        tasks.spawn(async move {
            let _drop_signal = drop_signal;
            pending().await
        });

        let error = wait_for_failure(tasks).await.unwrap_err();

        assert!(error.to_string().contains("root task error"));
        dropped_rx
            .await
            .expect("aborted connection task was not drained");
    }

    #[tokio::test]
    async fn releases_connection_resources_before_returning() {
        let (resource_tx, mut resource_rx) = mpsc::unbounded_channel::<()>();
        let mut tasks: JoinSet<YdbResult<Infallible>> = JoinSet::new();
        tasks.spawn(async { Err(YdbError::custom("root task error")) });
        tasks.spawn(async move {
            let _resource_tx = resource_tx;
            pending().await
        });

        wait_for_failure(tasks).await.unwrap_err();

        assert_eq!(resource_rx.recv().await, None);
    }

    #[tokio::test]
    async fn prefers_fatal_error_over_retriable_channel_closure() {
        let (finished_tx, mut finished_rx) = mpsc::unbounded_channel();
        let retriable_finished = finished_tx.clone();
        let mut tasks: JoinSet<YdbResult<Infallible>> = JoinSet::new();
        tasks.spawn(async move {
            retriable_finished.send(()).unwrap();
            Err(YdbError::Transport("dependent channel closed".to_string()))
        });
        tasks.spawn(async move {
            finished_tx.send(()).unwrap();
            Err(YdbError::custom("root task error"))
        });

        finished_rx.recv().await.unwrap();
        finished_rx.recv().await.unwrap();
        let error = wait_for_failure(tasks).await.unwrap_err();

        assert!(error.to_string().contains("root task error"));
    }
}
