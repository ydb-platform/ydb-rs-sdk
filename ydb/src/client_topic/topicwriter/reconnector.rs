use std::ops::ControlFlow;
use std::sync::Arc;

use futures_util::FutureExt;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, trace};
use ydb_grpc::ydb_proto::topic::stream_write_message;

use crate::client_topic::compression::Executor;
use crate::client_topic::topicwriter::connection::ConnectionInfo;
use crate::client_topic::topicwriter::message_write_status::MessageWriteStatusValidator;
use crate::client_topic::topicwriter::queue::WriterRuntime;
use crate::client_topic::topicwriter::stream_writer::StreamWriter;
use crate::client_topic::topicwriter::write_request::WriteRequestSettings;
use crate::client_topic::topicwriter::writer_options::{TopicWriterOptions, WriterFlowControl};
use crate::errors::NeedRetry;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::grpc_wrapper::raw_topic_service::client::RawTopicClient;
use crate::grpc_wrapper::raw_topic_service::stream_write::RawServerMessage;
use crate::retry_settings::{RetrySettings, RetryState};
use crate::{YdbError, YdbResult};
use ydb_grpc::ydb_proto::topic::TransactionIdentity;

pub(crate) struct ReconnectorParams {
    pub(crate) writer_options: TopicWriterOptions,
    pub(crate) producer_id: String,
    pub(crate) connection_manager: GrpcConnectionManager,
    pub(crate) cancellation_token: CancellationToken,
    pub(crate) retry_settings: RetrySettings,
    pub(crate) executor: Arc<dyn Executor>,
    pub(crate) tx_identity: Option<TransactionIdentity>,
    pub(crate) status_validator: MessageWriteStatusValidator,
}

pub(crate) struct Reconnector {
    cancellation_token: CancellationToken,
    reconnect_loop: JoinHandle<()>,
    runtime: WriterRuntime,
}

impl Reconnector {
    pub(crate) async fn new(params: ReconnectorParams) -> YdbResult<Self> {
        let flow_control = WriterFlowControl::try_from(&params.writer_options)?;
        let write_request_settings = WriteRequestSettings::new(
            params.tx_identity,
            params.connection_manager.max_message_size(),
        )?;
        let runtime = WriterRuntime::new_with_status_validator(
            params.status_validator,
            params.writer_options.auto_seq_no,
            flow_control,
        )?;
        let cancellation_token = params.cancellation_token;

        let (init_tx, init_rx) = oneshot::channel();

        let reconnect_loop = Reconnector::start_reconnection_loop(
            ReconnectionHelper {
                connection_manager: params.connection_manager,
                retry_settings: params.retry_settings,
                cancellation_token: cancellation_token.clone(),
                writer_options: params.writer_options,
                producer_id: params.producer_id,
                runtime: runtime.clone(),
                executor: params.executor,
                write_request_settings,
            },
            init_tx,
        );

        let connection_info = match init_rx.await {
            Ok(Ok(connection_info)) => connection_info,
            Ok(Err(err)) => {
                return Err(err);
            }
            Err(err) => {
                return Err(YdbError::from(err));
            }
        };
        runtime.initialize_last_seq_no(connection_info.last_seq_no_assigned)?;

        Ok(Reconnector {
            cancellation_token: cancellation_token.clone(),
            reconnect_loop,
            runtime,
        })
    }

    fn start_reconnection_loop(
        helper: ReconnectionHelper,
        init_tx: oneshot::Sender<YdbResult<ConnectionInfo>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move { ReconnectionLoop::new(helper, init_tx).run().await })
    }

    pub(crate) fn runtime(&self) -> WriterRuntime {
        self.runtime.clone()
    }

    pub(crate) async fn stop(self) -> YdbResult<()> {
        let flush_result = self.runtime.flush().await;

        self.cancellation_token.cancel();

        let reconnector_result = self.reconnect_loop.await.map_err(|err| {
            YdbError::custom(format!(
                "stop: error while waiting for reconnection_loop to finish: {err}"
            ))
        });

        flush_result?;
        reconnector_result?;
        self.runtime.ensure_available()
    }
}

struct ReconnectionHelper {
    runtime: WriterRuntime,
    writer_options: TopicWriterOptions,
    connection_manager: GrpcConnectionManager,
    retry_settings: RetrySettings,
    cancellation_token: CancellationToken,
    producer_id: String,
    executor: Arc<dyn Executor>,
    write_request_settings: WriteRequestSettings,
}

struct RecreateStreamWriterResult {
    stream_writer: StreamWriter,
    connection_info: ConnectionInfo,
}

impl ReconnectionHelper {
    async fn recreate_stream_writer(&self) -> YdbResult<RecreateStreamWriterResult> {
        self.runtime.reset_progress()?;

        let mut stream = self.connect().await?;
        let init_response = ConnectionInfo::try_from(stream.receive::<RawServerMessage>().await?)?;
        let server_codecs = init_response.codecs_from_server.clone().into();

        Ok(RecreateStreamWriterResult {
            stream_writer: StreamWriter::new(
                self.writer_options.clone(),
                stream,
                self.runtime.clone(),
                server_codecs,
                self.executor.clone(),
                self.write_request_settings.clone(),
            )
            .await?,
            connection_info: init_response,
        })
    }

    async fn connect(
        &self,
    ) -> YdbResult<
        AsyncGrpcStreamWrapper<stream_write_message::FromClient, stream_write_message::FromServer>,
    > {
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

        let stream = topic_service
            .stream_write(init_request_body.clone())
            .await?;

        Ok(stream)
    }

    fn is_retry_allowed(err: &YdbError) -> bool {
        match err.need_retry() {
            NeedRetry::True => true,
            // IdempotentOnly errors are retryable because the
            // 'Write to Topic With seq_no deduplication' operation is idempotent.
            NeedRetry::IdempotentOnly => true,
            NeedRetry::False => false,
        }
    }

    async fn wait_before_reconnect(&self, retry: &RetryState) -> Option<ControlFlow<()>> {
        tokio::select! {
            biased;
            _ = self.cancellation_token.cancelled() => None,
            result = self.retry_settings.wait_retry(retry) => Some(result)
        }
    }
}

struct ReconnectionLoop {
    helper: ReconnectionHelper,
    init_tx: Option<oneshot::Sender<YdbResult<ConnectionInfo>>>,
    retry: Option<RetryState>,
    stream_writer: Option<StreamWriter>,
}

#[derive(Debug)]
enum ReconnectionLoopStatus {
    HandleError(YdbError),
    RecreateStreamWriter,
    WaitForErrorOrCancellation,
    Exit(Option<YdbError>),
}

impl ReconnectionLoop {
    fn new(
        helper: ReconnectionHelper,
        init_tx: oneshot::Sender<YdbResult<ConnectionInfo>>,
    ) -> Self {
        Self {
            helper,
            init_tx: Some(init_tx),
            retry: Some(RetryState::init()),
            stream_writer: None,
        }
    }

    async fn run(&mut self) {
        let retry_settings = self.helper.retry_settings.clone();
        let mut deadline = retry_settings.wait_deadline().boxed();

        let mut status = ReconnectionLoopStatus::RecreateStreamWriter;

        let final_result = loop {
            status = match status {
                ReconnectionLoopStatus::HandleError(err) => {
                    RetrySettings::run_with_deadline(&mut deadline, self.handle_error(err))
                        .await
                        .unwrap_or(ReconnectionLoopStatus::Exit(Some(
                            YdbError::DeadlineExceeded,
                        )))
                }
                ReconnectionLoopStatus::RecreateStreamWriter => self.recreate_stream_writer().await,
                ReconnectionLoopStatus::WaitForErrorOrCancellation => {
                    deadline = retry_settings.wait_deadline().boxed();
                    self.wait_for_error_or_cancellation().await
                }
                ReconnectionLoopStatus::Exit(err) => {
                    break err;
                }
            };
        };

        if let Some(stream_writer) = self.stream_writer.take() {
            let _ = stream_writer.stop().await;
        }

        if let Some(final_error) = final_result {
            if let Some(tx) = self.init_tx.take() {
                let _ = tx.send(Err(final_error.clone()));
            }

            if let Err(err) = self.helper.runtime.fail(final_error) {
                error!("failed to store terminal topic writer error: {err}");
            }
        }
    }

    async fn handle_error(&mut self, err: YdbError) -> ReconnectionLoopStatus {
        if !ReconnectionHelper::is_retry_allowed(&err) {
            trace!("reconnect is not allowed for error: {err}");
            return ReconnectionLoopStatus::Exit(Some(err));
        }

        trace!("error, trying to reconnect: {err}");

        let retry = self.retry.unwrap_or_else(RetryState::init);
        let state = match self.helper.wait_before_reconnect(&retry).await {
            // Retry budget blocked the retry
            Some(ControlFlow::Break(())) => ReconnectionLoopStatus::Exit(Some(err)),
            Some(ControlFlow::Continue(())) => ReconnectionLoopStatus::RecreateStreamWriter,
            None => ReconnectionLoopStatus::Exit(None),
        };
        if let Some(retry) = &mut self.retry {
            retry.attempt += 1;
        }

        state
    }

    async fn recreate_stream_writer(&mut self) -> ReconnectionLoopStatus {
        if self.helper.cancellation_token.is_cancelled() {
            return ReconnectionLoopStatus::Exit(None);
        }

        // Wait ending old stream writer before recreating
        if let Some(old) = self.stream_writer.take()
            && let Err(err) = old.stop().await
        {
            return ReconnectionLoopStatus::HandleError(err);
        }

        if self.retry.is_none() {
            self.retry = Some(RetryState::init());
        }

        match self.helper.recreate_stream_writer().await {
            Ok(swr) => {
                self.stream_writer = Some(swr.stream_writer);
                self.retry = None;

                if let Some(tx) = self.init_tx.take() {
                    let _ = tx.send(Ok(swr.connection_info));
                }

                ReconnectionLoopStatus::WaitForErrorOrCancellation
            }
            Err(err) => {
                trace!("error creating stream writer: {err}");

                ReconnectionLoopStatus::HandleError(err)
            }
        }
    }

    async fn wait_for_error_or_cancellation(&mut self) -> ReconnectionLoopStatus {
        let Some(stream_writer) = &mut self.stream_writer else {
            return ReconnectionLoopStatus::Exit(Some(YdbError::custom(
                "topic writer connection is missing while waiting for its result",
            )));
        };

        tokio::select! {
            _ = self.helper.cancellation_token.cancelled() => ReconnectionLoopStatus::Exit(None),
            result = stream_writer.wait() => match result {
                Ok(()) => ReconnectionLoopStatus::Exit(Some(YdbError::custom(
                    "topic writer connection stopped without an error or cancellation",
                ))),
                Err(err) => ReconnectionLoopStatus::HandleError(err),
            },
        }
    }
}
