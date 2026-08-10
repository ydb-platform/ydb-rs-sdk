use std::ops::ControlFlow;
use std::sync::Arc;

use futures_util::FutureExt;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, trace};
use ydb_grpc::ydb_proto::topic::stream_write_message;

use crate::client_topic::compression::Executor;
use crate::client_topic::topicwriter::connection::ConnectionInfo;
use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::message_write_status::{
    MessageWriteStatus, MessageWriteStatusValidator,
};
use crate::client_topic::topicwriter::queue::Queue;
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
    pub(crate) fatal_error_tx: oneshot::Sender<YdbError>,
    pub(crate) executor: Arc<dyn Executor>,
    pub(crate) tx_identity: Option<TransactionIdentity>,
    pub(crate) status_validator: MessageWriteStatusValidator,
}

#[derive(Clone)]
enum ReconnectorStatus {
    Working,
    FinishedWithError(YdbError),
    Stopped,
}

impl ReconnectorStatus {
    pub(crate) fn check_working(&self) -> YdbResult<()> {
        match self {
            ReconnectorStatus::Working => Ok(()),
            ReconnectorStatus::FinishedWithError(err) => Err(err.clone()),
            ReconnectorStatus::Stopped => Err(YdbError::custom("is stopped")),
        }
    }
}

pub(crate) struct Reconnector {
    cancellation_token: CancellationToken,
    reconnect_loop: JoinHandle<()>,
    queue: Queue,
    status_rx: watch::Receiver<ReconnectorStatus>,
}

impl Reconnector {
    pub(crate) async fn new(params: ReconnectorParams) -> YdbResult<Self> {
        let flow_control = WriterFlowControl::try_from(&params.writer_options)?;
        let write_request_settings = WriteRequestSettings::new(
            params.tx_identity,
            params.connection_manager.max_message_size(),
        )?;
        let queue = Queue::new_with_status_validator(
            params.status_validator,
            params.writer_options.auto_seq_no,
            flow_control,
        )?;
        let cancellation_token = params.cancellation_token;

        let (init_tx, init_rx) = oneshot::channel();
        let (status_tx, status_rx) = watch::channel(ReconnectorStatus::Working);

        let reconnect_loop = Reconnector::start_reconnection_loop(
            ReconnectionHelper {
                connection_manager: params.connection_manager,
                retry_settings: params.retry_settings,
                cancellation_token: cancellation_token.clone(),
                writer_options: params.writer_options,
                producer_id: params.producer_id,
                queue: queue.clone(),
                executor: params.executor,
                write_request_settings,
            },
            params.fatal_error_tx,
            init_tx,
            status_tx,
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
        queue
            .initialize_last_seq_no(connection_info.last_seq_no_assigned)
            .await?;

        Ok(Reconnector {
            cancellation_token: cancellation_token.clone(),
            reconnect_loop,
            queue,
            status_rx,
        })
    }

    fn start_reconnection_loop(
        helper: ReconnectionHelper,
        fatal_error_tx: oneshot::Sender<YdbError>,
        init_tx: oneshot::Sender<YdbResult<ConnectionInfo>>,
        status_tx: watch::Sender<ReconnectorStatus>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            ReconnectionLoop::new(helper, init_tx, status_tx)
                .run(fatal_error_tx)
                .await
        })
    }

    pub(crate) async fn add_message(
        &self,
        message: TopicWriterMessage,
        ack_sender: Option<oneshot::Sender<YdbResult<MessageWriteStatus>>>,
    ) -> YdbResult<()> {
        self.check_working()?;
        self.queue.add_message(message, ack_sender).await
    }

    pub(crate) async fn flush(&self) -> YdbResult<()> {
        self.check_working()?;
        self.queue.flush().await
    }

    pub(crate) async fn stop(self) -> YdbResult<()> {
        self.queue.close_for_new_messages().await;
        let flush_result = self.flush().await;

        self.cancellation_token.cancel();

        let reconnector_result = self.stop_inner().await;

        flush_result?;
        reconnector_result?;

        Ok(())
    }

    async fn stop_inner(self) -> YdbResult<()> {
        match self.status() {
            ReconnectorStatus::Working => {
                self.reconnect_loop.await.map_err(|err| {
                    YdbError::custom(format!(
                        "stop: error while waiting for reconnection_loop to finish: {err}"
                    ))
                })?;
            }
            ReconnectorStatus::FinishedWithError(err) => return Err(err.clone()),
            ReconnectorStatus::Stopped => return Ok(()),
        }

        Ok(())
    }

    fn status(&self) -> ReconnectorStatus {
        self.status_rx.borrow().clone()
    }

    fn check_working(&self) -> YdbResult<()> {
        self.status().check_working()
    }
}

struct ReconnectionHelper {
    queue: Queue,
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
    async fn recreate_stream_writer(
        &self,
        error_sender: oneshot::Sender<YdbError>,
    ) -> YdbResult<RecreateStreamWriterResult> {
        self.queue.reset_progress().await;

        let mut stream = self.connect().await?;
        let init_response = ConnectionInfo::try_from(stream.receive::<RawServerMessage>().await?)?;
        let server_codecs = init_response.codecs_from_server.clone().into();

        Ok(RecreateStreamWriterResult {
            stream_writer: StreamWriter::new(
                self.writer_options.clone(),
                stream,
                self.queue.clone(),
                error_sender,
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
    status_tx: watch::Sender<ReconnectorStatus>,
    retry: Option<RetryState>,
    stream_writer: Option<StreamWriter>,
}

#[derive(Debug)]
enum ReconnectionLoopStatus {
    HandleError(YdbError),
    RecreateStreamWriter,
    WaitForErrorOrCancellation(oneshot::Receiver<YdbError>),
    Exit(Option<YdbError>),
}

impl ReconnectionLoop {
    fn new(
        helper: ReconnectionHelper,
        init_tx: oneshot::Sender<YdbResult<ConnectionInfo>>,
        status_tx: watch::Sender<ReconnectorStatus>,
    ) -> Self {
        Self {
            helper,
            init_tx: Some(init_tx),
            status_tx,
            retry: Some(RetryState::init()),
            stream_writer: None,
        }
    }

    async fn run(&mut self, fatal_error_tx: oneshot::Sender<YdbError>) {
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
                ReconnectionLoopStatus::WaitForErrorOrCancellation(error_receiver) => {
                    deadline = retry_settings.wait_deadline().boxed();
                    self.wait_for_error_or_cancellation(error_receiver).await
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
            self.update_status(ReconnectorStatus::FinishedWithError(final_error.clone()));
            self.helper.queue.close_for_new_messages().await;
            self.helper
                .queue
                .notify_reception_tickets(final_error.clone())
                .await;

            if let Some(tx) = self.init_tx.take() {
                let _ = tx.send(Err(final_error.clone()));
            }

            if let Err(err) = fatal_error_tx.send(final_error) {
                error!("can't send fatal error to TopicWriter: channel is closed: {err}");
            }
        } else {
            self.update_status(ReconnectorStatus::Stopped);
        }
    }

    fn update_status(&self, status: ReconnectorStatus) {
        if let Err(err) = self.status_tx.send(status) {
            error!("can't update status: status channel is closed: {err}");
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

        let (error_sender, error_receiver) = oneshot::channel();

        if self.retry.is_none() {
            self.retry = Some(RetryState::init());
        }

        match self.helper.recreate_stream_writer(error_sender).await {
            Ok(swr) => {
                self.stream_writer = Some(swr.stream_writer);
                self.retry = None;

                if let Some(tx) = self.init_tx.take() {
                    let _ = tx.send(Ok(swr.connection_info));
                }

                ReconnectionLoopStatus::WaitForErrorOrCancellation(error_receiver)
            }
            Err(err) => {
                trace!("error creating stream writer: {err}");

                ReconnectionLoopStatus::HandleError(err)
            }
        }
    }

    async fn wait_for_error_or_cancellation(
        &mut self,
        error_receiver: oneshot::Receiver<YdbError>,
    ) -> ReconnectionLoopStatus {
        tokio::select! {
            _ = self.helper.cancellation_token.cancelled() => ReconnectionLoopStatus::Exit(None),
            received_err = error_receiver => match received_err {
                Ok(err) => {
                    ReconnectionLoopStatus::HandleError(err)
                },
                Err(chan_err) => ReconnectionLoopStatus::Exit(Some(YdbError::custom(format!("error channel error: {chan_err}"))))
            },
        }
    }
}
