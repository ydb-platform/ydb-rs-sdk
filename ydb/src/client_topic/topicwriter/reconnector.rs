use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use tokio::sync::oneshot;
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::log::{error, trace};
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

use crate::client_topic::topicwriter::message_queue::MessageQueue;
use crate::client_topic::topicwriter::message_write_status::MessageWriteStatus;
use crate::client_topic::topicwriter::stream_writer::StreamWriter;
use crate::client_topic::topicwriter::writer::{TopicWriterStatus, WriterState};
use crate::client_topic::topicwriter::writer_reception_queue::{
    TopicWriterReceptionTicket, TopicWriterReceptionType,
};
use crate::errors::NeedRetry;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::retry::{Retry, RetryParams};
use crate::{TopicWriterMessage, TopicWriterOptions, YdbError, YdbResult};

pub(crate) struct ReconnectorParams {
    pub(crate) writer_options: TopicWriterOptions,
    pub(crate) producer_id: String,
    pub(crate) connection_manager: GrpcConnectionManager,
    pub(crate) writer_state: Arc<TokioMutex<WriterState>>,
    pub(crate) cancellation_token: CancellationToken,
    pub(crate) message_queue: MessageQueue,
    pub(crate) retrier: Arc<dyn Retry>,
    pub(crate) init_tx: oneshot::Sender<YdbResult<()>>,
    pub(crate) fatal_error_tx: oneshot::Sender<YdbError>,
}

enum ReconnectorStatus {
    Created,
    Working,
    Stopped,
}

struct ReconnectorState {
    status: ReconnectorStatus,
    loop_handle: Option<JoinHandle<()>>,
}

pub(crate) struct Reconnector {
    state: Arc<TokioMutex<ReconnectorState>>,
    writer_state: Arc<TokioMutex<WriterState>>,
    pub(crate) message_queue: MessageQueue,
}

impl Reconnector {
    pub(crate) async fn new(params: ReconnectorParams) -> Self {
        let r = Reconnector {
            state: Arc::new(TokioMutex::new(ReconnectorState {
                status: ReconnectorStatus::Created,
                loop_handle: None,
            })),
            message_queue: params.message_queue.clone(),
            writer_state: params.writer_state.clone(),
        };

        let loop_join_handle = Reconnector::start_reconnection_loop(
            ReconnectionHelper {
                connection_manager: params.connection_manager,
                retrier: params.retrier,
                cancellation_token: params.cancellation_token,
                writer_state: params.writer_state,
                writer_options: params.writer_options,
                producer_id: params.producer_id,
            },
            params.message_queue,
            params.fatal_error_tx,
            params.init_tx,
        )
        .await;

        {
            let mut state = r.state.lock().await;
            state.loop_handle = Some(loop_join_handle);
            state.status = ReconnectorStatus::Working;
        }

        r
    }

    async fn start_reconnection_loop(
        helper: ReconnectionHelper,
        message_queue: MessageQueue,
        fatal_error_tx: oneshot::Sender<YdbError>,
        init_tx: oneshot::Sender<YdbResult<()>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            ReconnectionLoop::new(helper, message_queue, init_tx)
                .run(fatal_error_tx)
                .await
        })
    }

    pub(crate) async fn add_message_for_processing(
        &self,
        message: TopicWriterMessage,
        ack: Option<oneshot::Sender<YdbResult<MessageWriteStatus>>>,
    ) -> YdbResult<()> {
        let data_size = message.data.len() as i64;

        let seq_no = message
            .seq_no
            .ok_or_else(|| YdbError::custom("empty message seq_no is provided"))?;

        let reception_type = ack.map_or(
            TopicWriterReceptionType::NoConfirmationExpected,
            TopicWriterReceptionType::AwaitingConfirmation,
        );

        let duration = message.created_at.duration_since(UNIX_EPOCH)?;
        self.message_queue
            .add_message(MessageData {
                seq_no,
                created_at: Some(ydb_grpc::google_proto_workaround::protobuf::Timestamp {
                    seconds: duration.as_secs() as i64,
                    nanos: duration.subsec_nanos() as i32,
                }),
                metadata_items: vec![],
                data: message.data,
                uncompressed_size: data_size,
                partitioning: None,
            })
            .await?;

        {
            let mut writer_state = self.writer_state.lock().await;
            writer_state
                .confirmation_reception_queue
                .add_ticket(TopicWriterReceptionTicket::new(seq_no, reception_type));
        }

        Ok(())
    }

    pub(crate) async fn stop(self) -> YdbResult<()> {
        let mut state = self.state.lock().await;
        match &state.status {
            ReconnectorStatus::Created => Ok(()),
            ReconnectorStatus::Working => match state.loop_handle.take() {
                Some(loop_handle) => {
                    state.status = ReconnectorStatus::Stopped;
                    loop_handle.await.map_err(|err| {
                        YdbError::custom(format!(
                            "stop: error while waiting for reconnection_loop to finish: {err}"
                        ))
                    })
                }
                None => Ok(()),
            },
            ReconnectorStatus::Stopped => Ok(()),
        }
    }
}

struct ReconnectionHelper {
    writer_state: Arc<TokioMutex<WriterState>>,
    writer_options: TopicWriterOptions,
    connection_manager: GrpcConnectionManager,
    retrier: Arc<dyn Retry>,
    cancellation_token: CancellationToken,
    producer_id: String,
}

enum WaitBeforeReconnectResult {
    Ok,
    Cancelled,
}

impl ReconnectionHelper {
    async fn recreate_stream_writer(
        &self,
        message_queue: MessageQueue,
        error_sender: oneshot::Sender<YdbError>,
    ) -> YdbResult<StreamWriter> {
        message_queue.reset_progress().await;

        StreamWriter::new(
            self.writer_options.clone(),
            self.producer_id.clone(),
            message_queue,
            self.connection_manager.clone(),
            self.writer_state.clone(),
            error_sender,
        )
        .await
    }

    fn is_retry_allowed(err: &YdbError) -> bool {
        match err.need_retry() {
            NeedRetry::True => true,
            // IdempotentOnly errors are retriable because the
            // 'Write to Topic With seq_no deduplication' operation is idempotent.
            NeedRetry::IdempotentOnly => true,
            NeedRetry::False => false,
        }
    }

    // Decides whether reconnect is allowed and returns a wait timeout if it is.
    fn get_timeout_before_reconnect(
        &self,
        attempt: usize,
        time_from_start: Duration,
    ) -> Option<Duration> {
        let decision = self.retrier.wait_duration(RetryParams {
            attempt,
            time_from_start,
        });

        decision.allow_retry.then_some(decision.wait_timeout)
    }

    async fn wait_before_reconnect(&self, wait_timeout: Duration) -> WaitBeforeReconnectResult {
        match timeout(wait_timeout, self.cancellation_token.cancelled()).await {
            Ok(_) => WaitBeforeReconnectResult::Cancelled,
            Err(_) => WaitBeforeReconnectResult::Ok,
        }
    }

    async fn set_writer_status(&self, new_status: TopicWriterStatus) {
        let mut writer_state = self.writer_state.lock().await;
        writer_state.status = new_status;
    }
}

struct ReconnectionLoop {
    helper: ReconnectionHelper,
    message_queue: MessageQueue,

    reconnect_start_time: Instant,
    attempt: usize,
    init_tx: Option<oneshot::Sender<YdbResult<()>>>,
    stream_writer: Option<StreamWriter>,
}

enum ReconnectionLoopStatus {
    HandleError(YdbError),
    RecreateStreamWriter,
    WaitForErrorOrCancellation(oneshot::Receiver<YdbError>),
    Exit(Option<YdbError>),
}

impl ReconnectionLoop {
    fn new(
        helper: ReconnectionHelper,
        message_queue: MessageQueue,
        init_tx: oneshot::Sender<YdbResult<()>>,
    ) -> Self {
        Self {
            helper,
            message_queue,
            reconnect_start_time: Instant::now(),
            attempt: 0,
            init_tx: Some(init_tx),
            stream_writer: None,
        }
    }

    async fn run(&mut self, fatal_error_tx: oneshot::Sender<YdbError>) {
        let mut status = ReconnectionLoopStatus::RecreateStreamWriter;

        let final_result = loop {
            status = match status {
                ReconnectionLoopStatus::HandleError(err) => self.handle_error(err).await,
                ReconnectionLoopStatus::RecreateStreamWriter => self.recreate_stream_writer().await,
                ReconnectionLoopStatus::WaitForErrorOrCancellation(error_receiver) => {
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
            if let Err(err) = fatal_error_tx.send(final_error.clone()) {
                error!("can't send fatal error to TopicWriter: channel is closed: {err}");
            }

            self.helper
                .set_writer_status(TopicWriterStatus::FinishedWithError(final_error))
                .await;
        }
    }

    async fn handle_error(&mut self, err: YdbError) -> ReconnectionLoopStatus {
        if !ReconnectionHelper::is_retry_allowed(&err) {
            trace!("Reconnect is not allowed for error: {err}");
            return ReconnectionLoopStatus::Exit(Some(err));
        }

        trace!("Error, trying to reconnect: {err}");

        let Some(wait_timeout) = self
            .helper
            .get_timeout_before_reconnect(self.attempt, self.reconnect_start_time.elapsed())
        else {
            return ReconnectionLoopStatus::Exit(Some(YdbError::custom(format!(
                "Reconnect is not allowed after {} attempts for error: {err}",
                self.attempt,
            ))));
        };

        self.helper
            .set_writer_status(TopicWriterStatus::Reconnecting)
            .await;

        match self.helper.wait_before_reconnect(wait_timeout).await {
            WaitBeforeReconnectResult::Ok => ReconnectionLoopStatus::RecreateStreamWriter,
            WaitBeforeReconnectResult::Cancelled => ReconnectionLoopStatus::Exit(None),
        }
    }

    async fn recreate_stream_writer(&mut self) -> ReconnectionLoopStatus {
        let (error_sender, error_receiver) = oneshot::channel();
        match self
            .helper
            .recreate_stream_writer(self.message_queue.clone(), error_sender)
            .await
        {
            Ok(sw) => {
                self.stream_writer = Some(sw);
                self.attempt = 0;
                self.helper
                    .set_writer_status(TopicWriterStatus::Working)
                    .await;

                if let Some(tx) = self.init_tx.take() {
                    let _ = tx.send(Ok(()));
                };

                ReconnectionLoopStatus::WaitForErrorOrCancellation(error_receiver)
            }
            Err(err) => {
                trace!("Error creating stream writer: {err}");
                self.attempt += 1;

                if let Some(tx) = self.init_tx.take() {
                    let _ = tx.send(Err(err.clone()));
                };

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
                    self.reconnect_start_time = Instant::now();
                    ReconnectionLoopStatus::HandleError(err)
                },
                Err(chan_err) => ReconnectionLoopStatus::Exit(Some(YdbError::custom(format!("Channel error: {chan_err}"))))
            },
        }
    }
}
