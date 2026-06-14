use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::log::{error, trace};
use ydb_grpc::ydb_proto::topic::stream_write_message;

use crate::client_topic::topicwriter::connection::ConnectionInfo;
use crate::client_topic::topicwriter::message_write_status::MessageWriteStatus;
use crate::client_topic::topicwriter::queue::Queue;
use crate::client_topic::topicwriter::stream_writer::StreamWriter;
use crate::errors::NeedRetry;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::grpc_wrapper::raw_topic_service::client::RawTopicClient;
use crate::grpc_wrapper::raw_topic_service::stream_write::init::RawInitResponse;
use crate::grpc_wrapper::raw_topic_service::stream_write::RawServerMessage;
use crate::retry::{Retry, RetryParams};
use crate::{TopicWriterMessage, TopicWriterOptions, YdbError, YdbResult};

pub(crate) struct ReconnectorParams {
    pub(crate) writer_options: TopicWriterOptions,
    pub(crate) producer_id: String,
    pub(crate) connection_manager: GrpcConnectionManager,
    pub(crate) cancellation_token: CancellationToken,
    pub(crate) retrier: Arc<dyn Retry>,
    pub(crate) init_tx: oneshot::Sender<YdbResult<()>>,
    pub(crate) fatal_error_tx: oneshot::Sender<YdbError>,
    pub(crate) flush_timeout: Duration,
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

struct ReconnectorState {
    status: ReconnectorStatus,
    loop_handle: Option<JoinHandle<()>>,
    connection_info: ConnectionInfo,
    queue: Queue,
}

pub(crate) struct Reconnector {
    state: Arc<TokioMutex<ReconnectorState>>,
    cancellation_token: CancellationToken,
    auto_set_seq_no: bool,
    flush_timeout: Duration,
}

impl Reconnector {
    pub(crate) async fn new(params: ReconnectorParams) -> Self {
        let queue = Queue::new();
        let state = Arc::new(TokioMutex::new(ReconnectorState {
            status: ReconnectorStatus::Working,
            loop_handle: None,
            connection_info: ConnectionInfo::default(),
            queue,
        }));
        let cancellation_token = params.cancellation_token;

        let r = Reconnector {
            state: state.clone(),
            cancellation_token: cancellation_token.clone(),
            auto_set_seq_no: params.writer_options.auto_seq_no,
            flush_timeout: params.flush_timeout,
        };

        let loop_join_handle = Reconnector::start_reconnection_loop(
            ReconnectionHelper {
                state: r.state.clone(),
                connection_manager: params.connection_manager,
                retrier: params.retrier,
                cancellation_token,
                writer_options: params.writer_options,
                producer_id: params.producer_id,
            },
            params.fatal_error_tx,
            params.init_tx,
        )
        .await;

        {
            let mut state = r.state.lock().await;
            state.loop_handle = Some(loop_join_handle);
        }

        r
    }

    async fn start_reconnection_loop(
        helper: ReconnectionHelper,
        fatal_error_tx: oneshot::Sender<YdbError>,
        init_tx: oneshot::Sender<YdbResult<()>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            ReconnectionLoop::new(helper, init_tx)
                .run(fatal_error_tx)
                .await
        })
    }

    pub(crate) async fn add_message_for_processing(
        &self,
        mut message: TopicWriterMessage,
        wait_ack: Option<oneshot::Sender<YdbResult<MessageWriteStatus>>>,
    ) -> YdbResult<()> {
        let mut state = self.state.lock().await;
        state.status.check_working()?;

        if self.auto_set_seq_no {
            if message.seq_no.is_some() {
                return Err(YdbError::custom(
                        "explicitly specifying message.seq_no is only allowed if auto_set_seq_no is disabled",
                    ));
            }
            let Some(last_seq_no_assigned) = state.connection_info.last_seq_no_assigned else {
                return Err(YdbError::InternalError(
                    "internal last_seq_no_assigned is unexpectedly not set".into(),
                ));
            };
            message.seq_no = Some(last_seq_no_assigned + 1);
        };

        let Some(message_seq_no) = message.seq_no else {
            return Err(YdbError::custom("empty message seq_no is provided"));
        };
        state.connection_info.last_seq_no_assigned = Some(message_seq_no);

        let message = message.try_into()?;
        state.queue.add_message(message, wait_ack).await
    }

    pub(crate) async fn flush(&self) -> YdbResult<()> {
        let queue = {
            let state = self.state.lock().await;
            state.status.check_working()?;
            state.queue.clone()
        };
        queue.flush().await
    }

    pub(crate) async fn stop(&self) -> YdbResult<()> {
        let queue = {
            let state = self.state.lock().await;
            state.queue.clone()
        };
        queue.close_for_new_messages().await;
        let flush_result = match timeout(self.flush_timeout, queue.flush()).await {
            Ok(result) => result,
            Err(_) => Err(YdbError::custom(
                "stop: flush() timed out while stopping topic writer",
            )),
        };

        self.cancellation_token.cancel();

        let reconnector_result = self.stop_inner().await;

        flush_result?;
        reconnector_result?;

        Ok(())
    }

    async fn stop_inner(&self) -> YdbResult<()> {
        let loop_handle = {
            let mut state = self.state.lock().await;
            match &state.status {
                ReconnectorStatus::Working => {
                    state.status = ReconnectorStatus::Stopped;
                    state.loop_handle.take()
                }
                ReconnectorStatus::FinishedWithError(err) => return Err(err.clone()),
                ReconnectorStatus::Stopped => return Ok(()),
            }
        };

        if let Some(loop_handle) = loop_handle {
            loop_handle.await.map_err(|err| {
                YdbError::custom(format!(
                    "stop: error while waiting for reconnection_loop to finish: {err}"
                ))
            })?;
        }

        Ok(())
    }
}

struct ReconnectionHelper {
    state: Arc<TokioMutex<ReconnectorState>>,
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
        error_sender: oneshot::Sender<YdbError>,
    ) -> YdbResult<StreamWriter> {
        let queue = {
            let state = self.state.lock().await;
            state.queue.clone()
        };

        queue.reset_progress().await;

        let stream = self.connect().await?;

        Ok(StreamWriter::new(self.writer_options.clone(), stream, queue, error_sender).await)
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

        let mut stream = topic_service
            .stream_write(init_request_body.clone())
            .await?;
        let init_response = RawInitResponse::try_from(stream.receive::<RawServerMessage>().await?)?;
        {
            let mut state = self.state.lock().await;
            state
                .connection_info
                .update_from_init_response(init_response);
        }

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
}

struct ReconnectionLoop {
    helper: ReconnectionHelper,

    reconnect_start_time: Instant,
    attempt: usize,
    init_tx: Option<oneshot::Sender<YdbResult<()>>>,
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
    fn new(helper: ReconnectionHelper, init_tx: oneshot::Sender<YdbResult<()>>) -> Self {
        Self {
            helper,
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
            {
                let mut state = self.helper.state.lock().await;
                state.status = ReconnectorStatus::FinishedWithError(final_error.clone());
                state
                    .queue
                    .notify_reception_tickets(final_error.clone())
                    .await;
            }

            if let Err(err) = fatal_error_tx.send(final_error) {
                error!("can't send fatal error to TopicWriter: channel is closed: {err}");
            }
        }
    }

    async fn handle_error(&mut self, err: YdbError) -> ReconnectionLoopStatus {
        if !ReconnectionHelper::is_retry_allowed(&err) {
            trace!("reconnect is not allowed for error: {err}");
            return ReconnectionLoopStatus::Exit(Some(err));
        }

        trace!("error, trying to reconnect: {err}");

        let Some(wait_timeout) = self
            .helper
            .get_timeout_before_reconnect(self.attempt, self.reconnect_start_time.elapsed())
        else {
            return ReconnectionLoopStatus::Exit(Some(YdbError::custom(format!(
                "reconnect is not allowed after {} attempts for error: {err}",
                self.attempt,
            ))));
        };

        match self.helper.wait_before_reconnect(wait_timeout).await {
            WaitBeforeReconnectResult::Ok => ReconnectionLoopStatus::RecreateStreamWriter,
            WaitBeforeReconnectResult::Cancelled => ReconnectionLoopStatus::Exit(None),
        }
    }

    async fn recreate_stream_writer(&mut self) -> ReconnectionLoopStatus {
        if self.helper.cancellation_token.is_cancelled() {
            return ReconnectionLoopStatus::Exit(None);
        }

        let (error_sender, error_receiver) = oneshot::channel();
        match self.helper.recreate_stream_writer(error_sender).await {
            Ok(sw) => {
                self.stream_writer = Some(sw);
                self.attempt = 0;

                if let Some(tx) = self.init_tx.take() {
                    let _ = tx.send(Ok(()));
                };

                ReconnectionLoopStatus::WaitForErrorOrCancellation(error_receiver)
            }
            Err(err) => {
                trace!("error creating stream writer: {err}");
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
                Err(chan_err) => ReconnectionLoopStatus::Exit(Some(YdbError::custom(format!("channel error: {chan_err}"))))
            },
        }
    }
}
