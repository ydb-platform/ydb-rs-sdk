use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{oneshot, watch, Mutex};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{error, trace};
use ydb_grpc::ydb_proto::topic::stream_write_message;

use crate::client_topic::compression::Executor;
use crate::client_topic::list_types::Codec;
use crate::client_topic::topicwriter::connection::ConnectionInfo;
use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::message_write_status::MessageWriteStatus;
use crate::client_topic::topicwriter::queue::Queue;
use crate::client_topic::topicwriter::stream_writer::StreamWriter;
use crate::client_topic::topicwriter::writer_options::TopicWriterOptions;
use crate::errors::NeedRetry;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::grpc_wrapper::raw_topic_service::client::RawTopicClient;
use crate::grpc_wrapper::raw_topic_service::stream_write::RawServerMessage;
use crate::retry::{Retry, RetryParams};
use crate::{YdbError, YdbResult};

pub(crate) struct ReconnectorParams {
    pub(crate) writer_options: TopicWriterOptions,
    pub(crate) producer_id: String,
    pub(crate) connection_manager: GrpcConnectionManager,
    pub(crate) cancellation_token: CancellationToken,
    pub(crate) retrier: Arc<dyn Retry>,
    pub(crate) fatal_error_tx: oneshot::Sender<YdbError>,
    pub(crate) flush_timeout: Duration,
    pub(crate) executor: Arc<dyn Executor>,
    pub(crate) supported_codecs: Vec<Codec>,
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
    connection_info: ConnectionInfo,
}

pub(crate) struct Reconnector {
    state: Arc<Mutex<ReconnectorState>>,
    cancellation_token: CancellationToken,
    reconnect_loop: JoinHandle<()>,
    queue: Queue,
    auto_seq_no: bool,
    flush_timeout: Duration,
    status_rx: watch::Receiver<ReconnectorStatus>,
}

impl Reconnector {
    pub(crate) async fn new(params: ReconnectorParams) -> YdbResult<Self> {
        let queue = Queue::new();
        let cancellation_token = params.cancellation_token;
        let auto_seq_no = params.writer_options.auto_seq_no;

        let (init_tx, init_rx) = oneshot::channel();
        let (status_tx, status_rx) = watch::channel(ReconnectorStatus::Working);

        let reconnect_loop = Reconnector::start_reconnection_loop(
            ReconnectionHelper {
                connection_manager: params.connection_manager,
                retrier: params.retrier,
                cancellation_token: cancellation_token.clone(),
                writer_options: params.writer_options,
                producer_id: params.producer_id,
                queue: queue.clone(),
                executor: params.executor,
                supported_codecs: params.supported_codecs,
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

        Ok(Reconnector {
            state: Arc::new(Mutex::new(ReconnectorState { connection_info })),
            cancellation_token: cancellation_token.clone(),
            reconnect_loop,
            queue,
            auto_seq_no,
            flush_timeout: params.flush_timeout,
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
        mut message: TopicWriterMessage,
        ack_sender: Option<oneshot::Sender<YdbResult<MessageWriteStatus>>>,
    ) -> YdbResult<()> {
        self.check_working()?;

        // Here we take a lock across the whole function.
        // Updating last_seq_no_assigned and putting a new message to the queue must be transactional.
        //
        // Example of a race condition that we prevent here (two threads):
        // 1. Thread1: last_seq_no_assigned = 1
        // 2. Thread2: last_seq_no_assigned = 2
        // 3. Thread2: add message<seq_no=2> to queue
        // 4. Thread1: add message<seq_no=1> to queue <--- ERROR: Message seq_no order is violated.
        let mut state_guard = self.state.lock().await;

        if self.auto_seq_no {
            if message.seq_no.is_some() {
                return Err(YdbError::custom(
                        "explicitly specifying message.seq_no is only allowed if auto_seq_no is disabled",
                    ));
            }
            let last_seq_no_assigned = state_guard.connection_info.last_seq_no_assigned;
            message.seq_no = Some(last_seq_no_assigned + 1);
        }

        let Some(message_seq_no) = message.seq_no else {
            return Err(YdbError::custom("empty message seq_no is provided"));
        };
        state_guard.connection_info.last_seq_no_assigned = message_seq_no;

        let message = message.try_into()?;
        self.queue.add_message(message, ack_sender).await
    }

    pub(crate) async fn flush(&self) -> YdbResult<()> {
        self.check_working()?;
        match timeout(self.flush_timeout, self.queue.flush()).await {
            Ok(result) => result,
            Err(_) => Err(YdbError::custom("flush: timed out")),
        }
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
    retrier: Arc<dyn Retry>,
    cancellation_token: CancellationToken,
    producer_id: String,
    executor: Arc<dyn Executor>,
    supported_codecs: Vec<Codec>,
}

enum WaitBeforeReconnectResult {
    Ok,
    Cancelled,
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

        Ok(RecreateStreamWriterResult {
            stream_writer: StreamWriter::new(
                self.writer_options.clone(),
                stream,
                self.queue.clone(),
                error_sender,
                self.supported_codecs.clone(),
                self.executor.clone(),
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
    init_tx: Option<oneshot::Sender<YdbResult<ConnectionInfo>>>,
    status_tx: watch::Sender<ReconnectorStatus>,
    reconnect_start_time: Instant,
    attempt: usize,
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
            reconnect_start_time: Instant::now(),
            attempt: 0,
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
            self.update_status(ReconnectorStatus::FinishedWithError(final_error.clone()));
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

        // Wait ending old stream writer before recreating
        if let Some(old) = self.stream_writer.take() {
            if let Err(err) = old.stop().await {
                return ReconnectionLoopStatus::HandleError(err);
            }
        }

        let (error_sender, error_receiver) = oneshot::channel();
        match self.helper.recreate_stream_writer(error_sender).await {
            Ok(swr) => {
                self.stream_writer = Some(swr.stream_writer);
                self.attempt = 0;

                if let Some(tx) = self.init_tx.take() {
                    let _ = tx.send(Ok(swr.connection_info));
                }

                ReconnectionLoopStatus::WaitForErrorOrCancellation(error_receiver)
            }
            Err(err) => {
                trace!("error creating stream writer: {err}");
                self.attempt += 1;

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
                Err(chan_err) => ReconnectionLoopStatus::Exit(Some(YdbError::custom(format!("error channel error: {chan_err}"))))
            },
        }
    }
}
