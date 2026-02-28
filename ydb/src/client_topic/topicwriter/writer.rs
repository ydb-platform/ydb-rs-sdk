use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::Mutex as TokioMutex;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::log::trace;

use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

use crate::client_topic::topicwriter::connection::ConnectionInfo;
use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::message_write_status::MessageWriteStatus;
use crate::client_topic::topicwriter::stream_writer::{StreamWriter, StreamWriterParams};
use crate::client_topic::topicwriter::writer_options::TopicWriterOptions;
use crate::client_topic::topicwriter::writer_reception_queue::{
    TopicWriterReceptionQueue, TopicWriterReceptionTicket, TopicWriterReceptionType,
};
use crate::errors::NeedRetry;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_topic_service::common::codecs::RawSupportedCodecs;
use crate::retry::{Retry, RetryParams, TimeoutRetrier};
use crate::{YdbError, YdbResult};

pub(crate) enum TopicWriterState {
    Working,
    FinishedWithError(YdbError),
}

/// TopicWriter is currently in development.
/// It is mostly usable, but has some unimplemented features.
#[allow(dead_code)]
pub struct TopicWriter {
    pub(crate) path: String,
    pub(crate) producer_id: Option<String>,
    pub(crate) write_request_messages_chunk_size: usize,
    pub(crate) write_request_send_messages_period: Duration,

    pub(crate) auto_set_seq_no: bool,
    pub(crate) connection_info: Arc<TokioMutex<ConnectionInfo>>,

    flush_timeout: Duration,

    writer_message_sender: Arc<TokioMutex<mpsc::Sender<TopicWriterMessage>>>,

    cancellation_token: CancellationToken,
    writer_state: Arc<Mutex<TopicWriterState>>,

    confirmation_reception_queue: Arc<Mutex<TopicWriterReceptionQueue>>,

    reconnection_loop: JoinHandle<()>,
}

#[allow(dead_code)]
pub struct AckFuture {
    receiver: oneshot::Receiver<MessageWriteStatus>,
}

impl Future for AckFuture {
    type Output = YdbResult<MessageWriteStatus>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(_cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(Ok(result)),
            Poll::Ready(Err(_)) => Poll::Ready(Err(YdbError::custom("message writer was closed"))),
            Poll::Pending => Poll::Pending,
        }
    }
}

struct ReconnectionLoopParams {
    writer_options: TopicWriterOptions,
    producer_id: String,
    connection_manager: GrpcConnectionManager,
    writer_state: Arc<Mutex<TopicWriterState>>,
    cancellation_token: CancellationToken,
    writer_message_sender: Arc<TokioMutex<mpsc::Sender<TopicWriterMessage>>>,
    confirmation_reception_queue: Arc<Mutex<TopicWriterReceptionQueue>>,
    connection_info: Arc<TokioMutex<ConnectionInfo>>,
    initial_messages_receiver: mpsc::Receiver<TopicWriterMessage>,
    retrier: Arc<dyn Retry>,
}

impl TopicWriter {
    pub(crate) async fn new(
        writer_options: TopicWriterOptions,
        connection_manager: GrpcConnectionManager,
    ) -> YdbResult<Self> {
        let producer_id = writer_options
            .producer_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let cancellation_token = CancellationToken::new();

        let writer_state = Arc::new(Mutex::new(TopicWriterState::Working));

        let (initial_messages_sender, initial_messages_receiver) = mpsc::channel(32_usize);
        let writer_message_sender = Arc::new(TokioMutex::new(initial_messages_sender));

        let confirmation_reception_queue = Arc::new(Mutex::new(TopicWriterReceptionQueue::new()));
        let connection_info = Arc::new(TokioMutex::new(ConnectionInfo {
            partition_id: 0,
            session_id: String::new(),
            last_seq_no_assigned: 0,
            codecs_from_server: RawSupportedCodecs::default(),
        }));
        let retrier = writer_options.retrier.clone().unwrap_or_else(|| {
            Arc::new(TimeoutRetrier {
                timeout: Duration::from_secs(30),
            })
        });

        let reconnection_loop = TopicWriter::spawn_reconnection_loop(ReconnectionLoopParams {
            writer_options: writer_options.clone(),
            producer_id: producer_id.clone(),
            connection_manager,
            writer_state: writer_state.clone(),
            cancellation_token: cancellation_token.clone(),
            writer_message_sender: writer_message_sender.clone(),
            confirmation_reception_queue: confirmation_reception_queue.clone(),
            connection_info: connection_info.clone(),
            initial_messages_receiver,
            retrier,
        })
        .await?;

        Ok(Self {
            path: writer_options.topic_path.clone(),
            producer_id: Some(producer_id),
            write_request_messages_chunk_size: writer_options.write_request_messages_chunk_size,
            write_request_send_messages_period: writer_options.write_request_send_messages_period,
            auto_set_seq_no: writer_options.auto_seq_no,
            connection_info,
            flush_timeout: writer_options.flush_timeout,
            writer_message_sender,
            cancellation_token,
            writer_state,
            confirmation_reception_queue,
            reconnection_loop,
        })
    }

    async fn spawn_reconnection_loop(params: ReconnectionLoopParams) -> YdbResult<JoinHandle<()>> {
        let (connection_info_filled_tx, connection_info_filled_rx) =
            oneshot::channel::<YdbResult<()>>();
        let reconnection_loop = tokio::spawn(async move {
            let mut messages_receiver = params.initial_messages_receiver;
            let mut connection_info_filled_tx = Some(connection_info_filled_tx);
            // TODO: buffer might grow quite big if reconnection keeps failing.
            let messages = Arc::new(TokioMutex::new(Vec::<MessageData>::new()));
            let retrier = params.retrier.clone();

            let mut attempt = 0;

            loop {
                let (error_sender, error_receiver) = oneshot::channel();

                let stream_writer = match StreamWriter::new(
                    StreamWriterParams {
                        writer_options: params.writer_options.clone(),
                        producer_id: params.producer_id.clone(),
                        messages: messages.clone(),
                    },
                    params.connection_manager.clone(),
                    params.connection_info.clone(),
                    params.confirmation_reception_queue.clone(),
                    messages_receiver,
                    error_sender,
                )
                .await
                {
                    Ok(stream_writer) => stream_writer,
                    Err(err) => {
                        trace!("Error creating stream writer: {}", err);
                        attempt += 1;
                        if let Some(wait_timeout) =
                            TopicWriter::get_timeout_before_reconnect(&retrier, &err, attempt)
                        {
                            TopicWriter::wait_before_reconnect(wait_timeout, &err).await;
                            messages_receiver = TopicWriter::recreate_message_channel(
                                &params.writer_message_sender,
                            )
                            .await;
                            continue;
                        }

                        if let Some(tx) = connection_info_filled_tx.take() {
                            let _ = tx.send(Err(err.clone()));
                        }
                        let mut writer_state = params.writer_state.lock().unwrap();
                        *writer_state = TopicWriterState::FinishedWithError(err);
                        break;
                    }
                };

                {
                    let mut writer_state = params.writer_state.lock().unwrap();
                    *writer_state = TopicWriterState::Working;
                }

                if let Some(tx) = connection_info_filled_tx.take() {
                    let _ = tx.send(Ok(()));
                };

                tokio::select! {
                    _ = params.cancellation_token.cancelled() => {
                        let _ = stream_writer.stop().await;
                        break;
                    }
                    err = error_receiver => {
                        let err = match err {
                            Ok(err) => err,
                            Err(chan_err) => {
                                // TODO: ???
                                trace!("Channel error: {}", chan_err);
                                let _ = stream_writer.stop().await;  // TODO: handle error
                                let mut writer_state = params.writer_state.lock().unwrap();
                                *writer_state = TopicWriterState::FinishedWithError(
                                    YdbError::custom(format!("stream writer channel closed: {chan_err}")),
                                );
                                break;
                            }
                        };

                        attempt += 1;
                        if let Some(wait_timeout) = TopicWriter::get_timeout_before_reconnect(
                            &retrier,
                            &err,
                            attempt,
                        ) {
                            TopicWriter::wait_before_reconnect(wait_timeout, &err).await;
                        } else {
                            trace!("Unknown error: {}", err);
                            let _ = stream_writer.stop().await;  // TODO: handle error
                            let mut writer_state = params.writer_state.lock().unwrap();
                            *writer_state = TopicWriterState::FinishedWithError(err);
                            break;
                        };
                    }
                }

                messages_receiver =
                    TopicWriter::recreate_message_channel(&params.writer_message_sender).await;
            }
        });

        match connection_info_filled_rx.await {
            Ok(Ok(())) => Ok(reconnection_loop),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(YdbError::custom("connection info filled channel closed")),
        }
    }

    async fn recreate_message_channel(
        writer_message_sender: &Arc<TokioMutex<mpsc::Sender<TopicWriterMessage>>>,
    ) -> mpsc::Receiver<TopicWriterMessage> {
        let (new_messages_sender, new_messages_receiver) = mpsc::channel(32_usize);
        {
            let mut sender_guard = writer_message_sender.lock().await;
            *sender_guard = new_messages_sender;
        }
        new_messages_receiver
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
        retrier: &Arc<dyn Retry>,
        err: &YdbError,
        attempt: usize,
    ) -> Option<Duration> {
        if !TopicWriter::is_retry_allowed(err) {
            return None;
        }

        let decision = retrier.wait_duration(RetryParams {
            attempt,
            time_from_start: Duration::from_secs(0), // TODO: from start of what?
        });

        if !decision.allow_retry {
            return None;
        }

        Some(decision.wait_timeout)
    }

    async fn wait_before_reconnect(wait_timeout: Duration, err: &YdbError) {
        trace!("Error, trying to reconnect: {}", err);
        sleep(wait_timeout).await;
    }

    pub async fn write(&mut self, message: TopicWriterMessage) -> YdbResult<()> {
        self.write_message(message, None).await?;
        Ok(())
    }

    pub async fn write_with_ack(
        &mut self,
        message: TopicWriterMessage,
    ) -> YdbResult<MessageWriteStatus> {
        let (tx, rx) = oneshot::channel();

        self.write_message(message, Some(tx)).await?;
        Ok(rx.await?)
    }

    pub async fn write_with_ack_future(
        &mut self,
        _message: TopicWriterMessage,
    ) -> YdbResult<AckFuture> {
        let (tx, rx) = oneshot::channel();

        self.write_message(_message, Some(tx)).await?;
        Ok(AckFuture { receiver: rx })
    }

    async fn write_message(
        &mut self,
        mut message: TopicWriterMessage,
        wait_ack: Option<oneshot::Sender<MessageWriteStatus>>,
    ) -> YdbResult<()> {
        self.check_working().await?;

        let message_seqno = {
            let mut connection_info = self.connection_info.lock().await;
            if self.auto_set_seq_no {
                if message.seq_no.is_some() {
                    return Err(YdbError::custom(
                        "force set message seqno possible only if auto_set_seq_no disabled",
                    ));
                }
                message.seq_no = Some(connection_info.last_seq_no_assigned + 1);
            };

            if let Some(mess_seqno) = message.seq_no {
                connection_info.last_seq_no_assigned = mess_seqno;
                mess_seqno
            } else {
                return Err(YdbError::custom("need to set message seq_no"));
            }
        };

        let sender = { self.writer_message_sender.lock().await.clone() };
        sender
            .send(message)
            .await
            .map_err(|err| YdbError::custom(format!("can't send the message to channel: {err}")))?;

        let reception_type = wait_ack.map_or(
            TopicWriterReceptionType::NoConfirmationExpected,
            TopicWriterReceptionType::AwaitingConfirmation,
        );

        {
            // brackets are needed for mutex to be released as soon as possible - before await
            let mut reception_queue = self.confirmation_reception_queue.lock().unwrap();
            reception_queue.add_ticket(TopicWriterReceptionTicket::new(
                message_seqno,
                reception_type,
            ));
        }

        Ok(())
    }

    pub async fn flush(&self) -> YdbResult<()> {
        self.check_working().await?;

        let flush_op_completed = {
            let mut reception_queue = self.confirmation_reception_queue.lock().unwrap();
            reception_queue.init_flush_op()?
        };

        Ok(flush_op_completed.await?)
    }

    async fn check_working(&self) -> YdbResult<()> {
        let state = self.writer_state.lock().unwrap();
        match state.deref() {
            TopicWriterState::Working => Ok(()),
            TopicWriterState::FinishedWithError(err) => Err(err.clone()),
        }
    }

    pub async fn stop(self) -> YdbResult<()> {
        trace!("Stopping...");

        let flush_result = match timeout(self.flush_timeout, self.flush()).await {
            Ok(result) => result,
            Err(_) => Err(YdbError::custom(
                "stop: flush() timed out while stopping topic writer",
            )),
        };

        self.cancellation_token.cancel();
        let loop_result = self.reconnection_loop.await.map_err(|err| {
            YdbError::custom(format!(
                "stop: error while waiting for reconnection_loop to finish: {err}"
            ))
        });

        trace!("Reconnection loop stopped");

        // First clean up all resources, then return result.
        flush_result?;
        loop_result?;

        Ok(())
    }
}
