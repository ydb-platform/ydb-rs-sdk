use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Mutex as TokioMutex;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::log::trace;

use crate::client_topic::topicwriter::connection::ConnectionInfo;
use crate::client_topic::topicwriter::message::TopicWriterMessageWithAck;
use crate::client_topic::topicwriter::message_queue::MessageQueue;
use crate::client_topic::topicwriter::stream_writer::{StreamWriter, StreamWriterParams};
use crate::client_topic::topicwriter::writer::TopicWriterState;
use crate::client_topic::topicwriter::writer_reception_queue::TopicWriterReceptionQueue;
use crate::errors::NeedRetry;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::retry::{Retry, RetryParams};
use crate::{TopicWriterOptions, YdbError, YdbResult};

pub(crate) struct ReconnectorParams {
    pub(crate) writer_options: TopicWriterOptions,
    pub(crate) producer_id: String,
    pub(crate) connection_manager: GrpcConnectionManager,
    pub(crate) writer_state: Arc<Mutex<TopicWriterState>>,
    pub(crate) cancellation_token: CancellationToken,
    pub(crate) confirmation_reception_queue: Arc<Mutex<TopicWriterReceptionQueue>>,
    pub(crate) message_queue: Arc<Mutex<MessageQueue>>,
    pub(crate) connection_info: Arc<TokioMutex<ConnectionInfo>>,
    pub(crate) retrier: Arc<dyn Retry>,
}

enum ReconnectorState {
    Created,
    Working,
    Stopped,
}

pub(crate) struct Reconnector {
    state: Arc<TokioMutex<ReconnectorState>>,
    loop_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    messages_sender: Arc<TokioMutex<mpsc::Sender<TopicWriterMessageWithAck>>>,
}

impl Reconnector {
    pub(crate) async fn new(params: ReconnectorParams) -> YdbResult<Self> {
        let (initial_messages_sender, initial_messages_receiver) = mpsc::channel(32_usize);

        let messages_sender = Arc::new(TokioMutex::new(initial_messages_sender));
        let r = Reconnector {
            state: Arc::new(TokioMutex::new(ReconnectorState::Created)),
            loop_handle: Arc::new(TokioMutex::new(None)),
            messages_sender: messages_sender.clone(),
        };

        let loop_join_handle = match Reconnector::start_loop(
            ReconnectorLoopHelper {
                connection_manager: params.connection_manager,
                connection_info: params.connection_info,
                retrier: params.retrier,
                cancellation_token: params.cancellation_token,
                writer_state: params.writer_state,
                writer_options: params.writer_options,
                producer_id: params.producer_id,
                confirmation_reception_queue: params.confirmation_reception_queue,
                writer_message_sender: messages_sender.clone(),
            },
            initial_messages_receiver,
            params.message_queue,
        )
        .await
        {
            Ok(handle) => handle,
            Err(err) => return Err(err),
        };
        {
            let mut loop_handle_guard = r.loop_handle.lock().await;
            *loop_handle_guard = Some(loop_join_handle);
        }
        {
            let mut state_guard = r.state.lock().await;
            *state_guard = ReconnectorState::Working;
        }

        Ok(r)
    }

    pub(crate) fn get_writer_message_sender(
        &self,
    ) -> Arc<TokioMutex<mpsc::Sender<TopicWriterMessageWithAck>>> {
        self.messages_sender.clone()
    }

    async fn start_loop(
        helper: ReconnectorLoopHelper,
        initial_messages_receiver: mpsc::Receiver<TopicWriterMessageWithAck>,
        message_queue: Arc<Mutex<MessageQueue>>,
    ) -> YdbResult<JoinHandle<()>> {
        let (connection_info_filled_tx, connection_info_filled_rx) = oneshot::channel::<()>();

        let reconnection_loop = tokio::spawn(async move {
            let mut connection_info_filled_tx = Some(connection_info_filled_tx);
            let mut messages_receiver = initial_messages_receiver;
            let message_queue = message_queue;

            let mut reconnect_start_time = Instant::now();
            let mut attempt = 0;

            loop {
                let (error_sender, error_receiver) = oneshot::channel();

                {
                    let mut message_queue_guard = message_queue.lock().unwrap();
                    message_queue_guard.reset_progress();
                }

                let stream_writer = match StreamWriter::new(
                    StreamWriterParams {
                        writer_options: helper.writer_options.clone(),
                        producer_id: helper.producer_id.clone(),
                        message_queue: message_queue.clone(),
                    },
                    helper.connection_manager.clone(),
                    helper.connection_info.clone(),
                    helper.confirmation_reception_queue.clone(),
                    messages_receiver,
                    error_sender,
                )
                .await
                {
                    Ok(stream_writer) => stream_writer,
                    Err(err) => {
                        trace!("Error creating stream writer: {}", err);
                        attempt += 1;
                        if let Some(wait_timeout) = helper.get_timeout_before_reconnect(
                            &err,
                            attempt,
                            reconnect_start_time.elapsed(),
                        ) {
                            ReconnectorLoopHelper::wait_before_reconnect(wait_timeout, &err).await;
                            messages_receiver = helper.recreate_message_channel().await;
                            continue;
                        }

                        helper.set_writer_state(TopicWriterState::FinishedWithError(err));
                        break;
                    }
                };

                helper.set_writer_state(TopicWriterState::Working);
                attempt = 0;
                if let Some(tx) = connection_info_filled_tx.take() {
                    let _ = tx.send(());
                };

                tokio::select! {
                    _ = helper.cancellation_token.cancelled() => {
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
                                helper.set_writer_state(TopicWriterState::FinishedWithError(
                                    YdbError::custom(format!("stream writer channel closed: {chan_err}")),
                                ));
                                break;
                            }
                        };

                        attempt += 1;
                        reconnect_start_time = Instant::now();
                        if let Some(wait_timeout) = helper.get_timeout_before_reconnect(
                            &err,
                            attempt,
                            reconnect_start_time.elapsed(),
                        ) {
                            ReconnectorLoopHelper::wait_before_reconnect(wait_timeout, &err).await;
                        } else {
                            trace!("Unknown error: {}", err);
                            let _ = stream_writer.stop().await;  // TODO: handle error
                            helper.set_writer_state(TopicWriterState::FinishedWithError(err));
                            break;
                        };
                    }
                }

                messages_receiver = helper.recreate_message_channel().await;
            }
        });

        match connection_info_filled_rx.await {
            Ok(_) => Ok(reconnection_loop),
            Err(err) => Err(YdbError::custom(format!(
                "connection_info_filled channel closed: {err}"
            ))),
        }
    }

    pub(crate) async fn stop(self) -> YdbResult<()> {
        let mut state_guard = self.state.lock().await;
        match *state_guard {
            // TODO: wait?
            ReconnectorState::Created => Ok(()),
            ReconnectorState::Working => {
                // TODO: handle error
                let mut handle_guard = self.loop_handle.lock().await;

                match handle_guard.take() {
                    Some(handle) => {
                        *state_guard = ReconnectorState::Stopped;
                        handle.await.map_err(|err| {
                            YdbError::custom(format!(
                                "stop: error while waiting for reconnection_loop to finish: {err}"
                            ))
                        })
                    }
                    None => Ok(()),
                }
            }
            ReconnectorState::Stopped => Ok(()),
        }
    }
}

struct ReconnectorLoopHelper {
    connection_manager: GrpcConnectionManager,
    connection_info: Arc<TokioMutex<ConnectionInfo>>,
    retrier: Arc<dyn Retry>,
    cancellation_token: CancellationToken,
    writer_message_sender: Arc<TokioMutex<mpsc::Sender<TopicWriterMessageWithAck>>>,
    writer_state: Arc<Mutex<TopicWriterState>>,
    writer_options: TopicWriterOptions,
    producer_id: String,
    confirmation_reception_queue: Arc<Mutex<TopicWriterReceptionQueue>>,
}

impl ReconnectorLoopHelper {
    // TODO: don't lose data when recreating the channel!!!
    async fn recreate_message_channel(&self) -> mpsc::Receiver<TopicWriterMessageWithAck> {
        let (new_messages_sender, new_messages_receiver) = mpsc::channel(32_usize);
        {
            let mut sender_guard = self.writer_message_sender.lock().await;
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
        &self,
        err: &YdbError,
        attempt: usize,
        time_from_start: Duration,
    ) -> Option<Duration> {
        if !ReconnectorLoopHelper::is_retry_allowed(err) {
            return None;
        }

        let decision = self.retrier.wait_duration(RetryParams {
            attempt,
            time_from_start,
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

    fn set_writer_state(&self, new_state: TopicWriterState) {
        let mut writer_state = self.writer_state.lock().unwrap();
        *writer_state = new_state;
    }
}
