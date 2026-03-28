use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, UNIX_EPOCH};

use tokio::sync::oneshot;
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::log::trace;
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

use crate::client_topic::topicwriter::connection::ConnectionInfo;
use crate::client_topic::topicwriter::message_queue::MessageQueue;
use crate::client_topic::topicwriter::message_write_status::MessageWriteStatus;
use crate::client_topic::topicwriter::stream_writer::{StreamWriter, StreamWriterParams};
use crate::client_topic::topicwriter::writer::TopicWriterState;
use crate::client_topic::topicwriter::writer_reception_queue::{
    TopicWriterReceptionQueue, TopicWriterReceptionTicket, TopicWriterReceptionType,
};
use crate::errors::NeedRetry;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::retry::{Retry, RetryParams};
use crate::{TopicWriterMessage, TopicWriterOptions, YdbError, YdbResult};

pub(crate) struct ReconnectorParams {
    pub(crate) writer_options: TopicWriterOptions,
    pub(crate) producer_id: String,
    pub(crate) connection_manager: GrpcConnectionManager,
    pub(crate) writer_state: Arc<Mutex<TopicWriterState>>,
    pub(crate) cancellation_token: CancellationToken,
    pub(crate) confirmation_reception_queue: Arc<Mutex<TopicWriterReceptionQueue>>,
    pub(crate) message_queue: MessageQueue,
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
    confirmation_reception_queue: Arc<Mutex<TopicWriterReceptionQueue>>,
    pub(crate) message_queue: MessageQueue,
}

impl Reconnector {
    pub(crate) async fn new(params: ReconnectorParams) -> YdbResult<Self> {
        let r = Reconnector {
            state: Arc::new(TokioMutex::new(ReconnectorState::Created)),
            loop_handle: Arc::new(TokioMutex::new(None)),
            message_queue: params.message_queue.clone(),
            confirmation_reception_queue: params.confirmation_reception_queue.clone(),
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
            },
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

    async fn start_loop(
        helper: ReconnectorLoopHelper,
        message_queue: MessageQueue,
    ) -> YdbResult<JoinHandle<()>> {
        let (connection_info_filled_tx, connection_info_filled_rx) = oneshot::channel::<()>();

        let reconnection_loop = tokio::spawn(async move {
            let mut connection_info_filled_tx = Some(connection_info_filled_tx);
            let message_queue = message_queue;

            let mut reconnect_start_time = Instant::now();
            let mut attempt = 0;

            loop {
                let (error_sender, error_receiver) = oneshot::channel();

                message_queue.reset_progress().await;

                let stream_writer = match StreamWriter::new(
                    StreamWriterParams {
                        writer_options: helper.writer_options.clone(),
                        producer_id: helper.producer_id.clone(),
                        message_queue: message_queue.clone(),
                    },
                    helper.connection_manager.clone(),
                    helper.connection_info.clone(),
                    helper.confirmation_reception_queue.clone(),
                    error_sender,
                )
                .await
                {
                    Ok(stream_writer) => stream_writer,
                    Err(err) => {
                        trace!("Error creating stream writer: {}", err);
                        helper.set_writer_state(TopicWriterState::Reconnecting);
                        attempt += 1;
                        if let Some(wait_timeout) = helper.get_timeout_before_reconnect(
                            &err,
                            attempt,
                            reconnect_start_time.elapsed(),
                        ) {
                            ReconnectorLoopHelper::wait_before_reconnect(wait_timeout, &err).await;
                            continue;
                        }

                        helper.set_writer_state(TopicWriterState::FinishedWithError(err));
                        break;
                    }
                };

                helper.set_writer_state(TopicWriterState::Working);
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
                                helper.set_writer_state(TopicWriterState::Reconnecting);
                                break;
                            }
                        };

                        reconnect_start_time = Instant::now();
                        attempt = 0;
                        if let Some(wait_timeout) = helper.get_timeout_before_reconnect(
                            &err,
                            attempt,
                            reconnect_start_time.elapsed(),
                        ) {
                            ReconnectorLoopHelper::wait_before_reconnect(wait_timeout, &err).await;
                        } else {
                            trace!("Unknown error: {}", err);
                            let _ = stream_writer.stop().await;  // TODO: handle error
                            helper.set_writer_state(TopicWriterState::Reconnecting);
                            break;
                        };
                    }
                }
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

    pub(crate) async fn add_message_for_processing(
        &self,
        message: TopicWriterMessage,
        ack: Option<oneshot::Sender<MessageWriteStatus>>,
    ) -> YdbResult<()> {
        let data_size = message.data.len() as i64;

        let seq_no = message
            .seq_no
            .ok_or_else(|| YdbError::custom("empty message seq_no is provided"))?;

        let reception_type = ack.map_or(
            TopicWriterReceptionType::NoConfirmationExpected,
            TopicWriterReceptionType::AwaitingConfirmation,
        );

        self.message_queue
            .add_message(MessageData {
                seq_no,
                created_at: Some(ydb_grpc::google_proto_workaround::protobuf::Timestamp {
                    seconds: message.created_at.duration_since(UNIX_EPOCH)?.as_secs() as i64,
                    nanos: message.created_at.duration_since(UNIX_EPOCH)?.as_nanos() as i32,
                }),
                metadata_items: vec![],
                data: message.data,
                uncompressed_size: data_size,
                partitioning: None,
            })
            .await?;

        {
            let mut reception_queue = self.confirmation_reception_queue.lock().unwrap();
            reception_queue.add_ticket(TopicWriterReceptionTicket::new(seq_no, reception_type));
        }

        Ok(())
    }
}

struct ReconnectorLoopHelper {
    connection_manager: GrpcConnectionManager,
    connection_info: Arc<TokioMutex<ConnectionInfo>>,
    retrier: Arc<dyn Retry>,
    cancellation_token: CancellationToken,
    writer_state: Arc<Mutex<TopicWriterState>>,
    writer_options: TopicWriterOptions,
    producer_id: String,
    confirmation_reception_queue: Arc<Mutex<TopicWriterReceptionQueue>>,
}

impl ReconnectorLoopHelper {
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
