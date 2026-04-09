use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use tokio::sync::oneshot;
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::log::{error, trace};
use ydb_grpc::ydb_proto::topic::stream_write_message::write_request::MessageData;

use crate::client_topic::topicwriter::connection::ConnectionInfo;
use crate::client_topic::topicwriter::message_queue::MessageQueue;
use crate::client_topic::topicwriter::message_write_status::MessageWriteStatus;
use crate::client_topic::topicwriter::stream_writer::StreamWriter;
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
    pub(crate) writer_state: Arc<TokioMutex<TopicWriterState>>,
    pub(crate) cancellation_token: CancellationToken,
    pub(crate) confirmation_reception_queue: Arc<TokioMutex<TopicWriterReceptionQueue>>,
    pub(crate) message_queue: MessageQueue,
    pub(crate) connection_info: Arc<TokioMutex<ConnectionInfo>>,
    pub(crate) retrier: Arc<dyn Retry>,
    pub(crate) fatal_error_tx: oneshot::Sender<YdbError>,
}

enum ReconnectorState {
    Created,
    Working,
    Stopped,
}

pub(crate) struct Reconnector {
    state: Arc<TokioMutex<ReconnectorState>>,
    loop_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    confirmation_reception_queue: Arc<TokioMutex<TopicWriterReceptionQueue>>,
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

        let loop_join_handle = match Reconnector::start_reconnection_loop(
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
            params.fatal_error_tx,
        )
        .await
        {
            Ok(handle) => handle,
            Err(err) => return Err(err),
        };

        {
            let mut loop_handle = r.loop_handle.lock().await;
            *loop_handle = Some(loop_join_handle);
        }

        {
            let mut state = r.state.lock().await;
            *state = ReconnectorState::Working;
        }

        Ok(r)
    }

    async fn start_reconnection_loop(
        helper: ReconnectorLoopHelper,
        message_queue: MessageQueue,
        fatal_error_tx: oneshot::Sender<YdbError>,
    ) -> YdbResult<JoinHandle<()>> {
        let (done_once_tx, done_once_rx) = oneshot::channel::<()>();

        let reconnection_loop = tokio::spawn(Reconnector::reconnection_loop(
            helper,
            message_queue,
            done_once_tx,
            fatal_error_tx,
        ));

        match done_once_rx.await {
            Ok(_) => Ok(reconnection_loop),
            Err(err) => Err(YdbError::InternalError(format!(
                "connection_info_filled channel closed: {err}"
            ))),
        }
    }

    async fn reconnection_loop(
        helper: ReconnectorLoopHelper,
        message_queue: MessageQueue,
        done_once_tx: oneshot::Sender<()>,
        fatal_error_tx: oneshot::Sender<YdbError>,
    ) -> () {
        let mut reconnect_start_time = Instant::now();
        let mut attempt = 0;
        let mut done_once_tx = Some(done_once_tx);

        let mut stream_writer = None;
        let mut stream_writer_err = None;

        let mut final_error = None;

        loop {
            if let Some(err) = stream_writer_err {
                if !ReconnectorLoopHelper::is_retry_allowed(&err) {
                    final_error = Some(err);
                    break;
                }

                match helper.get_timeout_before_reconnect(attempt, reconnect_start_time.elapsed()) {
                    Some(wait_timeout) => {
                        trace!("Error, trying to reconnect: {err}");
                        helper
                            .set_writer_state(TopicWriterState::Reconnecting)
                            .await;
                        match helper.wait_before_reconnect(wait_timeout).await {
                            WaitBeforeReconnectResult::Ok => {}
                            WaitBeforeReconnectResult::Cancelled => break,
                        }
                    }
                    None => {
                        final_error = Some(YdbError::custom(format!(
                            "Reconnect is not allowed after {attempt} attempts for error: {err}",
                        )));
                        break;
                    }
                }
            }

            let (error_sender, error_receiver) = oneshot::channel();
            match helper
                .recreate_stream_writer(message_queue.clone(), error_sender)
                .await
            {
                Ok(sw) => {
                    stream_writer = Some(sw);
                    attempt = 0;
                    helper.set_writer_state(TopicWriterState::Working).await;
                    if let Some(tx) = done_once_tx.take() {
                        let _ = tx.send(());
                    };
                }
                Err(err) => {
                    trace!("Error creating stream writer: {err}");
                    stream_writer_err = Some(err);
                    attempt += 1;
                    continue;
                }
            };

            tokio::select! {
                _ = helper.cancellation_token.cancelled() => {
                    break;
                }
                received_err = error_receiver => {
                    let err = match received_err {
                        Ok(err) => err,
                        Err(chan_err) => {
                            final_error = Some(YdbError::custom(format!("Channel error: {chan_err}")));
                            break;
                        }
                    };

                    stream_writer_err = Some(err);
                    reconnect_start_time = Instant::now();
                }
            }
        }

        if let Some(stream_writer) = stream_writer {
            let _ = stream_writer.stop().await;
        }

        if let Some(final_error) = final_error {
            if let Err(err) = fatal_error_tx.send(final_error.clone()) {
                error!("can't send fatal error to TopicWriter: channel is closed: {err}");
            }

            helper
                .set_writer_state(TopicWriterState::FinishedWithError(final_error))
                .await;
        }
    }

    pub(crate) async fn stop(self) -> YdbResult<()> {
        let mut state = self.state.lock().await;
        match *state {
            ReconnectorState::Created => Ok(()),
            ReconnectorState::Working => {
                let mut loop_handle = self.loop_handle.lock().await;

                match loop_handle.take() {
                    Some(loop_handle) => {
                        *state = ReconnectorState::Stopped;
                        loop_handle.await.map_err(|err| {
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
            let mut reception_queue = self.confirmation_reception_queue.lock().await;
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
    writer_state: Arc<TokioMutex<TopicWriterState>>,
    writer_options: TopicWriterOptions,
    producer_id: String,
    confirmation_reception_queue: Arc<TokioMutex<TopicWriterReceptionQueue>>,
}

enum WaitBeforeReconnectResult {
    Ok,
    Cancelled,
}

impl ReconnectorLoopHelper {
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
            self.connection_info.clone(),
            self.confirmation_reception_queue.clone(),
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

        if !decision.allow_retry {
            return None;
        }

        Some(decision.wait_timeout)
    }

    async fn wait_before_reconnect(&self, wait_timeout: Duration) -> WaitBeforeReconnectResult {
        match timeout(wait_timeout, self.cancellation_token.cancelled()).await {
            Ok(_) => WaitBeforeReconnectResult::Cancelled,
            Err(_) => WaitBeforeReconnectResult::Ok,
        }
    }

    async fn set_writer_state(&self, new_state: TopicWriterState) {
        let mut writer_state = self.writer_state.lock().await;
        *writer_state = new_state;
    }
}
