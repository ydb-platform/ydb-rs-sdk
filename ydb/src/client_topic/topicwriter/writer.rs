use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::{oneshot, Mutex as TokioMutex, RwLock as TokioRwLock};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::log::trace;

use crate::client_topic::topicwriter::connection::ConnectionInfo;
use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::message_queue::MessageQueue;
use crate::client_topic::topicwriter::message_write_status::MessageWriteStatus;
use crate::client_topic::topicwriter::reconnector::{Reconnector, ReconnectorParams};
use crate::client_topic::topicwriter::writer_options::TopicWriterOptions;
use crate::client_topic::topicwriter::writer_reception_queue::TopicWriterReceptionQueue;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_topic_service::common::codecs::RawSupportedCodecs;
use crate::retry::TimeoutRetrier;
use crate::{YdbError, YdbResult};

pub(crate) enum TopicWriterState {
    Working,
    Reconnecting,
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

    flush_timeout: Duration,

    cancellation_token: CancellationToken,

    connection_info: Arc<TokioMutex<ConnectionInfo>>,
    writer_state: Arc<TokioMutex<TopicWriterState>>,
    fatal_error: Arc<TokioRwLock<Option<YdbError>>>,

    confirmation_reception_queue: Arc<TokioMutex<TopicWriterReceptionQueue>>,
    message_queue: MessageQueue,

    wait_for_fatal_error_handle: JoinHandle<()>,

    reconnector: Reconnector,
}

#[allow(dead_code)]
pub struct AckFuture {
    receiver: oneshot::Receiver<YdbResult<MessageWriteStatus>>,
}

impl Future for AckFuture {
    type Output = YdbResult<MessageWriteStatus>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(_cx) {
            // Inner value is already Ok(status) or Err(from send_error_if_needed).
            Poll::Ready(Ok(write_result)) => Poll::Ready(write_result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(YdbError::custom("message writer was closed"))),
            Poll::Pending => Poll::Pending,
        }
    }
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

        let writer_state = Arc::new(TokioMutex::new(TopicWriterState::Working));
        let message_queue = MessageQueue::new();

        let confirmation_reception_queue =
            Arc::new(TokioMutex::new(TopicWriterReceptionQueue::new()));
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

        let fatal_error = Arc::new(TokioRwLock::new(None));
        let (fatal_error_tx, fatal_error_rx) = oneshot::channel();
        let wait_for_fatal_error_handle = tokio::spawn(TopicWriter::wait_for_fatal_error(
            cancellation_token.clone(),
            fatal_error_rx,
            confirmation_reception_queue.clone(),
            fatal_error.clone(),
        ));

        let reconnector = Reconnector::new(ReconnectorParams {
            writer_options: writer_options.clone(),
            producer_id: producer_id.clone(),
            connection_manager,
            writer_state: writer_state.clone(),
            cancellation_token: cancellation_token.clone(),
            confirmation_reception_queue: confirmation_reception_queue.clone(),
            message_queue: message_queue.clone(),
            connection_info: connection_info.clone(),
            retrier,
            fatal_error_tx,
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
            cancellation_token,
            writer_state,
            confirmation_reception_queue,
            message_queue,
            reconnector,
            wait_for_fatal_error_handle,
            fatal_error,
        })
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

        rx.await
            .unwrap_or_else(|chan_err| Err(YdbError::from(chan_err)))
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
        wait_ack: Option<oneshot::Sender<YdbResult<MessageWriteStatus>>>,
    ) -> YdbResult<()> {
        self.check_working().await?;

        let mut connection_info = self.connection_info.lock().await;
        if self.auto_set_seq_no {
            if message.seq_no.is_some() {
                return Err(YdbError::custom(
                    "force set message seqno is only possible if auto_set_seq_no is disabled",
                ));
            }
            message.seq_no = Some(connection_info.last_seq_no_assigned + 1);
        };

        if let Some(mess_seqno) = message.seq_no {
            connection_info.last_seq_no_assigned = mess_seqno;
        } else {
            return Err(YdbError::custom("empty message seq_no is provided"));
        }

        self.reconnector
            .add_message_for_processing(message, wait_ack)
            .await?;

        Ok(())
    }

    pub async fn flush(&self) -> YdbResult<()> {
        self.check_working().await?;

        let flush_op_completed = {
            let mut reception_queue = self.confirmation_reception_queue.lock().await;
            reception_queue.init_flush_op()?
        };

        self.message_queue
            .wait_for_messages_to_be_acknowledged(&self.cancellation_token)
            .await;

        Ok(flush_op_completed.await?)
    }

    async fn check_working(&self) -> YdbResult<()> {
        let state = self.writer_state.lock().await;
        match state.deref() {
            TopicWriterState::Working => Ok(()),
            TopicWriterState::Reconnecting => Ok(()),
            TopicWriterState::FinishedWithError(err) => Err(err.clone()),
        }
    }

    async fn wait_for_fatal_error(
        cancellation_token: CancellationToken,
        fatal_error_rx: oneshot::Receiver<YdbError>,
        confirmation_reception_queue: Arc<TokioMutex<TopicWriterReceptionQueue>>,
        fatal_error: Arc<TokioRwLock<Option<YdbError>>>,
    ) {
        tokio::select! {
            _ = cancellation_token.cancelled() => {}
            result = fatal_error_rx => {
                let err = result.unwrap_or_else(YdbError::from);

                {
                    let mut fatal_error = fatal_error.write().await;
                    *fatal_error = Some(err.clone());
                }
                {
                    let mut reception_queue = confirmation_reception_queue.lock().await;
                    reception_queue.send_error_to_tickets_and_clear(err);
                }
            }
        }
    }

    pub async fn stop(self) -> YdbResult<()> {
        trace!("Stopping...");

        self.message_queue.close_for_new_messages().await;
        let flush_result = match timeout(self.flush_timeout, self.flush()).await {
            Ok(result) => result,
            Err(_) => Err(YdbError::custom(
                "stop: flush() timed out while stopping topic writer",
            )),
        };

        self.cancellation_token.cancel();
        let reconnector_result = self.reconnector.stop().await.map_err(|err| {
            YdbError::custom(format!(
                "stop: error while waiting for reconnector to finish: {err}"
            ))
        });

        let wait_for_fatal_error_result = self.wait_for_fatal_error_handle.await.map_err(|err| {
            YdbError::custom(format!(
                "stop: error while waiting for wait_for_fatal_error to finish: {err}"
            ))
        });

        trace!("Reconnection loop stopped");

        // First clean up all resources, then return result.
        flush_result?;
        reconnector_result?;
        wait_for_fatal_error_result?;

        Ok(())
    }
}
