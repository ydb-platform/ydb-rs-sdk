use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::sync::Mutex as TokioMutex;
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
    pub(crate) connection_info: Arc<TokioMutex<ConnectionInfo>>,

    flush_timeout: Duration,

    cancellation_token: CancellationToken,
    writer_state: Arc<Mutex<TopicWriterState>>,

    confirmation_reception_queue: Arc<Mutex<TopicWriterReceptionQueue>>,
    message_queue: MessageQueue,

    reconnector: Reconnector,
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
        let message_queue = MessageQueue::new();

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
        })
    }

    pub async fn write(&mut self, message: TopicWriterMessage) -> YdbResult<()> {
        self.write_message(message, None).await?;
        Ok(())
    }

    // TODO: В случае нетрабельной ошибки, мы хотим ее сохранить (например, "пропал" топик).
    // Для новых вызовов методов - возвращаем эту ошибку.
    // Для старых - хотим через канал (уже есть, снизу, oneshot::channel()) возвращать эту ошибку ожидающим.
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
            let mut reception_queue = self.confirmation_reception_queue.lock().unwrap();
            reception_queue.init_flush_op()?
        };

        self.message_queue
            .wait_for_messages_to_be_acknowledged()
            .await;

        Ok(flush_op_completed.await?)
    }

    async fn check_working(&self) -> YdbResult<()> {
        let state = self.writer_state.lock().unwrap();
        match state.deref() {
            TopicWriterState::Working => Ok(()),
            TopicWriterState::Reconnecting => Ok(()),
            TopicWriterState::FinishedWithError(err) => Err(err.clone()),
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
        let loop_result = self.reconnector.stop().await.map_err(|err| {
            YdbError::custom(format!(
                "stop: error while waiting for reconnector to finish: {err}"
            ))
        });

        trace!("Reconnection loop stopped");

        // First clean up all resources, then return result.
        flush_result?;
        loop_result?;

        Ok(())
    }
}
