use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::sync::{RwLock, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::trace;

use crate::client_topic::compression::Executor;
use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::message_write_status::MessageWriteStatus;
use crate::client_topic::topicwriter::reconnector::{Reconnector, ReconnectorParams};
use crate::client_topic::topicwriter::writer_options::TopicWriterOptions;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::{YdbError, YdbResult};
use ydb_grpc::ydb_proto::topic::TransactionIdentity;

/// TopicWriter is currently in development.
/// It is mostly usable, but has some unimplemented features.
pub struct TopicWriter {
    fatal_error: Arc<RwLock<Option<YdbError>>>,
    wait_for_fatal_error_handle: JoinHandle<()>,
    reconnector: Reconnector,
    _cancel_on_drop: DropGuard,
}

pub struct AckFuture {
    receiver: oneshot::Receiver<YdbResult<MessageWriteStatus>>,
}

impl Future for AckFuture {
    type Output = YdbResult<MessageWriteStatus>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
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
        executor: Arc<dyn Executor>,
    ) -> YdbResult<Self> {
        Self::new_inner(writer_options, connection_manager, executor, None).await
    }

    pub(crate) async fn with_tx_identity(
        writer_options: TopicWriterOptions,
        connection_manager: GrpcConnectionManager,
        executor: Arc<dyn Executor>,
        tx_identity: TransactionIdentity,
    ) -> YdbResult<Self> {
        Self::new_inner(
            writer_options,
            connection_manager,
            executor,
            Some(tx_identity),
        )
        .await
    }

    async fn new_inner(
        writer_options: TopicWriterOptions,
        connection_manager: GrpcConnectionManager,
        executor: Arc<dyn Executor>,
        tx_identity: Option<TransactionIdentity>,
    ) -> YdbResult<Self> {
        let producer_id = writer_options
            .producer_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let cancellation_token = CancellationToken::new();
        let cancel_on_drop = cancellation_token.clone().drop_guard();

        let retrier = writer_options.retrier.clone();

        let (fatal_error_tx, fatal_error_rx) = oneshot::channel();

        let reconnector = Reconnector::new(ReconnectorParams {
            writer_options: writer_options.clone(),
            producer_id: producer_id.clone(),
            connection_manager,
            cancellation_token: cancellation_token.clone(),
            retrier,
            fatal_error_tx,
            flush_timeout: writer_options.flush_timeout,
            executor,
            tx_identity,
        })
        .await?;

        let fatal_error = Arc::new(RwLock::new(None));
        let wait_for_fatal_error_handle = tokio::spawn(TopicWriter::wait_for_fatal_error(
            cancellation_token,
            fatal_error_rx,
            fatal_error.clone(),
        ));

        Ok(Self {
            fatal_error,
            wait_for_fatal_error_handle,
            reconnector,
            _cancel_on_drop: cancel_on_drop,
        })
    }

    pub async fn write(&self, message: TopicWriterMessage) -> YdbResult<()> {
        self.write_message(message, None).await
    }

    pub async fn write_with_ack(
        &self,
        message: TopicWriterMessage,
    ) -> YdbResult<MessageWriteStatus> {
        let (tx, rx) = oneshot::channel();

        self.write_message(message, Some(tx)).await?;

        rx.await
            .unwrap_or_else(|chan_err| Err(YdbError::from(chan_err)))
    }

    pub async fn write_with_ack_future(&self, message: TopicWriterMessage) -> YdbResult<AckFuture> {
        let (tx, rx) = oneshot::channel();

        self.write_message(message, Some(tx)).await?;

        Ok(AckFuture { receiver: rx })
    }

    async fn write_message(
        &self,
        message: TopicWriterMessage,
        ack_sender: Option<oneshot::Sender<YdbResult<MessageWriteStatus>>>,
    ) -> YdbResult<()> {
        if let Some(err) = self.fatal_error.read().await.as_ref() {
            return Err(err.clone());
        }

        self.reconnector.add_message(message, ack_sender).await?;

        Ok(())
    }

    pub async fn flush(&self) -> YdbResult<()> {
        self.reconnector.flush().await
    }

    async fn wait_for_fatal_error(
        cancellation_token: CancellationToken,
        fatal_error_rx: oneshot::Receiver<YdbError>,
        fatal_error: Arc<RwLock<Option<YdbError>>>,
    ) {
        tokio::select! {
            _ = cancellation_token.cancelled() => {}
            result = fatal_error_rx => {
                let err = result.unwrap_or_else(YdbError::from);

                {
                    let mut fatal_error = fatal_error.write().await;
                    *fatal_error = Some(err.clone());
                }
            }
        }
    }

    pub async fn stop(self) -> YdbResult<()> {
        trace!("stopping...");

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

        trace!("reconnection loop stopped");

        // First clean up all resources, then return result.
        reconnector_result?;
        wait_for_fatal_error_result?;

        Ok(())
    }
}
