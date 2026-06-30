use std::marker::PhantomData;
use std::sync::Arc;

use ydb_grpc::ydb_proto::topic::TransactionIdentity;

use crate::client_topic::compression::Executor;
use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::message_write_status::{
    MessageSkipReason, MessageWriteStatus,
};
use crate::client_topic::topicwriter::writer::TopicWriter;
use crate::client_topic::topicwriter::writer_options::TopicWriterOptions;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::{Transaction, YdbError, YdbResult};

/// A topic writer bound to an active YDB transaction.
///
/// Messages written through this writer are attached to the transaction and become visible
/// only after the transaction is committed. The writer borrows the transaction mutably for
/// its whole lifetime, so callers cannot use the transaction again until the writer is
/// stopped or dropped.
pub struct TopicWriterTx<'a> {
    inner: TopicWriter,
    _tx: PhantomData<&'a mut dyn Transaction>,
}

impl<'a> TopicWriterTx<'a> {
    pub(crate) async fn new(
        options: TopicWriterOptions,
        connection_manager: GrpcConnectionManager,
        executor: Arc<dyn Executor>,
        tx: &'a mut dyn Transaction,
    ) -> YdbResult<Self> {
        let info = tx.transaction_info().await?;

        let tx_identity = TransactionIdentity {
            id: info.transaction_id.clone(),
            session: info.session_id.clone(),
        };

        let inner =
            TopicWriter::with_tx_identity(options, connection_manager, executor, tx_identity)
                .await?;

        Ok(Self {
            inner,
            _tx: PhantomData,
        })
    }

    /// Writes a message and waits for the server acknowledgement.
    ///
    /// Returns `Ok(())` when YDB acknowledges the message as `WrittenInTx`. A
    /// `Skipped(AlreadyWritten)` acknowledgement is also treated as success, because it
    /// means the server deduplicated the write by sequence number.
    ///
    /// No topic offset is returned. Transactional topic writes are published, and receive
    /// their final offsets, only when the transaction commits.
    ///
    /// # Cancel safety
    ///
    /// This method is not cancel safe. If the returned future is dropped before it
    /// completes, the SDK cannot tell whether YDB attached the message to the transaction.
    /// Roll the transaction back if that ambiguity is not acceptable.
    pub async fn write(&mut self, message: TopicWriterMessage) -> YdbResult<()> {
        match self.inner.write_with_ack(message).await? {
            MessageWriteStatus::WrittenInTx(_) => Ok(()),

            MessageWriteStatus::Skipped(MessageSkipReason::AlreadyWritten) => Ok(()),

            status => Err(YdbError::custom(format!(
                "expected WrittenInTx or AlreadyWritten ack from server, got: {status:?}"
            ))),
        }
    }

    /// Shuts down the writer and waits for its background tasks to finish.
    ///
    /// Calling `stop` is the explicit way to finish using the writer before committing or
    /// rolling back the transaction. Dropping the writer cancels its background work, but
    /// does not report shutdown errors.
    pub async fn stop(self) -> YdbResult<()> {
        self.inner.stop().await
    }
}
