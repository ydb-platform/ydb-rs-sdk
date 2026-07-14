use std::sync::Arc;

use ydb_grpc::ydb_proto::topic::TransactionIdentity;

use tracing::instrument;

use crate::client_query::Transaction;
use crate::client_topic::compression::Executor;
use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::message_write_status::{
    MessageSkipReason, MessageWriteStatus,
};
use crate::client_topic::topicwriter::writer::TopicWriter;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::{YdbError, YdbResult};

use super::writer_tx_options::TopicWriterTxOptions;

/// A topic writer bound to an active YDB transaction.
///
/// Messages written through this writer are attached to the transaction and become visible
/// only after the transaction is committed.
pub struct TopicWriterTx {
    inner: TopicWriter,
}

impl TopicWriterTx {
    pub(crate) async fn new(
        options: TopicWriterTxOptions,
        connection_manager: GrpcConnectionManager,
        executor: Arc<dyn Executor>,
        tx: &mut Transaction,
    ) -> YdbResult<Self> {
        let (session_id, transaction_id) = tx.identity().await?;

        let tx_identity = TransactionIdentity {
            id: transaction_id,
            session: session_id,
        };

        // All validation and configuration, specific for `TopicWriterTx` should be done in
        // options construction and conversion.
        let options = options.into_non_tx_options();

        let inner =
            TopicWriter::with_tx_identity(options, connection_manager, executor, tx_identity)
                .await?;

        Ok(Self { inner })
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
    #[instrument(name = "ydb.TopicWriterTx.Write", skip_all, fields(db.system.name = "ydb"), err)]
    pub async fn write(&mut self, message: TopicWriterMessage) -> YdbResult<()> {
        match self.inner.write_with_ack(message).await? {
            MessageWriteStatus::WrittenInTx(_) => Ok(()),

            MessageWriteStatus::Skipped(MessageSkipReason::AlreadyWritten) => Ok(()),

            other_message_status => Err(YdbError::custom(format!(
                "expected WrittenInTx or AlreadyWritten ack from server, got: {other_message_status:?}"
            ))),
        }
    }

    /// Shuts down the writer and waits for its background tasks to finish.
    ///
    /// Calling `stop` is the explicit way to finish using the writer before committing or
    /// rolling back the transaction. Dropping the writer cancels its background work, but
    /// does not report shutdown errors.
    #[instrument(name = "ydb.TopicWriterTx.Stop", skip_all, fields(db.system.name = "ydb"), err)]
    pub async fn stop(self) -> YdbResult<()> {
        self.inner.stop().await
    }
}
