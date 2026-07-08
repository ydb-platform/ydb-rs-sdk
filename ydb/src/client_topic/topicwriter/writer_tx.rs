use std::sync::Arc;

use ydb_grpc::ydb_proto::topic::TransactionIdentity;

use crate::YdbResult;
use crate::client_query::Transaction;
use crate::client_query::hooks::{QueryTxCommitStatus, QueryTxHook};
use crate::client_topic::compression::Executor;
use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::writer::TopicWriter;
use crate::grpc_connection_manager::GrpcConnectionManager;

use super::writer_tx_options::TopicWriterTxOptions;

/// A topic writer bound to an active YDB transaction.
///
/// Messages written through this writer are attached to the transaction and become visible
/// only after the transaction is committed.
pub struct TopicWriterTx {
    inner: Arc<TopicWriter>,
}

struct WriterTxHook {
    writer: Arc<TopicWriter>,
}

#[async_trait::async_trait]
impl QueryTxHook for WriterTxHook {
    async fn before_commit(&mut self) -> YdbResult<()> {
        self.writer.flush().await
    }

    fn after_commit(&mut self, _status: QueryTxCommitStatus) {}
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

        let writer =
            TopicWriter::with_tx_identity(options, connection_manager, executor, tx_identity)
                .await?;

        let inner = Arc::new(writer);
        tx.register_hook(WriterTxHook {
            writer: inner.clone(),
        });

        Ok(Self { inner })
    }

    /// Enqueues a message for transactional write.
    ///
    /// No topic offset is returned. Transactional topic writes are published, and receive
    /// their final offsets, only when the transaction commits.
    pub async fn write(&mut self, message: TopicWriterMessage) -> YdbResult<()> {
        self.inner.write(message).await
    }
}
