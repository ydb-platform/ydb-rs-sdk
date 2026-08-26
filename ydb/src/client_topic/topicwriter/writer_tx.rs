use std::sync::Arc;

use ydb_grpc::ydb_proto::topic::TransactionIdentity;

use crate::client_query::Transaction;
use crate::client_query::hooks::{QueryTxCommitStatus, QueryTxHook};
use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::state::WriterState;
use crate::client_topic::topicwriter::writer::TopicWriter;
use crate::{YdbError, YdbResult};
use tracing::{error, instrument};

/// A temporary transactional view of an existing topic writer.
///
/// Messages written through this wrapper belong to the query transaction and become visible only
/// after it commits. Ordinary writes through the underlying writer remain disabled until the
/// transaction finishes.
pub struct TopicWriterTx<'writer> {
    writer: &'writer mut TopicWriter,
    transaction_identity: Arc<TransactionIdentity>,
}

struct WriterTxHook {
    state: WriterState,
    transaction_identity: Arc<TransactionIdentity>,
}

#[async_trait::async_trait]
impl QueryTxHook for WriterTxHook {
    async fn before_commit(&mut self) -> YdbResult<()> {
        self.state
            .begin_commit_and_flush(&self.transaction_identity)
            .await
    }

    fn after_commit(&mut self, status: QueryTxCommitStatus) {
        let result = match status {
            QueryTxCommitStatus::Committed => self
                .state
                .finish_committed_transaction(&self.transaction_identity),
            QueryTxCommitStatus::Aborted => self.state.finish_aborted_transaction(
                &self.transaction_identity,
                YdbError::custom(format!(
                    "query transaction was aborted: transaction_id={}",
                    self.transaction_identity.id,
                )),
            ),
        };

        if let Err(error) = result {
            error!(
                transaction_id = %self.transaction_identity.id,
                %error,
                "failed to finish topic writer transaction binding",
            );
        }
    }
}

impl<'writer> TopicWriterTx<'writer> {
    pub(super) async fn new(
        writer: &'writer mut TopicWriter,
        tx: &mut Transaction,
    ) -> YdbResult<Self> {
        writer.flush_inner().await?;

        let (session, id) = tx.identity().await?;
        let transaction_identity = Arc::new(TransactionIdentity { id, session });
        let state = writer.state();
        tx.register_hook(WriterTxHook {
            state: state.clone(),
            transaction_identity: transaction_identity.clone(),
        })?;
        state.bind_transaction(transaction_identity.clone())?;

        Ok(Self {
            writer,
            transaction_identity,
        })
    }

    /// Enqueues a message for transactional write.
    ///
    /// No topic offset is returned. Transactional topic writes are published, and receive their
    /// final offsets, only when the transaction commits.
    #[instrument(name = "ydb.TopicWriterTx.Write", skip_all, fields(db.system.name = "ydb"), err)]
    pub async fn write(&self, message: TopicWriterMessage) -> YdbResult<()> {
        self.writer
            .write_transactional_inner(message, &self.transaction_identity)
            .await
    }
}
