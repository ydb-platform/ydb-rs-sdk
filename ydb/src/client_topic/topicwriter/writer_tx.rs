use std::sync::Arc;

use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::topic::TransactionIdentity;

use tracing::instrument;

use crate::client_query::Transaction;
use crate::client_query::hooks::{QueryTxCommitStatus, QueryTxHook};
use crate::client_topic::compression::Executor;
use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::writer::TopicWriter;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::{YdbError, YdbResult};

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

impl WriterTxHook {
    #[instrument(name = "ydb.TopicWriterTx.Flush", skip_all, fields(db.system.name = "ydb"), err)]
    async fn flush(&self) -> YdbResult<()> {
        self.writer.flush_inner().await
    }
}

#[async_trait::async_trait]
impl QueryTxHook for WriterTxHook {
    async fn before_commit(&mut self) -> YdbResult<()> {
        self.flush().await.map_err(normalize_topic_tx_error)
    }

    fn after_commit(&mut self, _status: QueryTxCommitStatus) {}
}

fn normalize_topic_tx_error(mut error: YdbError) -> YdbError {
    // The Topic transaction bridge collapses KQP transaction failures, including a vanished
    // Query session reported as BAD_SESSION, to UNKNOWN_TXID and exposes it as NOT_FOUND. At this
    // boundary that means the Topic transaction identity can no longer be used. The observed
    // response was:
    // operation_status=400140, issue_code=500030,
    // message="status is not ok: <main>: Error: Session not found.\n".
    // Some server responses omit all issue details, so this workaround deliberately matches the
    // status at the exact Topic transaction boundary instead of depending on issue_code or text.
    // Ordinary Query NOT_FOUND errors retain their documented retry and session-reuse policy.
    // https://ydb.tech/docs/en/reference/ydb-sdk/ydb-status-codes
    if let YdbError::YdbStatusError(status) = &mut error
        && status.operation_status == StatusCode::NotFound as i32
    {
        status.operation_status = StatusCode::BadSession as i32;
    }
    error
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
    #[instrument(name = "ydb.TopicWriterTx.Write", skip_all, fields(db.system.name = "ydb"), err)]
    pub async fn write(&mut self, message: TopicWriterMessage) -> YdbResult<()> {
        self.inner.write_inner(message).await
    }
}
