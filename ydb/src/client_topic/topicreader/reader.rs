use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::Future;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use tracing::instrument;

use crate::client_common::TokenCache;
use crate::client_query::Transaction;
use crate::client_topic::compression::Executor;
use crate::client_topic::topicreader::ids::{PartitionId, PartitionSessionId};
use crate::client_topic::topicreader::messages::TopicReaderBatch;
use crate::client_topic::topicreader::reader_options::TopicReaderOptions;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_topic_service::client::RawTopicClient;
use crate::grpc_wrapper::raw_topic_service::common::partition::RawOffsetsRange;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawTopicReadSettings;
use crate::grpc_wrapper::raw_topic_service::update_offsets_in_transaction::{
    RawPartitionOffsets, RawTopicOffsets, RawTransactionIdentity,
    RawUpdateOffsetsInTransactionRequest,
};
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use crate::{YdbError, YdbResult};

use super::reader_tx::TopicReaderTx;
use super::reconnector::{Reconnector, ReconnectorTask};
use super::runtime::RuntimeHandle;

pub struct TopicReader {
    pub(super) manager: GrpcConnectionManager,
    options: TopicReaderOptions,
    reconnect_handle: JoinHandle<YdbResult<()>>,
    runtime: RuntimeHandle,
    cancellation: CancellationToken,
}

static READER_ID: AtomicUsize = AtomicUsize::new(0);

fn new_reader_id() -> usize {
    READER_ID.fetch_add(1, Ordering::Relaxed)
}

impl TopicReader {
    pub(crate) async fn new(
        options: TopicReaderOptions,
        manager: GrpcConnectionManager,
        token_cache: TokenCache,
        compression_executor: Arc<dyn Executor>,
    ) -> YdbResult<Self> {
        let cancellation = CancellationToken::new();
        let ReconnectorTask {
            join_handle,
            runtime,
            cancellation_token,
        } = Reconnector::new(
            manager.clone(),
            options.clone(),
            token_cache,
            compression_executor,
            cancellation,
            new_reader_id(),
        )
        .run();

        Ok(Self {
            manager,
            options,
            reconnect_handle: join_handle,
            runtime,
            cancellation: cancellation_token,
        })
    }

    #[instrument(name = "ydb.TopicReader.ReadBatch", skip_all, fields(db.system.name = "ydb"), err)]
    pub async fn read_batch(&mut self) -> YdbResult<TopicReaderBatch> {
        self.read_batch_inner().await
    }

    pub(super) async fn read_batch_inner(&mut self) -> YdbResult<TopicReaderBatch> {
        self.runtime.pop_batch(self.options.batch_size).await
    }

    /// Read a batch and register consumer offsets via [`UpdateOffsetsInTransaction`]
    /// using the given [`Transaction`].
    ///
    /// Offsets are committed when the query transaction commits.
    #[instrument(name = "ydb.TopicReader.PopBatchInTx", skip_all, fields(db.system.name = "ydb"), err)]
    pub async fn pop_batch_in_tx(&mut self, tx: &mut Transaction) -> YdbResult<TopicReaderBatch> {
        let (session_id, transaction_id) = tx.identity().await?;
        let batch = self.read_batch_inner().await?;
        self.update_offsets_in_transaction(&batch, session_id, transaction_id)
            .await?;
        Ok(batch)
    }

    #[instrument(name = "ydb.TopicReader.TxReader", skip_all, fields(db.system.name = "ydb"), err)]
    pub async fn tx_reader<'a>(&'a mut self, tx: &mut Transaction) -> YdbResult<TopicReaderTx<'a>> {
        TopicReaderTx::new(self, tx).await
    }

    /// Sends a commit for the given [`TopicReaderCommitMarker`] to the server.
    ///
    /// Returns as soon as the commit message has been queued for sending; it does
    /// not wait for the server to acknowledge the commit. Use
    /// [`commit_with_ack`](Self::commit_with_ack) if you need that guarantee.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit message could not be queued.
    pub fn commit(&mut self, commit_marker: TopicReaderCommitMarker) -> YdbResult<()> {
        self.runtime.commit(commit_marker).map(|_| ())
    }

    /// Sends a commit for the given [`TopicReaderCommitMarker`] and returns a
    /// handle that resolves once the server acknowledges it.
    ///
    /// `.await` the returned handle to wait for the acknowledgement; it
    /// resolves to `Ok(())` when the server acks the commit.
    ///
    /// # Errors
    ///
    /// The handle resolves to an error if the acknowledgement will never
    /// arrive, either because the initial send failed or because the reader was
    /// closed before the server replied.
    pub fn commit_with_ack(
        &mut self,
        commit_marker: TopicReaderCommitMarker,
    ) -> impl Future<Output = YdbResult<()>> + use<> {
        let ack = self.runtime.commit(commit_marker);

        async {
            let ack = ack?;

            match ack.await {
                Ok(res) => res,
                Err(_) => Err(YdbError::custom(
                    "commit channel was closed without error message",
                )),
            }
        }
    }

    pub(super) fn runtime_handle(&self) -> RuntimeHandle {
        self.runtime.clone()
    }

    pub(super) fn consumer(&self) -> &str {
        &self.options.consumer
    }

    async fn update_offsets_in_transaction(
        &self,
        batch: &TopicReaderBatch,
        session_id: String,
        transaction_id: String,
    ) -> YdbResult<()> {
        let commit_marker = batch.get_commit_marker();

        let request = RawUpdateOffsetsInTransactionRequest {
            operation_params: RawOperationParams::new_with_timeouts(
                Duration::from_secs(30),
                Duration::from_secs(60),
            ),
            tx: RawTransactionIdentity {
                id: transaction_id,
                session: session_id,
            },
            topics: vec![RawTopicOffsets {
                path: commit_marker.topic.clone(),
                partitions: vec![RawPartitionOffsets {
                    partition_id: commit_marker.partition_id.into_raw(),
                    partition_offsets: vec![RawOffsetsRange {
                        start: commit_marker.start_offset,
                        end: commit_marker.end_offset,
                    }],
                }],
            }],
            consumer: self.options.consumer.clone(),
        };

        let mut topic_service = self.manager.get_auth_service(RawTopicClient::new).await?;
        topic_service.update_offsets_in_transaction(request).await?;

        Ok(())
    }
}

impl Drop for TopicReader {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.reconnect_handle.abort();
    }
}

#[derive(Clone)]
pub struct TopicSelectors(pub Vec<TopicSelector>);

impl TopicSelectors {
    pub(crate) fn into_topics_read_settings(self) -> Vec<RawTopicReadSettings> {
        self.0
            .into_iter()
            .map(|selector| selector.into_raw_topic_read_setting())
            .collect()
    }
}

#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
#[derive(bon::Builder, Clone)]
pub struct TopicSelector {
    #[builder(into)]
    pub path: String,
    pub partition_ids: Option<Vec<i64>>,
    pub read_from: Option<SystemTime>,
}

impl TopicSelector {
    pub(crate) fn into_raw_topic_read_setting(self) -> RawTopicReadSettings {
        RawTopicReadSettings {
            path: self.path,
            partition_ids: self.partition_ids.unwrap_or_default(),
            read_from: self.read_from.map(|time| time.into()),
            max_lag: None,
        }
    }
}

impl TopicSelector {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            partition_ids: None,
            read_from: Some(UNIX_EPOCH),
        }
    }
}

impl<S: Into<String>> From<S> for TopicSelector {
    fn from(path: S) -> Self {
        Self::new(path)
    }
}

impl<S: Into<TopicSelector>> From<S> for TopicSelectors {
    fn from(s: S) -> Self {
        Self(vec![s.into()])
    }
}

impl<S: Into<TopicSelector>> FromIterator<S> for TopicSelectors {
    fn from_iter<I: IntoIterator<Item = S>>(iter: I) -> Self {
        Self(iter.into_iter().map(Into::into).collect())
    }
}

#[derive(Clone, Debug)]
pub struct TopicReaderCommitMarker {
    pub(crate) partition_session_id: PartitionSessionId,
    pub(crate) partition_id: PartitionId,
    pub(crate) start_offset: i64,
    pub(crate) end_offset: i64,
    pub(crate) topic: String,
    pub(crate) epoch: usize,
}
