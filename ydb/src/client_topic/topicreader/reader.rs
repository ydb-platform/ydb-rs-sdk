use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::Future;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::client_common::TokenCache;
use crate::client_topic::compression::Executor;
use crate::client_topic::topicreader::ids::{PartitionId, PartitionSessionId};
use crate::client_topic::topicreader::messages::TopicReaderBatch;
use crate::client_topic::topicreader::reader_options::{
    TopicReaderOptions, TopicReaderOptionsBuilder,
};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_topic_service::client::RawTopicClient;
use crate::grpc_wrapper::raw_topic_service::common::partition::RawOffsetsRange;
use crate::grpc_wrapper::raw_topic_service::stream_read::messages::RawTopicReadSettings;
use crate::grpc_wrapper::raw_topic_service::update_offsets_in_transaction::{
    RawPartitionOffsets, RawTopicOffsets, RawTransactionIdentity,
    RawUpdateOffsetsInTransactionRequest,
};
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use crate::transaction::{Transaction, TransactionInfo};
use crate::{YdbError, YdbResult};

use super::reconnector::{Reconnector, ReconnectorTask};
use super::runtime::RuntimeHandle;

pub struct TopicReader {
    manager: GrpcConnectionManager,
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

    pub async fn read_batch(&mut self) -> YdbResult<TopicReaderBatch> {
        self.runtime.pop_batch(self.options.batch_size).await
    }

    /// WARN: DO NOT USE IN PRODUCTION
    ///
    /// Read a batch of messages within a transaction context.
    /// The TopicReaderBatch from the result will be committed within the `tx` transaction.
    /// This is an EXAMPLE of the interface. IT IS NOT PRODUCTION READY.
    /// The reader will fail consistently on ANY error, including TLI.
    pub async fn pop_batch_in_tx(
        &mut self,
        tx: &mut Box<dyn Transaction>,
    ) -> YdbResult<TopicReaderBatch> {
        let tx_info = tx.transaction_info().await?;
        let batch = self.read_batch().await?;
        self.update_offsets_in_transaction(&batch, &tx_info).await?;
        Ok(batch)
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
    ) -> impl Future<Output = YdbResult<()>> {
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

    async fn update_offsets_in_transaction(
        &self,
        batch: &TopicReaderBatch,
        tx_info: &TransactionInfo,
    ) -> YdbResult<()> {
        let commit_marker = batch.get_commit_marker();

        let request = RawUpdateOffsetsInTransactionRequest {
            operation_params: RawOperationParams::new_with_timeouts(
                Duration::from_secs(30),
                Duration::from_secs(60),
            ),
            tx: RawTransactionIdentity {
                id: tx_info.transaction_id.clone(),
                session: tx_info.session_id.clone(),
            },
            topics: vec![RawTopicOffsets {
                path: commit_marker.topic.clone(),
                partitions: vec![RawPartitionOffsets {
                    partition_id: commit_marker.partition_id.as_raw(),
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
#[derive(Clone)]
pub struct TopicSelector {
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

impl TopicReaderOptionsBuilder {
    pub fn from_consumer_topic(
        consumer: impl Into<String>,
        topic: impl Into<TopicSelectors>,
    ) -> Self {
        let mut builder = TopicReaderOptionsBuilder::default();
        builder.consumer(consumer.into()).topic(topic.into());
        builder
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
