use std::time::Duration;

use crate::{
    client_query::{
        hooks::{QueryTxCommitStatus, QueryTxHook},
        QueryTxIdentity, Transaction,
    },
    grpc_wrapper::{
        raw_topic_service::{
            client::RawTopicClient,
            common::partition::RawOffsetsRange,
            update_offsets_in_transaction::{
                RawPartitionOffsets, RawTopicOffsets, RawTransactionIdentity,
                RawUpdateOffsetsInTransactionRequest,
            },
        },
        raw_ydb_operation::RawOperationParams,
    },
    TopicReader, TopicReaderBatch, YdbError, YdbResult,
};

use super::runtime::RuntimeHandle;

struct ReaderTxHook {
    runtime: RuntimeHandle,
}

impl QueryTxHook for ReaderTxHook {
    fn after_commit(&mut self, status: QueryTxCommitStatus) {
        match status {
            QueryTxCommitStatus::Committed => {}

            QueryTxCommitStatus::Aborted => {
                let _ = self.runtime.force_reconnection(YdbError::custom(
                    "query transaction was aborted: force new connection",
                ));
            }
        }
    }
}

pub struct TopicReaderTx<'a> {
    inner: &'a mut TopicReader,
    runtime: RuntimeHandle,
    client: RawTopicClient,
    tx_identity: QueryTxIdentity,
}

impl<'a> TopicReaderTx<'a> {
    pub(super) async fn new(reader: &'a mut TopicReader, tx: &mut Transaction) -> YdbResult<Self> {
        let uri = tx.uri().await?.ok_or(YdbError::custom("no node Uri"))?;

        let client = reader
            .manager
            .get_auth_service_to_node(RawTopicClient::new, uri)
            .await?;

        let tx_identity = tx.tx_identity().await?;
        let runtime = reader.runtime_handle();

        tx.register_hook(ReaderTxHook {
            runtime: runtime.clone(),
        });

        Ok(Self {
            inner: reader,
            runtime,
            client,
            tx_identity,
        })
    }

    pub async fn read_batch(&mut self) -> YdbResult<TopicReaderBatch> {
        let batch = self.inner.read_batch().await?;
        if let Err(err) = self.update_offsets_in_transaction(&batch).await {
            let _ = self.runtime.force_reconnection(YdbError::custom(
                "UpdateOffsetsInTransaction failed after reading batch",
            ));
            return Err(err);
        }
        Ok(batch)
    }

    async fn update_offsets_in_transaction(&mut self, batch: &TopicReaderBatch) -> YdbResult<()> {
        let commit_marker = batch.get_commit_marker();

        let request = RawUpdateOffsetsInTransactionRequest {
            operation_params: RawOperationParams::new_with_timeouts(
                Duration::from_secs(30),
                Duration::from_secs(60),
            ),
            tx: RawTransactionIdentity {
                id: self.tx_identity.transaction_id.clone(),
                session: self.tx_identity.session_id.clone(),
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
            consumer: self.inner.consumer().to_string(),
        };

        self.client.update_offsets_in_transaction(request).await?;

        Ok(())
    }
}
