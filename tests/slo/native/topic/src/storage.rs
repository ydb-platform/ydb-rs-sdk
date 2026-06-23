use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use slo_framework::queue::{Params, Queue};
use slo_framework::{
    BucketID, CommitMarker, Framework, MessageReader, MessageWriter, PartitionID, QueueWorkload,
    TestMessage, TopicBatch, Workload, WriterHandle,
};
use ydb::{
    ClientBuilder, Consumer, ConsumerBuilder, CreateTopicOptionsBuilder, PartitioningStrategy,
    TopicClient, TopicReader, TopicReaderCommitMarker, TopicReaderOptionsBuilder, TopicSelector,
    TopicWriter, TopicWriterMessageBuilder, TopicWriterOptionsBuilder,
};

fn consumer_name(prefix: &str, index: u32) -> String {
    format!("{prefix}-{index}")
}

fn producer_id(prefix: &str, partition_id: PartitionID, writer_idx: u32) -> String {
    format!("{prefix}-{partition_id}-{writer_idx}")
}

pub struct TopicStorage {
    topic_client: Mutex<TopicClient>,
    params: Params,
}

impl TopicStorage {
    pub async fn new(fw: &Framework, params: Params) -> Result<Self, String> {
        let client = ClientBuilder::new_from_connection_string(&fw.config.connection_string)
            .map_err(|err| err.to_string())?
            .client()
            .map_err(|err| err.to_string())?;

        client.wait().await.map_err(|err| err.to_string())?;

        Ok(Self {
            topic_client: Mutex::new(client.topic_client()),
            params,
        })
    }
}

#[async_trait]
impl Queue for TopicStorage {
    async fn create_topic(&self) -> Result<(), String> {
        let consumers: Vec<Consumer> = (0..self.params.consumer_count)
            .map(|i| {
                ConsumerBuilder::default()
                    .name(consumer_name(&self.params.consumer_prefix, i))
                    .important(true)
                    .build()
                    .map_err(|err| err.to_string())
            })
            .collect::<Result<_, _>>()?;

        let options = CreateTopicOptionsBuilder::default()
            .min_active_partitions(self.params.partition_count as i64)
            .consumers(consumers)
            .build()
            .map_err(|err| err.to_string())?;

        let mut tc = self.topic_client.lock().await;
        tc.create_topic(self.params.topic_path.clone(), options)
            .await
            .map_err(|err| err.to_string())
    }

    async fn drop_topic(&self) -> Result<(), String> {
        let mut tc = self.topic_client.lock().await;
        tc.drop_topic(self.params.topic_path.clone())
            .await
            .map_err(|err| err.to_string())
    }

    async fn open_writers(&self) -> Result<Vec<WriterHandle>, String> {
        let total = (self.params.partition_count * self.params.writers_per_partition) as usize;
        let mut writers: Vec<WriterHandle> = Vec::with_capacity(total);
        let mut tc = self.topic_client.lock().await;

        for partition_id in 0..self.params.partition_count as i64 {
            // Every consumer-scoped bucket that will observe this partition's
            // messages — workload mirrors each payload to all of them.
            let buckets: Vec<BucketID> = (0..self.params.consumer_count)
                .map(|c_idx| BucketID::for_consumer_partition(c_idx, partition_id))
                .collect();

            for writer_idx in 0..self.params.writers_per_partition {
                let id = producer_id(&self.params.producer_id_prefix, partition_id, writer_idx);

                let options = TopicWriterOptionsBuilder::default()
                    .topic_path(self.params.topic_path.clone())
                    .producer_id(id)
                    .partitioning(PartitioningStrategy::PartitionId(partition_id))
                    .build()
                    .map_err(|err| err.to_string())?;

                let writer = tc
                    .create_writer_with_params(options)
                    .await
                    .map_err(|err| err.to_string())?;

                writers.push(WriterHandle {
                    writer: Box::new(YdbTopicWriter {
                        inner: writer,
                        partition_id,
                    }),
                    buckets: buckets.clone(),
                });
            }
        }

        Ok(writers)
    }

    async fn open_readers(&self) -> Result<Vec<Box<dyn MessageReader>>, String> {
        let total = (self.params.partition_count * self.params.consumer_count) as usize;
        let mut readers: Vec<Box<dyn MessageReader>> = Vec::with_capacity(total);
        let mut tc = self.topic_client.lock().await;

        for consumer_idx in 0..self.params.consumer_count {
            let consumer = consumer_name(&self.params.consumer_prefix, consumer_idx);

            for partition_id in 0..self.params.partition_count as i64 {
                let mut selector = TopicSelector::new(self.params.topic_path.clone());
                selector.partition_ids = Some(vec![partition_id]);
                selector.read_from = None;

                let options =
                    TopicReaderOptionsBuilder::from_consumer_topic(consumer.clone(), selector)
                        .build()
                        .map_err(|err| err.to_string())?;

                let reader = tc
                    .create_reader_with_params(options)
                    .await
                    .map_err(|err| err.to_string())?;

                readers.push(Box::new(YdbTopicReader {
                    inner: reader,
                    partition_id,
                    bucket_id: BucketID::for_consumer_partition(consumer_idx, partition_id),
                }));
            }
        }

        Ok(readers)
    }

    async fn close(&self) -> Result<(), String> {
        Ok(())
    }
}

struct YdbTopicWriter {
    inner: TopicWriter,
    partition_id: PartitionID,
}

#[async_trait]
impl MessageWriter for YdbTopicWriter {
    async fn write(&mut self, data: Vec<u8>) -> Result<(), String> {
        let message = TopicWriterMessageBuilder::default()
            .data(data)
            .build()
            .map_err(|err| err.to_string())?;

        self.inner
            .write_with_ack(message)
            .await
            .map(|_| ())
            .map_err(|err| format!("partition {}: {err}", self.partition_id))
    }
}

struct YdbTopicReader {
    inner: TopicReader,
    partition_id: PartitionID,
    bucket_id: BucketID,
}

#[async_trait]
impl MessageReader for YdbTopicReader {
    async fn read_batch(&mut self) -> Result<TopicBatch, String> {
        let batch = self
            .inner
            .read_batch()
            .await
            .map_err(|err| format!("partition {}: {err}", self.partition_id))?;

        let marker = CommitMarker::new(batch.get_commit_marker());

        let mut messages = Vec::with_capacity(batch.messages.len());

        for mut raw in batch.messages {
            let partition_id = self.partition_id;
            let data = raw
                .read_and_take()
                .await
                .map_err(|err| format!("partition {partition_id}: {err}"))?
                .ok_or_else(|| {
                    format!("partition {partition_id}: message payload already taken")
                })?;

            messages.push(TestMessage {
                partition_id: raw.get_partition_id(),
                bucket_id: self.bucket_id.clone(),
                offset: raw.offset,
                seq_no: raw.seq_no,
                data,
                commit_marker: CommitMarker::new(raw.get_commit_marker()),
            });
        }

        Ok(TopicBatch { messages, marker })
    }

    async fn commit(&mut self, marker: CommitMarker) -> Result<(), String> {
        let marker_arc: Arc<TopicReaderCommitMarker> = marker
            .0
            .downcast::<TopicReaderCommitMarker>()
            .map_err(|_| "commit marker type mismatch".to_string())?;

        // commit_with_ack consumes the marker; clone out of the Arc.
        let marker = (*marker_arc).clone();

        self.inner
            .commit_with_ack(marker)
            .await
            .map_err(|err| format!("partition {}: {err}", self.partition_id))
    }
}

pub async fn new_workload(fw: Framework) -> Result<Box<dyn Workload>, String> {
    let params = slo_framework::queue::parse_params(&fw);
    let storage = TopicStorage::new(&fw, params.clone()).await?;

    Ok(Box::new(QueueWorkload::new(fw, params, storage)))
}
