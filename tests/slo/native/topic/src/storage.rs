use async_trait::async_trait;
use tokio::sync::Mutex;

use slo_framework::topic::{Params, Topic};
use slo_framework::{Framework, TopicWorkload, Workload};
use ydb::{
    ClientBuilder, Consumer, ConsumerBuilder, CreateTopicOptionsBuilder, PartitioningStrategy,
    TopicClient, TopicReader, TopicReaderOptionsBuilder, TopicSelector, TopicWriter,
    TopicWriterOptionsBuilder,
};

fn consumer_name(prefix: &str, index: u32) -> String {
    format!("{prefix}-{index}")
}

fn producer_id(prefix: &str, idx: u32) -> String {
    format!("{prefix}-{idx}")
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
impl Topic for TopicStorage {
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

    async fn open_writers(&self) -> Result<Vec<TopicWriter>, String> {
        let mut writers: Vec<TopicWriter> = Vec::with_capacity(self.params.writer_count as usize);
        let mut tc = self.topic_client.lock().await;

        for i in 0..self.params.writer_count {
            let id = producer_id(&self.params.producer_id_prefix, i);

            let options = TopicWriterOptionsBuilder::default()
                .topic_path(self.params.topic_path.clone())
                .producer_id(id)
                .partitioning(PartitioningStrategy::ByProducerId)
                .build()
                .map_err(|err| err.to_string())?;

            let writer = tc
                .create_writer_with_params(options)
                .await
                .map_err(|err| err.to_string())?;

            writers.push(writer);
        }

        Ok(writers)
    }

    async fn open_readers(&self) -> Result<Vec<TopicReader>, String> {
        let total = self.params.consumer_count as usize;
        let mut readers: Vec<TopicReader> = Vec::with_capacity(total);
        let mut tc = self.topic_client.lock().await;

        for consumer_idx in 0..self.params.consumer_count {
            let consumer = consumer_name(&self.params.consumer_prefix, consumer_idx);
            let selector = TopicSelector::new(self.params.topic_path.clone());

            let options = TopicReaderOptionsBuilder::from_consumer_topic(consumer, selector)
                .build()
                .map_err(|err| err.to_string())?;

            let reader = tc
                .create_reader_with_params(options)
                .await
                .map_err(|err| err.to_string())?;

            readers.push(reader);
        }

        Ok(readers)
    }

    async fn close(&self) -> Result<(), String> {
        Ok(())
    }
}

pub async fn new_workload(fw: Framework) -> Result<Box<dyn Workload>, String> {
    let params = slo_framework::topic::parse_params(&fw);
    let storage = TopicStorage::new(&fw, params.clone()).await?;

    Ok(Box::new(TopicWorkload::new(fw, params, storage)))
}
