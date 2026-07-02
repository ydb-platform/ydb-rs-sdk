use async_trait::async_trait;
use tokio::sync::Mutex;

use slo_framework::topic::{Params, TopicService};
use slo_framework::{Framework, TopicWorkload, Workload};
use ydb::{
    ClientBuilder, ConsumerBuilder, CreateTopicOptionsBuilder, PartitioningStrategy, TopicClient,
    TopicReader, TopicReaderOptions, TopicSelector, TopicWriter, TopicWriterOptionsBuilder,
};

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
impl TopicService for TopicStorage {
    async fn create_topic(&self) -> Result<(), String> {
        let consumer = ConsumerBuilder::default()
            .name(self.params.consumer_name.clone())
            .important(true)
            .build()
            .map_err(|err| err.to_string())?;

        let options = CreateTopicOptionsBuilder::default()
            .min_active_partitions(self.params.partition_count as i64)
            .consumers(vec![consumer])
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
        let mut readers: Vec<TopicReader> = Vec::with_capacity(self.params.reader_count as usize);
        let mut tc = self.topic_client.lock().await;

        for _ in 0..self.params.reader_count {
            let selector = TopicSelector::new(self.params.topic_path.clone());

            let options = TopicReaderOptions::builder()
                .consumer(self.params.consumer_name.clone())
                .topic(selector)
                .build();

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
