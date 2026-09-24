use super::compression::Executor;
use super::list_types::{Codec, TopicDescription};
use super::topicwriter::writer_tx_options::{TopicWriterTxOptions, TopicWriterTxOptionsBuilder};
use crate::YdbError::InternalError;
use crate::client::TimeoutSettings;
use crate::client_common::TokenCache;
use crate::client_query::Transaction;
use crate::client_topic::list_types::{AlterConsumer, Consumer, MeteringMode};
use crate::client_topic::topicreader::reader::{TopicReader, TopicSelectors};
use crate::client_topic::topicreader::reader_options::TopicReaderOptions;
use crate::client_topic::topicwriter::writer::TopicWriter;
use crate::client_topic::topicwriter::writer_options::TopicWriterOptions;
use crate::client_topic::topicwriter::writer_tx::TopicWriterTx;
use crate::driver_lifecycle::{DriverGuarded, DriverLifecycle};
use crate::errors;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_topic_service::alter_topic::RawAlterTopicRequest;
use crate::grpc_wrapper::raw_topic_service::create_topic::RawCreateTopicRequest;
use crate::grpc_wrapper::raw_topic_service::describe_consumer::RawDescribeConsumerRequest;
use crate::grpc_wrapper::raw_topic_service::describe_topic::RawDescribeTopicRequest;
use crate::grpc_wrapper::raw_topic_service::drop_topic::RawDropTopicRequest;
use crate::{YdbResult, grpc_wrapper};
use derive_builder::{Builder, UninitializedFieldError};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::instrument;

#[derive(Builder)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct CreateTopicOptions {
    // Use CreateTopicOptionsBuilder
    #[builder(default)]
    pub min_active_partitions: i64,
    #[builder(default)]
    pub partition_count_limit: i64,
    #[builder(setter(strip_option), default)]
    pub retention_period: Option<Duration>,
    #[builder(default)]
    pub retention_storage_mb: i64,
    #[builder(default)]
    pub supported_codecs: Vec<Codec>,
    #[builder(default)]
    pub partition_write_speed_bytes_per_second: i64,
    #[builder(default)]
    pub partition_write_burst_bytes: i64,
    #[builder(default)]
    pub consumers: Vec<Consumer>,
    #[builder(default)]
    pub attributes: HashMap<String, String>,
    #[builder(setter(strip_option), default)]
    pub metering_mode: Option<MeteringMode>,
}

#[derive(Builder)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct AlterTopicOptions {
    // Use AlterTopicOptionsBuilder
    #[builder(setter(strip_option), default)]
    pub set_min_active_partitions: Option<i64>,

    #[builder(setter(strip_option), default)]
    pub set_partition_count_limit: Option<i64>,

    #[builder(setter(strip_option), default)]
    pub set_retention_period: Option<Duration>,

    #[builder(setter(strip_option), default)]
    pub set_retention_storage_mb: Option<i64>,

    #[builder(setter(strip_option), default)]
    pub set_supported_codecs: Option<Vec<Codec>>,

    #[builder(setter(strip_option), default)]
    pub set_partition_write_speed_bytes_per_second: Option<i64>,

    #[builder(setter(strip_option), default)]
    pub set_partition_write_burst_bytes: Option<i64>,

    #[builder(default)]
    pub alter_attributes: HashMap<String, String>,

    #[builder(default)]
    pub add_consumers: Vec<Consumer>,

    #[builder(default)]
    pub drop_consumers: Vec<String>,

    #[builder(default)]
    pub alter_consumers: Vec<AlterConsumer>,

    #[builder(setter(strip_option), default)]
    pub set_metering_mode: Option<MeteringMode>,
}

#[derive(Builder)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct DescribeTopicOptions {
    // Use DescribeTopicOptionsBuilder
    #[builder(default)]
    pub include_stats: bool,
    #[builder(default)]
    pub include_location: bool,
}

#[derive(Builder)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct DescribeConsumerOptions {
    // Use DescribeConsumerOptionsBuilder
    #[builder(default)]
    pub include_stats: bool,
    #[builder(default)]
    pub include_location: bool,
}

impl From<UninitializedFieldError> for errors::YdbError {
    fn from(ufe: UninitializedFieldError) -> Self {
        InternalError(format!("Error during build type: {ufe}"))
    }
}

#[derive(Clone)]
pub struct TopicClient {
    inner: DriverGuarded<TopicClientInner>,
}

#[derive(Clone)]
struct TopicClientInner {
    timeouts: TimeoutSettings,
    connection_manager: GrpcConnectionManager,
    token_cache: TokenCache,
    executor: Arc<dyn Executor>,
}

impl TopicClient {
    pub(crate) fn new(
        connection_manager: GrpcConnectionManager,
        token_cache: TokenCache,
        executor: Arc<dyn Executor>,
        lifecycle: &DriverLifecycle,
    ) -> Self {
        Self {
            inner: lifecycle.guard(TopicClientInner {
                timeouts: TimeoutSettings::default(),
                connection_manager,
                token_cache,
                executor,
            }),
        }
    }

    #[instrument(name = "ydb.TopicClient.CreateTopic", skip_all, fields(db.system.name = "ydb", ydb.topic.path = %path))]
    pub async fn create_topic(
        &mut self,
        path: String,
        options: CreateTopicOptions,
    ) -> YdbResult<()> {
        let inner = self.inner.access()?;
        let req = RawCreateTopicRequest::new(path, inner.timeouts.operation_params(), options);

        let mut service = inner.raw_client_connection().await?;
        service.create_topic(req).await?;

        Ok(())
    }

    #[instrument(name = "ydb.TopicClient.AlterTopic", skip_all, fields(db.system.name = "ydb", ydb.topic.path = %path))]
    pub async fn alter_topic(&mut self, path: String, options: AlterTopicOptions) -> YdbResult<()> {
        let inner = self.inner.access()?;
        let req = RawAlterTopicRequest::new(path, inner.timeouts.operation_params(), options);

        let mut service = inner.raw_client_connection().await?;
        service.alter_topic(req).await?;

        Ok(())
    }

    #[instrument(name = "ydb.TopicClient.DescribeConsumer", skip_all, fields(db.system.name = "ydb", ydb.topic.path = %path, ydb.consumer.name = %consumer))]
    pub async fn describe_consumer(
        &mut self,
        path: String,
        consumer: String,
        options: DescribeConsumerOptions,
    ) -> YdbResult<super::list_types::ConsumerDescription> {
        let inner = self.inner.access()?;
        let req = RawDescribeConsumerRequest::new(
            path,
            consumer,
            inner.timeouts.operation_params(),
            options,
        );

        let mut service = inner.raw_client_connection().await?;
        let result = service.describe_consumer(req).await?;
        let description = super::list_types::ConsumerDescription::from(result);

        Ok(description)
    }

    #[instrument(name = "ydb.TopicClient.DescribeTopic", skip_all, fields(db.system.name = "ydb", ydb.topic.path = %path))]
    pub async fn describe_topic(
        &mut self,
        path: String,
        options: DescribeTopicOptions,
    ) -> YdbResult<TopicDescription> {
        let inner = self.inner.access()?;
        let req = RawDescribeTopicRequest::new(path, inner.timeouts.operation_params(), options);

        let mut service = inner.raw_client_connection().await?;
        let result = service.describe_topic(req).await?;
        let description = TopicDescription::from(result);

        Ok(description)
    }

    #[instrument(name = "ydb.TopicClient.DropTopic", skip_all, fields(db.system.name = "ydb", ydb.topic.path = %path))]
    pub async fn drop_topic(&mut self, path: String) -> YdbResult<()> {
        let inner = self.inner.access()?;
        let req = RawDropTopicRequest {
            operation_params: inner.timeouts.operation_params(),
            path,
        };

        let mut service = inner.raw_client_connection().await?;
        service.delete_topic(req).await?;

        Ok(())
    }

    #[instrument(name = "ydb.TopicClient.CreateReader", skip_all)]
    pub async fn create_reader(
        &mut self,
        consumer: impl Into<String>,
        topic: impl Into<TopicSelectors>,
    ) -> YdbResult<TopicReader> {
        let resource = self.inner.register_topic_reader()?;
        let inner = self.inner.access()?;
        let options = TopicReaderOptions::builder()
            .consumer(consumer)
            .topic(topic)
            .build();
        TopicReader::new(
            options,
            inner.connection_manager.clone(),
            inner.token_cache.clone(),
            inner.executor.clone(),
            resource,
        )
        .await
    }

    #[instrument(name = "ydb.TopicClient.CreateReader", skip_all)]
    pub async fn create_reader_with_params(
        &mut self,
        options: TopicReaderOptions,
    ) -> YdbResult<TopicReader> {
        let resource = self.inner.register_topic_reader()?;
        let inner = self.inner.access()?;
        TopicReader::new(
            options,
            inner.connection_manager.clone(),
            inner.token_cache.clone(),
            inner.executor.clone(),
            resource,
        )
        .await
    }

    #[instrument(name = "ydb.TopicClient.CreateWriter", skip_all)]
    pub async fn create_writer_with_params(
        &mut self,
        writer_options: TopicWriterOptions,
    ) -> YdbResult<TopicWriter> {
        let resource = self.inner.register_topic_writer()?;
        let inner = self.inner.access()?;
        TopicWriter::new(
            writer_options,
            inner.connection_manager.clone(),
            inner.executor.clone(),
            resource,
        )
        .await
    }

    pub async fn create_writer_tx(
        &mut self,
        topic_path: impl Into<String>,
        tx: &mut Transaction,
    ) -> YdbResult<TopicWriterTx> {
        let options = TopicWriterTxOptionsBuilder::default()
            .topic_path(topic_path.into())
            .build()?;
        self.create_writer_tx_with_params(options, tx).await
    }

    pub async fn create_writer_tx_with_params(
        &mut self,
        writer_tx_options: TopicWriterTxOptions,
        tx: &mut Transaction,
    ) -> YdbResult<TopicWriterTx> {
        let resource = self.inner.register_topic_writer()?;
        let inner = self.inner.access()?;
        TopicWriterTx::new(
            writer_tx_options,
            inner.connection_manager.clone(),
            inner.executor.clone(),
            tx,
            resource,
        )
        .await
    }

    #[instrument(name = "ydb.TopicClient.CreateWriter", skip_all)]
    pub async fn create_writer(&mut self, path: impl Into<String>) -> YdbResult<TopicWriter> {
        let resource = self.inner.register_topic_writer()?;
        let inner = self.inner.access()?;
        TopicWriter::new(
            TopicWriterOptions::builder().topic_path(path).build(),
            inner.connection_manager.clone(),
            inner.executor.clone(),
            resource,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn raw_client_connection(
        &self,
    ) -> YdbResult<grpc_wrapper::raw_topic_service::client::RawTopicClient> {
        self.inner.access()?.raw_client_connection().await
    }
}

impl TopicClientInner {
    async fn raw_client_connection(
        &self,
    ) -> YdbResult<grpc_wrapper::raw_topic_service::client::RawTopicClient> {
        self.connection_manager
            .get_auth_service(grpc_wrapper::raw_topic_service::client::RawTopicClient::new)
            .await
    }
}
