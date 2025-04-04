use super::list_types::{Codec, TopicDescription};
use crate::client::TimeoutSettings;
use crate::client_topic::list_types::{AlterConsumer, Consumer, MeteringMode};
use crate::client_topic::topicwriter::writer::TopicWriter;
use crate::client_topic::topicwriter::writer_options::{
    TopicWriterOptions, TopicWriterOptionsBuilder,
};
use crate::errors;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_topic_service::alter_topic::RawAlterTopicRequest;
use crate::grpc_wrapper::raw_topic_service::create_topic::RawCreateTopicRequest;
use crate::grpc_wrapper::raw_topic_service::describe_topic::RawDescribeTopicRequest;
use crate::grpc_wrapper::raw_topic_service::drop_topic::RawDropTopicRequest;
use crate::YdbError::InternalError;
use crate::{grpc_wrapper, YdbResult};
use derive_builder::{Builder, UninitializedFieldError};
use std::collections::HashMap;
use std::time::Duration;

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

impl From<UninitializedFieldError> for errors::YdbError {
    fn from(ufe: UninitializedFieldError) -> Self {
        InternalError(format!("Error during build type: {}", ufe))
    }
}

pub struct TopicClient {
    timeouts: TimeoutSettings,
    connection_manager: GrpcConnectionManager,
}

impl TopicClient {
    pub(crate) fn new(
        timeouts: TimeoutSettings,
        connection_manager: GrpcConnectionManager,
    ) -> Self {
        Self {
            timeouts,
            connection_manager,
        }
    }

    pub async fn create_topic(
        &mut self,
        path: String,
        options: CreateTopicOptions,
    ) -> YdbResult<()> {
        let req = RawCreateTopicRequest::new(path, self.timeouts.operation_params(), options);

        let mut service = self.raw_client_connection().await?;
        service.create_topic(req).await?;

        Ok(())
    }

    pub async fn alter_topic(&mut self, path: String, options: AlterTopicOptions) -> YdbResult<()> {
        let req = RawAlterTopicRequest::new(path, self.timeouts.operation_params(), options);

        let mut service = self.raw_client_connection().await?;
        service.alter_topic(req).await?;

        Ok(())
    }

    pub async fn describe_topic(
        &mut self,
        path: String,
        options: DescribeTopicOptions,
    ) -> YdbResult<TopicDescription> {
        let req = RawDescribeTopicRequest::new(path, self.timeouts.operation_params(), options);

        let mut service = self.raw_client_connection().await?;
        let result = service.describe_topic(req).await?;
        let description = TopicDescription::from(result);

        Ok(description)
    }

    pub async fn drop_topic(&mut self, path: String) -> YdbResult<()> {
        let req = RawDropTopicRequest {
            operation_params: self.timeouts.operation_params(),
            path,
        };

        let mut service = self.raw_client_connection().await?;
        service.delete_topic(req).await?;

        Ok(())
    }

    pub async fn create_writer_with_params(
        &mut self,
        writer_options: TopicWriterOptions,
    ) -> YdbResult<TopicWriter> {
        TopicWriter::new(writer_options, self.connection_manager.clone()).await
    }

    pub async fn create_writer(&mut self, path: String) -> YdbResult<TopicWriter> {
        TopicWriter::new(
            TopicWriterOptionsBuilder::default()
                .topic_path(path)
                .build()
                .unwrap(),
            self.connection_manager.clone(),
        )
        .await
    }

    pub(crate) async fn raw_client_connection(
        &self,
    ) -> YdbResult<grpc_wrapper::raw_topic_service::client::RawTopicClient> {
        self.connection_manager
            .get_auth_service(grpc_wrapper::raw_topic_service::client::RawTopicClient::new)
            .await
    }
}
