use crate::client::TimeoutSettings;
use crate::client_topic::list_types::{Consumer, MeteringMode, SupportedCodecs};
use crate::client_topic::topicwriter::writer::TopicWriter;
use crate::client_topic::topicwriter::writer_options::{TopicWriterOptions, TopicWriterOptionsBuilder};
use crate::errors;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_topic_service::create_topic::RawCreateTopicRequest;
use crate::grpc_wrapper::raw_topic_service::delete_topic::RawDropTopicRequest;
use crate::YdbError::InternalError;
use crate::{grpc_wrapper, YdbResult};
use derive_builder::{Builder, UninitializedFieldError};
use std::collections::HashMap;
use ydb_grpc::ydb_proto::topic::stream_write_message;

#[derive(Builder)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct TopicOptions {
    // Use TopicOptionsBuilder
    #[builder(setter(strip_option), default)]
    pub metering_mode: Option<MeteringMode>,
    #[builder(default)]
    pub min_active_partitions: i64,
    #[builder(default)]
    pub partition_count_limit: i64,
    #[builder(setter(strip_option), default)]
    pub retention_period: Option<core::time::Duration>,
    #[builder(default)]
    pub retention_storage_mb: i64,
    #[builder(default)]
    pub supported_codecs: SupportedCodecs,
    #[builder(default)]
    pub partition_write_speed_bytes_per_second: i64,
    #[builder(default)]
    pub partition_write_burst_bytes: i64,
    #[builder(default)]
    pub attributes: HashMap<String, String>,
    #[builder(default)]
    pub consumers: Vec<Consumer>,
}

impl From<UninitializedFieldError> for errors::YdbError {
    fn from(ufe: UninitializedFieldError) -> Self {
        InternalError(format!("Error during building topic options: {}", ufe))
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
        topic_options: TopicOptions,
    ) -> YdbResult<()> {
        let req = RawCreateTopicRequest::new(path, self.timeouts.operation_params(), topic_options);

        let mut service = self.connection().await?;
        service.create_topic(req).await?;

        Ok(())
    }

    pub async fn drop_topic(&mut self, path: String) -> YdbResult<()> {
        let req = RawDropTopicRequest {
            operation_params: self.timeouts.operation_params(),
            path,
        };

        let mut service = self.connection().await?;
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
        TopicWriter::new(TopicWriterOptionsBuilder::default().topic_path(path).build().unwrap(), self.connection_manager.clone()).await
    }

    async fn connection(
        &self,
    ) -> YdbResult<grpc_wrapper::raw_topic_service::client::RawTopicClient> {
        self.connection_manager
            .get_auth_service(grpc_wrapper::raw_topic_service::client::RawTopicClient::new)
            .await
    }
}
