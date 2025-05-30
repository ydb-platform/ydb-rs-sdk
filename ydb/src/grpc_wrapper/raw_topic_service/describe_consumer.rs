use super::common::{consumer::RawConsumer, partition::RawPartitionInfo};
use crate::{
    client_topic::client::DescribeConsumerOptions,
    grpc_wrapper::{
        raw_errors::{RawError, RawResult},
        raw_scheme_client::list_directory_types::from_grpc_to_scheme_entry,
        raw_ydb_operation::RawOperationParams,
    },
};
use ydb_grpc::ydb_proto::topic::{DescribeConsumerRequest, DescribeConsumerResult};

#[derive(Debug)]
pub(crate) struct RawDescribeConsumerRequest {
    pub path: String,
    pub consumer: String,
    pub operation_params: RawOperationParams,
    pub include_stats: bool,
    pub include_location: bool,
}

impl RawDescribeConsumerRequest {
    pub(crate) fn new(
        path: String,
        consumer: String,
        operation_params: RawOperationParams,
        options: DescribeConsumerOptions,
    ) -> Self {
        Self {
            operation_params,
            path,
            consumer,
            include_stats: options.include_stats,
            include_location: options.include_location,
        }
    }
}

impl From<RawDescribeConsumerRequest> for DescribeConsumerRequest {
    fn from(value: RawDescribeConsumerRequest) -> Self {
        Self {
            path: value.path,
            consumer: value.consumer,
            operation_params: Some(value.operation_params.into()),
            include_stats: value.include_stats,
            include_location: value.include_location,
        }
    }
}

#[derive(Debug)]
pub(crate) struct RawDescribeConsumerResult {
    pub self_: crate::SchemeEntry,
    pub consumer: RawConsumer,
    pub partitions: Vec<RawPartitionInfo>,
}

impl TryFrom<DescribeConsumerResult> for RawDescribeConsumerResult {
    type Error = RawError;

    fn try_from(value: DescribeConsumerResult) -> RawResult<Self> {
        let entry = value.self_.ok_or(RawError::ProtobufDecodeError(
            "self scheme is absent in result".to_string(),
        ))?;

        let consumer = value.consumer.ok_or(RawError::ProtobufDecodeError(
            "consumer is absent in result".to_string(),
        ))?;

        let consumer_stats = consumer.consumer_stats.clone().map(|stats| {
            crate::grpc_wrapper::raw_topic_service::common::consumer::RawConsumerStats {
                min_partitions_last_read_time: stats
                    .min_partitions_last_read_time
                    .map(|x| x.into()),
                max_read_time_lag: stats.max_read_time_lag.map(|x| x.into()),
                max_write_time_lag: stats.max_write_time_lag.map(|x| x.into()),
                bytes_read: stats.bytes_read.map(|x| x.into()),
                max_committed_time_lag: None,
            }
        });

        let consumer = crate::grpc_wrapper::raw_topic_service::common::consumer::RawConsumer {
            name: consumer.name,
            important: consumer.important,
            read_from: consumer.read_from.map(|x| x.into()),
            supported_codecs: consumer.supported_codecs.map_or_else(
                crate::grpc_wrapper::raw_topic_service::common::codecs::RawSupportedCodecs::default,
                |x| x.into(),
            ),
            attributes: consumer.attributes,
            consumer_stats,
        };

        let partitions = value
            .partitions
            .into_iter()
            .map(|partition| partition.try_into())
            .collect::<RawResult<Vec<RawPartitionInfo>>>()?;

        Ok(Self {
            self_: from_grpc_to_scheme_entry(entry),
            consumer,
            partitions,
        })
    }
}
