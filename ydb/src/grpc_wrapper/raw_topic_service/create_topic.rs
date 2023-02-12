use std::collections::HashMap;
use itertools::Itertools;

use ydb_grpc::ydb_proto::operations::OperationParams;
use ydb_grpc::ydb_proto::topic::{
    Consumer, CreateTopicRequest, CreateTopicResult, MeteringMode, PartitioningSettings,
    SupportedCodecs,
};

use crate::grpc_wrapper::raw_common_types::Duration;
use crate::grpc_wrapper::raw_topic_service::common::codecs::RawSupportedCodecs;
use crate::grpc_wrapper::raw_topic_service::common::consumer::RawConsumer;
use crate::grpc_wrapper::raw_topic_service::common::metering_mode::RawMeteringMode;
use crate::grpc_wrapper::raw_topic_service::common::partitioning_settings::RawPartitioningSettings;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use crate::grpc_wrapper::raw_errors::RawError;

#[derive(serde::Serialize)]
pub(crate) struct RawCreateTopicRequest {
    pub operation_params: RawOperationParams,
    pub path: String,
    pub partitioning_settings: RawPartitioningSettings,
    pub retention_period: Option<Duration>,
    pub retention_storage_mb: i64,
    pub supported_codecs: RawSupportedCodecs,
    pub partition_write_speed_bytes_per_second: i64,
    pub partition_write_burst_bytes: i64,
    pub attributes: HashMap<String, String>,
    pub consumers: Vec<RawConsumer>,
    pub metering_mode: RawMeteringMode,
}

impl From<RawCreateTopicRequest> for CreateTopicRequest {
    fn from(value: RawCreateTopicRequest) -> Self {
        Self {
            operation_params: Some(OperationParams::from(value.operation_params)),
            path: value.path,
            partitioning_settings: Some(PartitioningSettings::from(value.partitioning_settings)),
            retention_period: value
                .retention_period
                .map(ydb_grpc::google_proto_workaround::protobuf::Duration::from),
            retention_storage_mb: value.retention_storage_mb,
            supported_codecs: Some(SupportedCodecs::from(value.supported_codecs)),
            partition_write_speed_bytes_per_second: value.partition_write_speed_bytes_per_second,
            partition_write_burst_bytes: value.partition_write_burst_bytes,
            attributes: value.attributes,
            consumers: value.consumers.into_iter().map(Consumer::from).collect_vec(),
            metering_mode: MeteringMode::from(value.metering_mode) as i32,
        }
    }
}

pub(crate) struct RawCreateTopicResult {
}

impl TryFrom<CreateTopicResult> for RawCreateTopicResult {
    type Error = RawError;

    fn try_from(_value: CreateTopicResult) -> Result<Self, Self::Error> {
        Ok(Self {})
    }
}
