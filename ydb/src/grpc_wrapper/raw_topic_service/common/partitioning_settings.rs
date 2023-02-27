#[derive(serde::Serialize)]
pub(crate) struct RawPartitioningSettings {
    pub min_active_partitions: i64,
    pub partition_count_limit: i64,
}

impl From<RawPartitioningSettings> for ydb_grpc::ydb_proto::topic::PartitioningSettings {
    fn from(value: RawPartitioningSettings) -> Self {
        Self {
            min_active_partitions: value.min_active_partitions,
            partition_count_limit: value.partition_count_limit,
        }
    }
}
