use ydb_grpc::ydb_proto::topic::{AlterPartitioningSettings, PartitioningSettings};

#[derive(Debug, serde::Serialize)]
pub(crate) struct RawPartitioningSettings {
    pub min_active_partitions: i64,
    pub partition_count_limit: i64,
}

impl From<PartitioningSettings> for RawPartitioningSettings {
    fn from(value: PartitioningSettings) -> Self {
        Self {
            min_active_partitions: value.min_active_partitions,
            partition_count_limit: value.partition_count_limit,
        }
    }
}

impl From<RawPartitioningSettings> for PartitioningSettings {
    fn from(value: RawPartitioningSettings) -> Self {
        Self {
            min_active_partitions: value.min_active_partitions,
            partition_count_limit: value.partition_count_limit,
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct RawAlterPartitioningSettings {
    pub set_min_active_partitions: Option<i64>,
    pub set_partition_count_limit: Option<i64>,
}

impl From<AlterPartitioningSettings> for RawAlterPartitioningSettings {
    fn from(value: AlterPartitioningSettings) -> Self {
        Self {
            set_min_active_partitions: value.set_min_active_partitions,
            set_partition_count_limit: value.set_partition_count_limit,
        }
    }
}

impl From<RawAlterPartitioningSettings> for AlterPartitioningSettings {
    fn from(value: RawAlterPartitioningSettings) -> Self {
        Self {
            set_min_active_partitions: value.set_min_active_partitions,
            set_partition_count_limit: value.set_partition_count_limit,
        }
    }
}
