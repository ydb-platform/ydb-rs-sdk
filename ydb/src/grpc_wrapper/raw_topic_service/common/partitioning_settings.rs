use ydb_grpc::ydb_proto::topic::{AlterPartitioningSettings, PartitioningSettings};

#[derive(Debug, serde::Serialize)]
pub(crate) struct RawPartitioningSettings {
    pub min_active_partitions: i64,
    pub max_active_partitions: i64,
    pub auto_partitioning_settings: Option<ydb_grpc::ydb_proto::topic::AutoPartitioningSettings>,
}

impl From<PartitioningSettings> for RawPartitioningSettings {
    fn from(value: PartitioningSettings) -> Self {
        Self {
            min_active_partitions: value.min_active_partitions,
            max_active_partitions: value.max_active_partitions,
            auto_partitioning_settings: value.auto_partitioning_settings,
        }
    }
}

impl From<RawPartitioningSettings> for PartitioningSettings {
    fn from(value: RawPartitioningSettings) -> Self {
        Self {
            min_active_partitions: value.min_active_partitions,
            max_active_partitions: value.max_active_partitions,
            auto_partitioning_settings: value.auto_partitioning_settings,
            #[allow(deprecated)]
            partition_count_limit: 0, // deprecated
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct RawAlterPartitioningSettings {
    pub set_min_active_partitions: Option<i64>,
    pub set_max_active_partitions: Option<i64>,
    pub alter_auto_partitioning_settings:
        Option<ydb_grpc::ydb_proto::topic::AlterAutoPartitioningSettings>,
}

impl From<AlterPartitioningSettings> for RawAlterPartitioningSettings {
    fn from(value: AlterPartitioningSettings) -> Self {
        Self {
            set_min_active_partitions: value.set_min_active_partitions,
            set_max_active_partitions: value.set_max_active_partitions,
            alter_auto_partitioning_settings: value.alter_auto_partitioning_settings,
        }
    }
}

impl From<RawAlterPartitioningSettings> for AlterPartitioningSettings {
    fn from(value: RawAlterPartitioningSettings) -> Self {
        Self {
            set_min_active_partitions: value.set_min_active_partitions,
            set_max_active_partitions: value.set_max_active_partitions,
            alter_auto_partitioning_settings: value.alter_auto_partitioning_settings,
            #[allow(deprecated)]
            set_partition_count_limit: None, // deprecated
        }
    }
}
