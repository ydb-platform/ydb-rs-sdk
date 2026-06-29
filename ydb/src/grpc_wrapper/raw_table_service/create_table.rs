use std::collections::HashMap;

use crate::create_table_types::{
    AutoPartitioningPolicy, CachingPolicy, ColumnDefault, ColumnFamily, ColumnFamilyCompression,
    ColumnFamilyPolicy, ColumnFamilyPolicyCompression, CompactionPolicy, CreateTableIndex,
    CreateTableOptions, DateTypeColumnTtl, ExecutionPolicy, FeatureFlag, PartitioningPolicy,
    ReadReplicasSettings, ReplicationPolicy, SequenceOptions, StoragePolicy, StorageSettings,
    TableColumn, TablePartitioningSettings, TablePartitions, TableProfile, TtlMode, TtlSettings,
    UnixEpochUnit, ValueSinceUnixEpochTtl,
};
use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_table_service::describe_table::RawIndexType;
use crate::grpc_wrapper::raw_table_service::value::r#type::RawType;
use crate::grpc_wrapper::raw_table_service::value::RawTypedValue;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use crate::table_service_types::{IndexType, StoreType};

pub(crate) struct RawCreateTableRequest {
    pub session_id: String,
    pub path: String,
    pub options: RawCreateTableOptions,
    pub operation_params: RawOperationParams,
}

impl From<RawCreateTableRequest> for ydb_grpc::ydb_proto::table::CreateTableRequest {
    fn from(value: RawCreateTableRequest) -> Self {
        Self {
            session_id: value.session_id,
            path: value.path,
            columns: value
                .options
                .columns
                .into_iter()
                .map(ydb_grpc::ydb_proto::table::ColumnMeta::from)
                .collect(),
            primary_key: value.options.primary_key,
            profile: value
                .options
                .profile
                .map(ydb_grpc::ydb_proto::table::TableProfile::from),
            operation_params: Some(value.operation_params.into()),
            indexes: value
                .options
                .indexes
                .into_iter()
                .map(ydb_grpc::ydb_proto::table::TableIndex::from)
                .collect(),
            ttl_settings: value
                .options
                .ttl_settings
                .map(ydb_grpc::ydb_proto::table::TtlSettings::from),
            storage_settings: value
                .options
                .storage_settings
                .map(ydb_grpc::ydb_proto::table::StorageSettings::from),
            column_families: value
                .options
                .column_families
                .into_iter()
                .map(ydb_grpc::ydb_proto::table::ColumnFamily::from)
                .collect(),
            attributes: value.options.attributes,
            compaction_policy: value.options.compaction_policy.unwrap_or_default(),
            partitioning_settings: value
                .options
                .partitioning_settings
                .map(ydb_grpc::ydb_proto::table::PartitioningSettings::from),
            key_bloom_filter: i32::from(value.options.key_bloom_filter),
            read_replicas_settings: value
                .options
                .read_replicas_settings
                .map(ydb_grpc::ydb_proto::table::ReadReplicasSettings::from),
            tiering: value.options.tiering.unwrap_or_default(),
            temporary: value.options.temporary,
            store_type: i32::from(value.options.store_type),
            partitions: value
                .options
                .partitions
                .map(ydb_grpc::ydb_proto::table::create_table_request::Partitions::from),
        }
    }
}

pub(crate) struct RawCreateTableOptions {
    pub columns: Vec<RawTableColumn>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<RawCreateTableIndex>,
    pub profile: Option<RawTableProfile>,
    pub ttl_settings: Option<RawTtlSettings>,
    pub storage_settings: Option<RawStorageSettings>,
    pub column_families: Vec<RawColumnFamily>,
    pub attributes: HashMap<String, String>,
    pub compaction_policy: Option<String>,
    pub partitioning_settings: Option<RawTablePartitioningSettings>,
    pub partitions: Option<RawTablePartitions>,
    pub key_bloom_filter: RawFeatureFlag,
    pub read_replicas_settings: Option<RawReadReplicasSettings>,
    pub tiering: Option<String>,
    pub temporary: bool,
    pub store_type: RawStoreType,
}

impl TryFrom<CreateTableOptions> for RawCreateTableOptions {
    type Error = RawError;

    fn try_from(options: CreateTableOptions) -> Result<Self, Self::Error> {
        options
            .validate()
            .map_err(|e| RawError::custom(e.to_string()))?;

        Ok(Self {
            columns: options
                .columns
                .into_iter()
                .map(RawTableColumn::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            primary_key: options.primary_key,
            indexes: options
                .indexes
                .into_iter()
                .map(RawCreateTableIndex::from)
                .collect(),
            profile: options.profile.map(RawTableProfile::try_from).transpose()?,
            ttl_settings: options.ttl_settings.map(RawTtlSettings::from),
            storage_settings: options.storage_settings.map(RawStorageSettings::from),
            column_families: options
                .column_families
                .into_iter()
                .map(RawColumnFamily::from)
                .collect(),
            attributes: options.attributes,
            compaction_policy: options.compaction_policy,
            partitioning_settings: options
                .partitioning_settings
                .map(RawTablePartitioningSettings::from),
            partitions: options
                .partitions
                .map(RawTablePartitions::try_from)
                .transpose()?,
            key_bloom_filter: options.key_bloom_filter.into(),
            read_replicas_settings: options
                .read_replicas_settings
                .map(RawReadReplicasSettings::from),
            tiering: options.tiering,
            temporary: options.temporary,
            store_type: options.store_type.into(),
        })
    }
}

pub(crate) struct RawTableColumn {
    pub name: String,
    pub column_type: RawType,
    pub not_null: bool,
    pub family: String,
    pub default_value: Option<RawColumnDefault>,
}

impl TryFrom<TableColumn> for RawTableColumn {
    type Error = RawError;

    fn try_from(column: TableColumn) -> Result<Self, Self::Error> {
        let typed: RawTypedValue = column.type_example.try_into()?;
        let default_value = match column.default_value {
            None => None,
            Some(ColumnDefault::Literal(value)) => {
                Some(RawColumnDefault::Literal(RawTypedValue::try_from(value)?))
            }
            Some(ColumnDefault::Sequence(seq)) => {
                Some(RawColumnDefault::Sequence(RawSequenceOptions::from(seq)))
            }
        };

        Ok(Self {
            name: column.name,
            column_type: typed.r#type,
            not_null: column.not_null,
            family: column.family,
            default_value,
        })
    }
}

impl From<RawTableColumn> for ydb_grpc::ydb_proto::table::ColumnMeta {
    fn from(column: RawTableColumn) -> Self {
        Self {
            name: column.name,
            r#type: Some(column.column_type.into()),
            family: column.family,
            not_null: if column.not_null { Some(true) } else { None },
            default_value: column.default_value.map(Into::into),
        }
    }
}

pub(crate) enum RawColumnDefault {
    Literal(RawTypedValue),
    Sequence(RawSequenceOptions),
}

impl From<RawColumnDefault> for ydb_grpc::ydb_proto::table::column_meta::DefaultValue {
    fn from(value: RawColumnDefault) -> Self {
        match value {
            RawColumnDefault::Literal(typed) => {
                Self::FromLiteral(ydb_grpc::ydb_proto::TypedValue::from(typed))
            }
            RawColumnDefault::Sequence(seq) => {
                Self::FromSequence(ydb_grpc::ydb_proto::table::SequenceDescription::from(seq))
            }
        }
    }
}

pub(crate) struct RawSequenceOptions {
    pub name: Option<String>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub start_value: Option<i64>,
    pub cache: Option<u64>,
    pub increment: Option<i64>,
    pub cycle: Option<bool>,
}

impl From<SequenceOptions> for RawSequenceOptions {
    fn from(value: SequenceOptions) -> Self {
        Self {
            name: value.name,
            min_value: value.min_value,
            max_value: value.max_value,
            start_value: value.start_value,
            cache: value.cache,
            increment: value.increment,
            cycle: value.cycle,
        }
    }
}

impl From<RawSequenceOptions> for ydb_grpc::ydb_proto::table::SequenceDescription {
    fn from(value: RawSequenceOptions) -> Self {
        Self {
            name: value.name,
            min_value: value.min_value,
            max_value: value.max_value,
            start_value: value.start_value,
            cache: value.cache,
            increment: value.increment,
            cycle: value.cycle,
            set_val: None,
        }
    }
}

pub(crate) struct RawCreateTableIndex {
    pub name: String,
    pub index_columns: Vec<String>,
    pub data_columns: Vec<String>,
    pub index_type: RawIndexType,
}

impl From<CreateTableIndex> for RawCreateTableIndex {
    fn from(value: CreateTableIndex) -> Self {
        Self {
            name: value.name,
            index_columns: value.index_columns,
            data_columns: value.data_columns,
            index_type: value.index_type.into(),
        }
    }
}

impl From<RawCreateTableIndex> for ydb_grpc::ydb_proto::table::TableIndex {
    fn from(value: RawCreateTableIndex) -> Self {
        Self {
            name: value.name,
            index_columns: value.index_columns,
            data_columns: value.data_columns,
            r#type: Some(value.index_type.into()),
        }
    }
}

impl From<IndexType> for RawIndexType {
    fn from(value: IndexType) -> Self {
        match value {
            IndexType::Unspecified => RawIndexType::Unspecified,
            IndexType::Global => RawIndexType::Global,
            IndexType::GlobalAsync => RawIndexType::GlobalAsync,
            IndexType::GlobalUnique => RawIndexType::GlobalUnique,
        }
    }
}

impl From<RawIndexType> for ydb_grpc::ydb_proto::table::table_index::Type {
    fn from(value: RawIndexType) -> Self {
        use ydb_grpc::ydb_proto::table::{GlobalAsyncIndex, GlobalIndex, GlobalUniqueIndex};
        match value {
            RawIndexType::Unspecified | RawIndexType::Global => Self::GlobalIndex(GlobalIndex {}),
            RawIndexType::GlobalAsync => Self::GlobalAsyncIndex(GlobalAsyncIndex {}),
            RawIndexType::GlobalUnique => Self::GlobalUniqueIndex(GlobalUniqueIndex {}),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RawFeatureFlag {
    Unspecified,
    Enabled,
    Disabled,
}

impl From<FeatureFlag> for RawFeatureFlag {
    fn from(value: FeatureFlag) -> Self {
        match value {
            FeatureFlag::Unspecified => Self::Unspecified,
            FeatureFlag::Enabled => Self::Enabled,
            FeatureFlag::Disabled => Self::Disabled,
        }
    }
}

impl From<RawFeatureFlag> for i32 {
    fn from(value: RawFeatureFlag) -> Self {
        use ydb_grpc::ydb_proto::feature_flag::Status;
        match value {
            RawFeatureFlag::Unspecified => Status::Unspecified as Self,
            RawFeatureFlag::Enabled => Status::Enabled as Self,
            RawFeatureFlag::Disabled => Status::Disabled as Self,
        }
    }
}

pub(crate) struct RawStoragePool {
    pub media: String,
}

impl From<crate::create_table_types::StoragePool> for RawStoragePool {
    fn from(value: crate::create_table_types::StoragePool) -> Self {
        Self { media: value.media }
    }
}

impl From<RawStoragePool> for ydb_grpc::ydb_proto::table::StoragePool {
    fn from(value: RawStoragePool) -> Self {
        Self { media: value.media }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RawColumnFamilyCompression {
    Unspecified,
    None,
    Lz4,
}

impl From<ColumnFamilyCompression> for RawColumnFamilyCompression {
    fn from(value: ColumnFamilyCompression) -> Self {
        match value {
            ColumnFamilyCompression::Unspecified => Self::Unspecified,
            ColumnFamilyCompression::None => Self::None,
            ColumnFamilyCompression::Lz4 => Self::Lz4,
        }
    }
}

impl From<RawColumnFamilyCompression> for i32 {
    fn from(value: RawColumnFamilyCompression) -> Self {
        use ydb_grpc::ydb_proto::table::column_family::Compression;
        match value {
            RawColumnFamilyCompression::Unspecified => Compression::Unspecified as Self,
            RawColumnFamilyCompression::None => Compression::None as Self,
            RawColumnFamilyCompression::Lz4 => Compression::Lz4 as Self,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RawColumnFamilyPolicyCompression {
    Unspecified,
    Uncompressed,
    Compressed,
}

impl From<ColumnFamilyPolicyCompression> for RawColumnFamilyPolicyCompression {
    fn from(value: ColumnFamilyPolicyCompression) -> Self {
        match value {
            ColumnFamilyPolicyCompression::Unspecified => Self::Unspecified,
            ColumnFamilyPolicyCompression::Uncompressed => Self::Uncompressed,
            ColumnFamilyPolicyCompression::Compressed => Self::Compressed,
        }
    }
}

impl From<RawColumnFamilyPolicyCompression> for i32 {
    fn from(value: RawColumnFamilyPolicyCompression) -> Self {
        use ydb_grpc::ydb_proto::table::column_family_policy::Compression;
        match value {
            RawColumnFamilyPolicyCompression::Unspecified => Compression::Unspecified as Self,
            RawColumnFamilyPolicyCompression::Uncompressed => Compression::Uncompressed as Self,
            RawColumnFamilyPolicyCompression::Compressed => Compression::Compressed as Self,
        }
    }
}

pub(crate) struct RawColumnFamily {
    pub name: String,
    pub data: Option<RawStoragePool>,
    pub compression: RawColumnFamilyCompression,
    pub keep_in_memory: RawFeatureFlag,
}

impl From<ColumnFamily> for RawColumnFamily {
    fn from(value: ColumnFamily) -> Self {
        Self {
            name: value.name,
            data: value.data.map(Into::into),
            compression: value.compression.into(),
            keep_in_memory: value.keep_in_memory.into(),
        }
    }
}

impl From<RawColumnFamily> for ydb_grpc::ydb_proto::table::ColumnFamily {
    fn from(value: RawColumnFamily) -> Self {
        Self {
            name: value.name,
            data: value.data.map(Into::into),
            compression: value.compression.into(),
            keep_in_memory: value.keep_in_memory.into(),
        }
    }
}

pub(crate) struct RawColumnFamilyPolicy {
    pub name: String,
    pub data: Option<RawStoragePool>,
    pub external: Option<RawStoragePool>,
    pub keep_in_memory: RawFeatureFlag,
    pub compression: RawColumnFamilyPolicyCompression,
}

impl From<ColumnFamilyPolicy> for RawColumnFamilyPolicy {
    fn from(value: ColumnFamilyPolicy) -> Self {
        Self {
            name: value.name,
            data: value.data.map(Into::into),
            external: value.external.map(Into::into),
            keep_in_memory: value.keep_in_memory.into(),
            compression: value.compression.into(),
        }
    }
}

impl From<RawColumnFamilyPolicy> for ydb_grpc::ydb_proto::table::ColumnFamilyPolicy {
    fn from(value: RawColumnFamilyPolicy) -> Self {
        Self {
            name: value.name,
            data: value.data.map(Into::into),
            external: value.external.map(Into::into),
            keep_in_memory: value.keep_in_memory.into(),
            compression: value.compression.into(),
        }
    }
}

pub(crate) struct RawStorageSettings {
    pub tablet_commit_log0: Option<RawStoragePool>,
    pub tablet_commit_log1: Option<RawStoragePool>,
    pub external: Option<RawStoragePool>,
    pub store_external_blobs: RawFeatureFlag,
}

impl From<StorageSettings> for RawStorageSettings {
    fn from(value: StorageSettings) -> Self {
        Self {
            tablet_commit_log0: value.tablet_commit_log0.map(Into::into),
            tablet_commit_log1: value.tablet_commit_log1.map(Into::into),
            external: value.external.map(Into::into),
            store_external_blobs: value.store_external_blobs.into(),
        }
    }
}

impl From<RawStorageSettings> for ydb_grpc::ydb_proto::table::StorageSettings {
    fn from(value: RawStorageSettings) -> Self {
        Self {
            tablet_commit_log0: value.tablet_commit_log0.map(Into::into),
            tablet_commit_log1: value.tablet_commit_log1.map(Into::into),
            external: value.external.map(Into::into),
            store_external_blobs: value.store_external_blobs.into(),
        }
    }
}

pub(crate) struct RawStoragePolicy {
    pub preset_name: String,
    pub syslog: Option<RawStoragePool>,
    pub log: Option<RawStoragePool>,
    pub data: Option<RawStoragePool>,
    pub external: Option<RawStoragePool>,
    pub keep_in_memory: RawFeatureFlag,
    pub column_families: Vec<RawColumnFamilyPolicy>,
}

impl From<StoragePolicy> for RawStoragePolicy {
    fn from(value: StoragePolicy) -> Self {
        Self {
            preset_name: value.preset_name,
            syslog: value.syslog.map(Into::into),
            log: value.log.map(Into::into),
            data: value.data.map(Into::into),
            external: value.external.map(Into::into),
            keep_in_memory: value.keep_in_memory.into(),
            column_families: value.column_families.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<RawStoragePolicy> for ydb_grpc::ydb_proto::table::StoragePolicy {
    fn from(value: RawStoragePolicy) -> Self {
        Self {
            preset_name: value.preset_name,
            syslog: value.syslog.map(Into::into),
            log: value.log.map(Into::into),
            data: value.data.map(Into::into),
            external: value.external.map(Into::into),
            keep_in_memory: value.keep_in_memory.into(),
            column_families: value.column_families.into_iter().map(Into::into).collect(),
        }
    }
}

pub(crate) struct RawCompactionPolicy {
    pub preset_name: String,
}

impl From<CompactionPolicy> for RawCompactionPolicy {
    fn from(value: CompactionPolicy) -> Self {
        Self {
            preset_name: value.preset_name,
        }
    }
}

impl From<RawCompactionPolicy> for ydb_grpc::ydb_proto::table::CompactionPolicy {
    fn from(value: RawCompactionPolicy) -> Self {
        Self {
            preset_name: value.preset_name,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RawAutoPartitioningPolicy {
    Unspecified,
    Disabled,
    AutoSplit,
    AutoSplitMerge,
}

impl From<AutoPartitioningPolicy> for RawAutoPartitioningPolicy {
    fn from(value: AutoPartitioningPolicy) -> Self {
        match value {
            AutoPartitioningPolicy::Unspecified => Self::Unspecified,
            AutoPartitioningPolicy::Disabled => Self::Disabled,
            AutoPartitioningPolicy::AutoSplit => Self::AutoSplit,
            AutoPartitioningPolicy::AutoSplitMerge => Self::AutoSplitMerge,
        }
    }
}

impl From<RawAutoPartitioningPolicy> for i32 {
    fn from(value: RawAutoPartitioningPolicy) -> Self {
        use ydb_grpc::ydb_proto::table::partitioning_policy::AutoPartitioningPolicy;
        match value {
            RawAutoPartitioningPolicy::Unspecified => AutoPartitioningPolicy::Unspecified as Self,
            RawAutoPartitioningPolicy::Disabled => AutoPartitioningPolicy::Disabled as Self,
            RawAutoPartitioningPolicy::AutoSplit => AutoPartitioningPolicy::AutoSplit as Self,
            RawAutoPartitioningPolicy::AutoSplitMerge => {
                AutoPartitioningPolicy::AutoSplitMerge as Self
            }
        }
    }
}

pub(crate) struct RawPartitioningPolicy {
    pub preset_name: String,
    pub auto_partitioning: RawAutoPartitioningPolicy,
    pub uniform_partitions: Option<u64>,
    pub partition_at_keys: Vec<RawTypedValue>,
}

impl TryFrom<PartitioningPolicy> for RawPartitioningPolicy {
    type Error = RawError;

    fn try_from(value: PartitioningPolicy) -> Result<Self, Self::Error> {
        let partition_at_keys = value
            .partition_at_keys
            .into_iter()
            .map(RawTypedValue::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            preset_name: value.preset_name,
            auto_partitioning: value.auto_partitioning.into(),
            uniform_partitions: value.uniform_partitions,
            partition_at_keys,
        })
    }
}

impl From<RawPartitioningPolicy> for ydb_grpc::ydb_proto::table::PartitioningPolicy {
    fn from(value: RawPartitioningPolicy) -> Self {
        let partitions = if let Some(count) = value.uniform_partitions {
            Some(
                ydb_grpc::ydb_proto::table::partitioning_policy::Partitions::UniformPartitions(
                    count,
                ),
            )
        } else if !value.partition_at_keys.is_empty() {
            Some(
                ydb_grpc::ydb_proto::table::partitioning_policy::Partitions::ExplicitPartitions(
                    ydb_grpc::ydb_proto::table::ExplicitPartitions {
                        split_points: value
                            .partition_at_keys
                            .into_iter()
                            .map(ydb_grpc::ydb_proto::TypedValue::from)
                            .collect(),
                    },
                ),
            )
        } else {
            None
        };

        Self {
            preset_name: value.preset_name,
            auto_partitioning: value.auto_partitioning.into(),
            partitions,
        }
    }
}

pub(crate) struct RawExecutionPolicy {
    pub preset_name: String,
}

impl From<ExecutionPolicy> for RawExecutionPolicy {
    fn from(value: ExecutionPolicy) -> Self {
        Self {
            preset_name: value.preset_name,
        }
    }
}

impl From<RawExecutionPolicy> for ydb_grpc::ydb_proto::table::ExecutionPolicy {
    fn from(value: RawExecutionPolicy) -> Self {
        Self {
            preset_name: value.preset_name,
        }
    }
}

pub(crate) struct RawReplicationPolicy {
    pub preset_name: String,
    pub replicas_count: u32,
    pub create_per_availability_zone: RawFeatureFlag,
    pub allow_promotion: RawFeatureFlag,
}

impl From<ReplicationPolicy> for RawReplicationPolicy {
    fn from(value: ReplicationPolicy) -> Self {
        Self {
            preset_name: value.preset_name,
            replicas_count: value.replicas_count,
            create_per_availability_zone: value.create_per_availability_zone.into(),
            allow_promotion: value.allow_promotion.into(),
        }
    }
}

impl From<RawReplicationPolicy> for ydb_grpc::ydb_proto::table::ReplicationPolicy {
    fn from(value: RawReplicationPolicy) -> Self {
        Self {
            preset_name: value.preset_name,
            replicas_count: value.replicas_count,
            create_per_availability_zone: value.create_per_availability_zone.into(),
            allow_promotion: value.allow_promotion.into(),
        }
    }
}

pub(crate) struct RawCachingPolicy {
    pub preset_name: String,
}

impl From<CachingPolicy> for RawCachingPolicy {
    fn from(value: CachingPolicy) -> Self {
        Self {
            preset_name: value.preset_name,
        }
    }
}

impl From<RawCachingPolicy> for ydb_grpc::ydb_proto::table::CachingPolicy {
    fn from(value: RawCachingPolicy) -> Self {
        Self {
            preset_name: value.preset_name,
        }
    }
}

pub(crate) struct RawTableProfile {
    pub preset_name: String,
    pub storage_policy: Option<RawStoragePolicy>,
    pub compaction_policy: Option<RawCompactionPolicy>,
    pub partitioning_policy: Option<RawPartitioningPolicy>,
    pub execution_policy: Option<RawExecutionPolicy>,
    pub replication_policy: Option<RawReplicationPolicy>,
    pub caching_policy: Option<RawCachingPolicy>,
}

impl TryFrom<TableProfile> for RawTableProfile {
    type Error = RawError;

    fn try_from(value: TableProfile) -> Result<Self, Self::Error> {
        Ok(Self {
            preset_name: value.preset_name,
            storage_policy: value.storage_policy.map(Into::into),
            compaction_policy: value.compaction_policy.map(Into::into),
            partitioning_policy: value
                .partitioning_policy
                .map(RawPartitioningPolicy::try_from)
                .transpose()?,
            execution_policy: value.execution_policy.map(Into::into),
            replication_policy: value.replication_policy.map(Into::into),
            caching_policy: value.caching_policy.map(Into::into),
        })
    }
}

impl From<RawTableProfile> for ydb_grpc::ydb_proto::table::TableProfile {
    fn from(value: RawTableProfile) -> Self {
        Self {
            preset_name: value.preset_name,
            storage_policy: value.storage_policy.map(Into::into),
            compaction_policy: value.compaction_policy.map(Into::into),
            partitioning_policy: value.partitioning_policy.map(Into::into),
            execution_policy: value.execution_policy.map(Into::into),
            replication_policy: value.replication_policy.map(Into::into),
            caching_policy: value.caching_policy.map(Into::into),
        }
    }
}

pub(crate) struct RawDateTypeColumnTtl {
    pub column_name: String,
    pub expire_after_seconds: u32,
}

impl From<DateTypeColumnTtl> for RawDateTypeColumnTtl {
    fn from(value: DateTypeColumnTtl) -> Self {
        Self {
            column_name: value.column_name,
            expire_after_seconds: value.expire_after_seconds,
        }
    }
}

impl From<RawDateTypeColumnTtl> for ydb_grpc::ydb_proto::table::DateTypeColumnModeSettings {
    fn from(value: RawDateTypeColumnTtl) -> Self {
        Self {
            column_name: value.column_name,
            expire_after_seconds: value.expire_after_seconds,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RawUnixEpochUnit {
    Unspecified,
    Seconds,
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

impl From<UnixEpochUnit> for RawUnixEpochUnit {
    fn from(value: UnixEpochUnit) -> Self {
        match value {
            UnixEpochUnit::Unspecified => Self::Unspecified,
            UnixEpochUnit::Seconds => Self::Seconds,
            UnixEpochUnit::Milliseconds => Self::Milliseconds,
            UnixEpochUnit::Microseconds => Self::Microseconds,
            UnixEpochUnit::Nanoseconds => Self::Nanoseconds,
        }
    }
}

impl From<RawUnixEpochUnit> for i32 {
    fn from(value: RawUnixEpochUnit) -> Self {
        use ydb_grpc::ydb_proto::table::value_since_unix_epoch_mode_settings::Unit;
        match value {
            RawUnixEpochUnit::Unspecified => Unit::Unspecified as Self,
            RawUnixEpochUnit::Seconds => Unit::Seconds as Self,
            RawUnixEpochUnit::Milliseconds => Unit::Milliseconds as Self,
            RawUnixEpochUnit::Microseconds => Unit::Microseconds as Self,
            RawUnixEpochUnit::Nanoseconds => Unit::Nanoseconds as Self,
        }
    }
}

pub(crate) struct RawValueSinceUnixEpochTtl {
    pub column_name: String,
    pub column_unit: RawUnixEpochUnit,
    pub expire_after_seconds: u32,
}

impl From<ValueSinceUnixEpochTtl> for RawValueSinceUnixEpochTtl {
    fn from(value: ValueSinceUnixEpochTtl) -> Self {
        Self {
            column_name: value.column_name,
            column_unit: value.column_unit.into(),
            expire_after_seconds: value.expire_after_seconds,
        }
    }
}

impl From<RawValueSinceUnixEpochTtl>
    for ydb_grpc::ydb_proto::table::ValueSinceUnixEpochModeSettings
{
    fn from(value: RawValueSinceUnixEpochTtl) -> Self {
        Self {
            column_name: value.column_name,
            column_unit: value.column_unit.into(),
            expire_after_seconds: value.expire_after_seconds,
        }
    }
}

pub(crate) enum RawTtlMode {
    DateTypeColumn(RawDateTypeColumnTtl),
    ValueSinceUnixEpoch(RawValueSinceUnixEpochTtl),
}

impl From<TtlMode> for RawTtlMode {
    fn from(value: TtlMode) -> Self {
        match value {
            TtlMode::DateTypeColumn(date) => Self::DateTypeColumn(date.into()),
            TtlMode::ValueSinceUnixEpoch(epoch) => Self::ValueSinceUnixEpoch(epoch.into()),
        }
    }
}

impl From<RawTtlMode> for ydb_grpc::ydb_proto::table::ttl_settings::Mode {
    fn from(value: RawTtlMode) -> Self {
        match value {
            RawTtlMode::DateTypeColumn(date) => Self::DateTypeColumn(
                ydb_grpc::ydb_proto::table::DateTypeColumnModeSettings::from(date),
            ),
            RawTtlMode::ValueSinceUnixEpoch(epoch) => Self::ValueSinceUnixEpoch(
                ydb_grpc::ydb_proto::table::ValueSinceUnixEpochModeSettings::from(epoch),
            ),
        }
    }
}

pub(crate) struct RawTtlSettings {
    pub run_interval_seconds: u32,
    pub mode: RawTtlMode,
}

impl From<TtlSettings> for RawTtlSettings {
    fn from(value: TtlSettings) -> Self {
        Self {
            run_interval_seconds: value.run_interval_seconds,
            mode: value.mode.into(),
        }
    }
}

impl From<RawTtlSettings> for ydb_grpc::ydb_proto::table::TtlSettings {
    fn from(value: RawTtlSettings) -> Self {
        Self {
            run_interval_seconds: value.run_interval_seconds,
            mode: Some(value.mode.into()),
        }
    }
}

pub(crate) struct RawTablePartitioningSettings {
    pub partition_by: Vec<String>,
    pub partitioning_by_size: RawFeatureFlag,
    pub partition_size_mb: u64,
    pub partitioning_by_load: RawFeatureFlag,
    pub min_partitions_count: u64,
    pub max_partitions_count: u64,
}

impl From<TablePartitioningSettings> for RawTablePartitioningSettings {
    fn from(value: TablePartitioningSettings) -> Self {
        Self {
            partition_by: value.partition_by,
            partitioning_by_size: value.partitioning_by_size.into(),
            partition_size_mb: value.partition_size_mb,
            partitioning_by_load: value.partitioning_by_load.into(),
            min_partitions_count: value.min_partitions_count,
            max_partitions_count: value.max_partitions_count,
        }
    }
}

impl From<RawTablePartitioningSettings> for ydb_grpc::ydb_proto::table::PartitioningSettings {
    fn from(value: RawTablePartitioningSettings) -> Self {
        Self {
            partition_by: value.partition_by,
            partitioning_by_size: value.partitioning_by_size.into(),
            partition_size_mb: value.partition_size_mb,
            partitioning_by_load: value.partitioning_by_load.into(),
            min_partitions_count: value.min_partitions_count,
            max_partitions_count: value.max_partitions_count,
        }
    }
}

pub(crate) enum RawReadReplicasSettings {
    PerAzReadReplicasCount(u64),
    AnyAzReadReplicasCount(u64),
}

impl From<ReadReplicasSettings> for RawReadReplicasSettings {
    fn from(value: ReadReplicasSettings) -> Self {
        match value {
            ReadReplicasSettings::PerAzReadReplicasCount(count) => {
                Self::PerAzReadReplicasCount(count)
            }
            ReadReplicasSettings::AnyAzReadReplicasCount(count) => {
                Self::AnyAzReadReplicasCount(count)
            }
        }
    }
}

impl From<RawReadReplicasSettings> for ydb_grpc::ydb_proto::table::ReadReplicasSettings {
    fn from(value: RawReadReplicasSettings) -> Self {
        let settings = match value {
            RawReadReplicasSettings::PerAzReadReplicasCount(count) => {
                ydb_grpc::ydb_proto::table::read_replicas_settings::Settings::PerAzReadReplicasCount(
                    count,
                )
            }
            RawReadReplicasSettings::AnyAzReadReplicasCount(count) => {
                ydb_grpc::ydb_proto::table::read_replicas_settings::Settings::AnyAzReadReplicasCount(
                    count,
                )
            }
        };

        Self {
            settings: Some(settings),
        }
    }
}

pub(crate) enum RawTablePartitions {
    Uniform(u64),
    AtKeys(Vec<RawTypedValue>),
}

impl TryFrom<TablePartitions> for RawTablePartitions {
    type Error = RawError;

    fn try_from(value: TablePartitions) -> Result<Self, Self::Error> {
        Ok(match value {
            TablePartitions::Uniform(count) => Self::Uniform(count),
            TablePartitions::AtKeys(keys) => {
                let split_points = keys
                    .into_iter()
                    .map(RawTypedValue::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Self::AtKeys(split_points)
            }
        })
    }
}

impl From<RawTablePartitions> for ydb_grpc::ydb_proto::table::create_table_request::Partitions {
    fn from(value: RawTablePartitions) -> Self {
        match value {
            RawTablePartitions::Uniform(count) => Self::UniformPartitions(count),
            RawTablePartitions::AtKeys(split_points) => {
                Self::PartitionAtKeys(ydb_grpc::ydb_proto::table::ExplicitPartitions {
                    split_points: split_points
                        .into_iter()
                        .map(ydb_grpc::ydb_proto::TypedValue::from)
                        .collect(),
                })
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RawStoreType {
    Unspecified,
    Row,
    Column,
}

impl From<StoreType> for RawStoreType {
    fn from(value: StoreType) -> Self {
        match value {
            StoreType::Unspecified => Self::Unspecified,
            StoreType::Row => Self::Row,
            StoreType::Column => Self::Column,
        }
    }
}

impl From<RawStoreType> for i32 {
    fn from(value: RawStoreType) -> Self {
        use ydb_grpc::ydb_proto::table::StoreType;
        match value {
            RawStoreType::Unspecified => StoreType::Unspecified as Self,
            RawStoreType::Row => StoreType::Row as Self,
            RawStoreType::Column => StoreType::Column as Self,
        }
    }
}
