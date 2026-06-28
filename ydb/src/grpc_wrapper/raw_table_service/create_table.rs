use crate::create_table_types::{
    AutoPartitioningPolicy, ColumnDefault, ColumnFamily, ColumnFamilyCompression,
    ColumnFamilyPolicy, ColumnFamilyPolicyCompression, CreateTableIndex, CreateTableOptions,
    FeatureFlag, PartitioningPolicy, ReadReplicasSettings, SequenceOptions, StoragePolicy,
    StorageSettings, TableColumn, TablePartitions, TtlMode, UnixEpochUnit,
};
use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::grpc_wrapper::raw_table_service::value::r#type::RawType;
use crate::grpc_wrapper::raw_table_service::value::RawTypedValue;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use crate::table_service_types::{IndexType, StoreType};
use crate::Value;
use ydb_grpc::ydb_proto::table::{
    self, CachingPolicy, ColumnMeta, CompactionPolicy, ExecutionPolicy, GlobalAsyncIndex,
    GlobalIndex, GlobalUniqueIndex, PartitioningPolicy as ProtoPartitioningPolicy,
    ReplicationPolicy, SequenceDescription, StoragePool, TableIndex, TableProfile, TtlSettings,
};
use ydb_grpc::ydb_proto::TypedValue;

pub(crate) struct RawCreateTableRequest {
    pub session_id: String,
    pub path: String,
    pub options: CreateTableOptions,
    pub operation_params: RawOperationParams,
}

impl RawCreateTableRequest {
    pub(crate) fn try_into_proto(self) -> RawResult<table::CreateTableRequest> {
        self.options
            .validate()
            .map_err(|e| RawError::custom(e.to_string()))?;
        Ok(table::CreateTableRequest {
            session_id: self.session_id,
            path: self.path,
            columns: self
                .options
                .columns
                .iter()
                .map(table_column_to_proto)
                .collect::<RawResult<Vec<_>>>()?,
            primary_key: self.options.primary_key,
            profile: self
                .options
                .profile
                .as_ref()
                .map(table_profile_to_proto)
                .transpose()?,
            operation_params: Some(self.operation_params.into()),
            indexes: self
                .options
                .indexes
                .iter()
                .map(create_table_index_to_proto)
                .collect(),
            ttl_settings: self
                .options
                .ttl_settings
                .as_ref()
                .map(ttl_settings_to_proto),
            storage_settings: self
                .options
                .storage_settings
                .as_ref()
                .map(storage_settings_to_proto),
            column_families: self
                .options
                .column_families
                .iter()
                .map(column_family_to_proto)
                .collect(),
            attributes: self.options.attributes,
            compaction_policy: self.options.compaction_policy.clone().unwrap_or_default(),
            partitioning_settings: self
                .options
                .partitioning_settings
                .as_ref()
                .map(partitioning_settings_to_proto),
            key_bloom_filter: feature_flag_to_proto(self.options.key_bloom_filter),
            read_replicas_settings: self
                .options
                .read_replicas_settings
                .as_ref()
                .map(read_replicas_settings_to_proto),
            tiering: self.options.tiering.clone().unwrap_or_default(),
            temporary: self.options.temporary,
            store_type: store_type_to_proto(self.options.store_type),
            partitions: self
                .options
                .partitions
                .as_ref()
                .map(table_partitions_to_proto)
                .transpose()?,
        })
    }
}

impl TryFrom<RawCreateTableRequest> for table::CreateTableRequest {
    type Error = RawError;

    fn try_from(value: RawCreateTableRequest) -> Result<Self, Self::Error> {
        value.try_into_proto()
    }
}

fn column_type_from_value(value: &Value) -> RawResult<RawType> {
    let typed: RawTypedValue = value.clone().try_into()?;
    Ok(typed.r#type)
}

fn table_column_to_proto(column: &TableColumn) -> RawResult<ColumnMeta> {
    let column_type = column_type_from_value(&column.type_example)?;
    let default_value = match &column.default_value {
        None => None,
        Some(ColumnDefault::Literal(value)) => {
            let typed: RawTypedValue = value.clone().try_into()?;
            Some(table::column_meta::DefaultValue::FromLiteral(TypedValue {
                r#type: Some(typed.r#type.into()),
                value: Some(typed.value.into()),
            }))
        }
        Some(ColumnDefault::Sequence(seq)) => Some(table::column_meta::DefaultValue::FromSequence(
            sequence_options_to_proto(seq),
        )),
    };

    Ok(ColumnMeta {
        name: column.name.clone(),
        r#type: Some(column_type.into()),
        family: column.family.clone(),
        not_null: if column.not_null { Some(true) } else { None },
        default_value,
    })
}

fn sequence_options_to_proto(seq: &SequenceOptions) -> SequenceDescription {
    SequenceDescription {
        name: seq.name.clone(),
        min_value: seq.min_value,
        max_value: seq.max_value,
        start_value: seq.start_value,
        cache: seq.cache,
        increment: seq.increment,
        cycle: seq.cycle,
        set_val: None,
    }
}

fn create_table_index_to_proto(index: &CreateTableIndex) -> TableIndex {
    TableIndex {
        name: index.name.clone(),
        index_columns: index.index_columns.clone(),
        data_columns: index.data_columns.clone(),
        r#type: Some(index_type_to_proto(index.index_type)),
    }
}

fn index_type_to_proto(index_type: IndexType) -> table::table_index::Type {
    match index_type {
        IndexType::Unspecified => table::table_index::Type::GlobalIndex(GlobalIndex {}),
        IndexType::Global => table::table_index::Type::GlobalIndex(GlobalIndex {}),
        IndexType::GlobalAsync => table::table_index::Type::GlobalAsyncIndex(GlobalAsyncIndex {}),
        IndexType::GlobalUnique => {
            table::table_index::Type::GlobalUniqueIndex(GlobalUniqueIndex {})
        }
    }
}

fn feature_flag_to_proto(flag: FeatureFlag) -> i32 {
    use ydb_grpc::ydb_proto::feature_flag::Status;
    match flag {
        FeatureFlag::Unspecified => Status::Unspecified as i32,
        FeatureFlag::Enabled => Status::Enabled as i32,
        FeatureFlag::Disabled => Status::Disabled as i32,
    }
}

fn storage_pool_to_proto(pool: &crate::create_table_types::StoragePool) -> StoragePool {
    StoragePool {
        media: pool.media.clone(),
    }
}

fn column_family_compression_to_proto(compression: ColumnFamilyCompression) -> i32 {
    use table::column_family::Compression;
    match compression {
        ColumnFamilyCompression::Unspecified => Compression::Unspecified as i32,
        ColumnFamilyCompression::None => Compression::None as i32,
        ColumnFamilyCompression::Lz4 => Compression::Lz4 as i32,
    }
}

fn column_family_policy_compression_to_proto(compression: ColumnFamilyPolicyCompression) -> i32 {
    use table::column_family_policy::Compression;
    match compression {
        ColumnFamilyPolicyCompression::Unspecified => Compression::Unspecified as i32,
        ColumnFamilyPolicyCompression::Uncompressed => Compression::Uncompressed as i32,
        ColumnFamilyPolicyCompression::Compressed => Compression::Compressed as i32,
    }
}

fn column_family_to_proto(family: &ColumnFamily) -> table::ColumnFamily {
    table::ColumnFamily {
        name: family.name.clone(),
        data: family.data.as_ref().map(storage_pool_to_proto),
        compression: column_family_compression_to_proto(family.compression),
        keep_in_memory: feature_flag_to_proto(family.keep_in_memory),
    }
}

fn column_family_policy_to_proto(policy: &ColumnFamilyPolicy) -> table::ColumnFamilyPolicy {
    table::ColumnFamilyPolicy {
        name: policy.name.clone(),
        data: policy.data.as_ref().map(storage_pool_to_proto),
        external: policy.external.as_ref().map(storage_pool_to_proto),
        keep_in_memory: feature_flag_to_proto(policy.keep_in_memory),
        compression: column_family_policy_compression_to_proto(policy.compression),
    }
}

fn storage_settings_to_proto(settings: &StorageSettings) -> table::StorageSettings {
    table::StorageSettings {
        tablet_commit_log0: settings
            .tablet_commit_log0
            .as_ref()
            .map(storage_pool_to_proto),
        tablet_commit_log1: settings
            .tablet_commit_log1
            .as_ref()
            .map(storage_pool_to_proto),
        external: settings.external.as_ref().map(storage_pool_to_proto),
        store_external_blobs: feature_flag_to_proto(settings.store_external_blobs),
    }
}

fn storage_policy_to_proto(policy: &StoragePolicy) -> table::StoragePolicy {
    table::StoragePolicy {
        preset_name: policy.preset_name.clone(),
        syslog: policy.syslog.as_ref().map(storage_pool_to_proto),
        log: policy.log.as_ref().map(storage_pool_to_proto),
        data: policy.data.as_ref().map(storage_pool_to_proto),
        external: policy.external.as_ref().map(storage_pool_to_proto),
        keep_in_memory: feature_flag_to_proto(policy.keep_in_memory),
        column_families: policy
            .column_families
            .iter()
            .map(column_family_policy_to_proto)
            .collect(),
    }
}

fn auto_partitioning_policy_to_proto(policy: AutoPartitioningPolicy) -> i32 {
    use table::partitioning_policy::AutoPartitioningPolicy as ProtoPolicy;
    match policy {
        AutoPartitioningPolicy::Unspecified => ProtoPolicy::Unspecified as i32,
        AutoPartitioningPolicy::Disabled => ProtoPolicy::Disabled as i32,
        AutoPartitioningPolicy::AutoSplit => ProtoPolicy::AutoSplit as i32,
        AutoPartitioningPolicy::AutoSplitMerge => ProtoPolicy::AutoSplitMerge as i32,
    }
}

fn partitioning_policy_to_proto(policy: &PartitioningPolicy) -> RawResult<ProtoPartitioningPolicy> {
    let partitions = if let Some(count) = policy.uniform_partitions {
        Some(table::partitioning_policy::Partitions::UniformPartitions(
            count,
        ))
    } else if !policy.partition_at_keys.is_empty() {
        let split_points = policy
            .partition_at_keys
            .iter()
            .map(|value| {
                let typed: RawTypedValue = value.clone().try_into()?;
                Ok(TypedValue {
                    r#type: Some(typed.r#type.into()),
                    value: Some(typed.value.into()),
                })
            })
            .collect::<RawResult<Vec<_>>>()?;
        Some(table::partitioning_policy::Partitions::ExplicitPartitions(
            table::ExplicitPartitions { split_points },
        ))
    } else {
        None
    };

    Ok(ProtoPartitioningPolicy {
        preset_name: policy.preset_name.clone(),
        auto_partitioning: auto_partitioning_policy_to_proto(policy.auto_partitioning),
        partitions,
    })
}

fn table_profile_to_proto(
    profile: &crate::create_table_types::TableProfile,
) -> RawResult<TableProfile> {
    Ok(TableProfile {
        preset_name: profile.preset_name.clone(),
        storage_policy: profile.storage_policy.as_ref().map(storage_policy_to_proto),
        compaction_policy: profile
            .compaction_policy
            .as_ref()
            .map(|p| CompactionPolicy {
                preset_name: p.preset_name.clone(),
            }),
        partitioning_policy: profile
            .partitioning_policy
            .as_ref()
            .map(partitioning_policy_to_proto)
            .transpose()?,
        execution_policy: profile.execution_policy.as_ref().map(|p| ExecutionPolicy {
            preset_name: p.preset_name.clone(),
        }),
        replication_policy: profile
            .replication_policy
            .as_ref()
            .map(|p| ReplicationPolicy {
                preset_name: p.preset_name.clone(),
                replicas_count: p.replicas_count,
                create_per_availability_zone: feature_flag_to_proto(p.create_per_availability_zone),
                allow_promotion: feature_flag_to_proto(p.allow_promotion),
            }),
        caching_policy: profile.caching_policy.as_ref().map(|p| CachingPolicy {
            preset_name: p.preset_name.clone(),
        }),
    })
}

fn ttl_settings_to_proto(settings: &crate::create_table_types::TtlSettings) -> TtlSettings {
    let mode = match &settings.mode {
        TtlMode::DateTypeColumn(date) => {
            table::ttl_settings::Mode::DateTypeColumn(table::DateTypeColumnModeSettings {
                column_name: date.column_name.clone(),
                expire_after_seconds: date.expire_after_seconds,
            })
        }
        TtlMode::ValueSinceUnixEpoch(epoch) => {
            table::ttl_settings::Mode::ValueSinceUnixEpoch(table::ValueSinceUnixEpochModeSettings {
                column_name: epoch.column_name.clone(),
                column_unit: unix_epoch_unit_to_proto(epoch.column_unit),
                expire_after_seconds: epoch.expire_after_seconds,
            })
        }
    };

    TtlSettings {
        run_interval_seconds: settings.run_interval_seconds,
        mode: Some(mode),
    }
}

fn unix_epoch_unit_to_proto(unit: UnixEpochUnit) -> i32 {
    use table::value_since_unix_epoch_mode_settings::Unit;
    match unit {
        UnixEpochUnit::Unspecified => Unit::Unspecified as i32,
        UnixEpochUnit::Seconds => Unit::Seconds as i32,
        UnixEpochUnit::Milliseconds => Unit::Milliseconds as i32,
        UnixEpochUnit::Microseconds => Unit::Microseconds as i32,
        UnixEpochUnit::Nanoseconds => Unit::Nanoseconds as i32,
    }
}

fn partitioning_settings_to_proto(
    settings: &crate::create_table_types::TablePartitioningSettings,
) -> table::PartitioningSettings {
    table::PartitioningSettings {
        partition_by: settings.partition_by.clone(),
        partitioning_by_size: feature_flag_to_proto(settings.partitioning_by_size),
        partition_size_mb: settings.partition_size_mb,
        partitioning_by_load: feature_flag_to_proto(settings.partitioning_by_load),
        min_partitions_count: settings.min_partitions_count,
        max_partitions_count: settings.max_partitions_count,
    }
}

fn read_replicas_settings_to_proto(settings: &ReadReplicasSettings) -> table::ReadReplicasSettings {
    let proto_settings = match settings {
        ReadReplicasSettings::PerAzReadReplicasCount(count) => {
            table::read_replicas_settings::Settings::PerAzReadReplicasCount(*count)
        }
        ReadReplicasSettings::AnyAzReadReplicasCount(count) => {
            table::read_replicas_settings::Settings::AnyAzReadReplicasCount(*count)
        }
    };

    table::ReadReplicasSettings {
        settings: Some(proto_settings),
    }
}

fn table_partitions_to_proto(
    partitions: &TablePartitions,
) -> RawResult<table::create_table_request::Partitions> {
    Ok(match partitions {
        TablePartitions::Uniform(count) => {
            table::create_table_request::Partitions::UniformPartitions(*count)
        }
        TablePartitions::AtKeys(keys) => {
            let split_points = keys
                .iter()
                .map(|value| {
                    let typed: RawTypedValue = value.clone().try_into()?;
                    Ok(TypedValue {
                        r#type: Some(typed.r#type.into()),
                        value: Some(typed.value.into()),
                    })
                })
                .collect::<RawResult<Vec<_>>>()?;
            table::create_table_request::Partitions::PartitionAtKeys(table::ExplicitPartitions {
                split_points,
            })
        }
    })
}

fn store_type_to_proto(store_type: StoreType) -> i32 {
    use table::StoreType as ProtoStoreType;
    match store_type {
        StoreType::Unspecified => ProtoStoreType::Unspecified as i32,
        StoreType::Row => ProtoStoreType::Row as i32,
        StoreType::Column => ProtoStoreType::Column as i32,
    }
}
