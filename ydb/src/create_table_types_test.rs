use std::collections::HashMap;
use std::time::UNIX_EPOCH;

use crate::create_table_types::{
    AutoPartitioningPolicy, CachingPolicy, ColumnDefault, ColumnFamily, ColumnFamilyCompression,
    ColumnFamilyPolicy, ColumnFamilyPolicyCompression, CompactionPolicy, CreateTableIndex,
    CreateTableOptions, CreateTableOptionsBuilder, DateTypeColumnTtl, ExecutionPolicy, FeatureFlag,
    PartitioningPolicy, ReadReplicasSettings, ReplicationPolicy, SequenceOptions, StoragePolicy,
    StoragePool, StorageSettings, TableColumn, TablePartitioningSettings, TablePartitions,
    TableProfile, TtlMode, TtlSettings, UnixEpochUnit, ValueSinceUnixEpochTtl,
};
use crate::grpc_wrapper::raw_table_service::create_table::RawCreateTableRequest;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use crate::table_service_types::{IndexType, StoreType};
use crate::Value;

fn operation_params() -> RawOperationParams {
    RawOperationParams::new_with_timeouts(
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(1),
    )
}

fn raw_request(options: CreateTableOptions) -> RawCreateTableRequest {
    RawCreateTableRequest {
        session_id: "session".into(),
        path: "db/table".into(),
        options,
        operation_params: operation_params(),
    }
}

fn proto_from_options(
    options: CreateTableOptions,
) -> ydb_grpc::ydb_proto::table::CreateTableRequest {
    raw_request(options).try_into_proto().unwrap()
}

fn basic_options() -> CreateTableOptions {
    CreateTableOptionsBuilder::default()
        .columns(vec![
            TableColumn::required("id", Value::Int64(0)),
            TableColumn::nullable("message", Value::Text(String::new())).unwrap(),
        ])
        .primary_key(vec!["id".into()])
        .indexes(vec![CreateTableIndex::global(
            "idx_message",
            vec!["message".into()],
        )])
        .build()
        .unwrap()
}

fn sample_storage_pool() -> StoragePool {
    StoragePool::new("ssd")
}

#[test]
fn validate_rejects_empty_columns() {
    let options = CreateTableOptionsBuilder::default()
        .columns(vec![])
        .primary_key(vec!["id".into()])
        .build()
        .unwrap();

    let err = options.validate().unwrap_err().to_string();
    assert!(err.contains("columns must not be empty"));
}

#[test]
fn validate_rejects_empty_primary_key() {
    let options = CreateTableOptionsBuilder::default()
        .columns(vec![TableColumn::required("id", Value::Int64(0))])
        .primary_key(vec![])
        .build()
        .unwrap();

    let err = options.validate().unwrap_err().to_string();
    assert!(err.contains("primary_key must not be empty"));
}

#[test]
fn validate_rejects_unknown_primary_key_column() {
    let options = CreateTableOptionsBuilder::default()
        .columns(vec![TableColumn::required("id", Value::Int64(0))])
        .primary_key(vec!["missing".into()])
        .build()
        .unwrap();

    let err = options.validate().unwrap_err().to_string();
    assert!(err.contains("primary_key column 'missing'"));
}

#[test]
fn table_column_helpers() {
    let literal_col = TableColumn::required("id", Value::Int64(0))
        .with_family("default")
        .with_default(ColumnDefault::Literal(Value::Int64(42)));

    let sequence_col = TableColumn::required("seq", Value::Int64(0)).with_default(
        ColumnDefault::Sequence(SequenceOptions {
            name: Some("seq_name".into()),
            min_value: Some(1),
            max_value: Some(100),
            start_value: Some(1),
            cache: Some(10),
            increment: Some(1),
            cycle: Some(false),
        }),
    );

    let options = CreateTableOptionsBuilder::default()
        .columns(vec![literal_col, sequence_col])
        .primary_key(vec!["id".into(), "seq".into()])
        .build()
        .unwrap();

    let proto = proto_from_options(options);
    assert_eq!(proto.columns[0].family, "default");
    assert!(proto.columns[0].not_null.unwrap());
    assert!(matches!(
        proto.columns[0].default_value,
        Some(ydb_grpc::ydb_proto::table::column_meta::DefaultValue::FromLiteral(_))
    ));
    assert!(matches!(
        proto.columns[1].default_value,
        Some(ydb_grpc::ydb_proto::table::column_meta::DefaultValue::FromSequence(_))
    ));
}

#[test]
fn create_table_index_helpers() {
    let index = CreateTableIndex::global_async("idx_async", vec!["a".into()])
        .with_data_columns(vec!["b".into()]);
    assert_eq!(index.index_type, IndexType::GlobalAsync);
    assert_eq!(index.data_columns, vec!["b"]);

    let unique = CreateTableIndex::global_unique("idx_unique", vec!["c".into()]);
    assert_eq!(unique.index_type, IndexType::GlobalUnique);
}

#[test]
fn create_table_options_builder_uniform_partitions() {
    let mut builder = CreateTableOptionsBuilder::default();
    builder
        .columns(vec![TableColumn::required("id", Value::Uint64(0))])
        .primary_key(vec!["id".into()])
        .uniform_partitions(16);

    let options = builder.build().unwrap();
    assert_eq!(options.partitions, Some(TablePartitions::Uniform(16)));
}

#[test]
fn try_from_raw_create_table_request() {
    let req = raw_request(basic_options());
    let proto: ydb_grpc::ydb_proto::table::CreateTableRequest = req.try_into().unwrap();
    assert_eq!(proto.path, "db/table");
}

#[test]
fn proto_mapping_basic_table() {
    let proto = proto_from_options(basic_options());
    assert_eq!(proto.session_id, "session");
    assert_eq!(proto.path, "db/table");
    assert_eq!(proto.columns.len(), 2);
    assert_eq!(proto.primary_key, vec!["id"]);
    assert_eq!(proto.indexes.len(), 1);
    assert_eq!(proto.indexes[0].name, "idx_message");
}

#[test]
fn proto_mapping_uniform_partitions() {
    let mut builder = CreateTableOptionsBuilder::default();
    builder
        .columns(vec![TableColumn::required("id", Value::Uint64(0))])
        .primary_key(vec!["id".into()])
        .uniform_partitions(8);

    let proto = proto_from_options(builder.build().unwrap());
    match proto.partitions {
        Some(ydb_grpc::ydb_proto::table::create_table_request::Partitions::UniformPartitions(
            count,
        )) => assert_eq!(count, 8),
        other => panic!("unexpected partitions: {other:?}"),
    }
}

#[test]
fn proto_mapping_read_replicas_per_az() {
    let options = CreateTableOptionsBuilder::default()
        .columns(vec![TableColumn::required("id", Value::Int64(0))])
        .primary_key(vec!["id".into()])
        .read_replicas_settings(ReadReplicasSettings::PerAzReadReplicasCount(3))
        .build()
        .unwrap();

    let settings = proto_from_options(options)
        .read_replicas_settings
        .unwrap()
        .settings
        .unwrap();
    match settings {
        ydb_grpc::ydb_proto::table::read_replicas_settings::Settings::PerAzReadReplicasCount(
            count,
        ) => assert_eq!(count, 3),
        other => panic!("unexpected read replicas settings: {other:?}"),
    }
}

#[test]
fn proto_mapping_read_replicas_any_az() {
    let options = CreateTableOptionsBuilder::default()
        .columns(vec![TableColumn::required("id", Value::Int64(0))])
        .primary_key(vec!["id".into()])
        .read_replicas_settings(ReadReplicasSettings::AnyAzReadReplicasCount(5))
        .build()
        .unwrap();

    let settings = proto_from_options(options)
        .read_replicas_settings
        .unwrap()
        .settings
        .unwrap();
    match settings {
        ydb_grpc::ydb_proto::table::read_replicas_settings::Settings::AnyAzReadReplicasCount(
            count,
        ) => assert_eq!(count, 5),
        other => panic!("unexpected read replicas settings: {other:?}"),
    }
}

#[test]
fn proto_mapping_ttl_unix_epoch() {
    let options = CreateTableOptionsBuilder::default()
        .columns(vec![TableColumn::required("id", Value::Int64(0))])
        .primary_key(vec!["id".into()])
        .ttl_settings(TtlSettings {
            run_interval_seconds: 60,
            mode: TtlMode::ValueSinceUnixEpoch(ValueSinceUnixEpochTtl {
                column_name: "ts".into(),
                column_unit: UnixEpochUnit::Seconds,
                expire_after_seconds: 3600,
            }),
        })
        .build()
        .unwrap();

    let ttl = proto_from_options(options).ttl_settings.unwrap();
    assert_eq!(ttl.run_interval_seconds, 60);
    assert!(matches!(
        ttl.mode,
        Some(ydb_grpc::ydb_proto::table::ttl_settings::Mode::ValueSinceUnixEpoch(_))
    ));
}

#[test]
fn proto_mapping_ttl_date_column() {
    let options = CreateTableOptionsBuilder::default()
        .columns(vec![TableColumn::required("id", Value::Int64(0))])
        .primary_key(vec!["id".into()])
        .ttl_settings(TtlSettings {
            run_interval_seconds: 30,
            mode: TtlMode::DateTypeColumn(DateTypeColumnTtl {
                column_name: "created".into(),
                expire_after_seconds: 86400,
            }),
        })
        .build()
        .unwrap();

    let ttl = proto_from_options(options).ttl_settings.unwrap();
    assert!(matches!(
        ttl.mode,
        Some(ydb_grpc::ydb_proto::table::ttl_settings::Mode::DateTypeColumn(_))
    ));
}

#[test]
fn proto_mapping_unix_epoch_units() {
    for unit in [
        UnixEpochUnit::Unspecified,
        UnixEpochUnit::Seconds,
        UnixEpochUnit::Milliseconds,
        UnixEpochUnit::Microseconds,
        UnixEpochUnit::Nanoseconds,
    ] {
        let options = CreateTableOptionsBuilder::default()
            .columns(vec![TableColumn::required("id", Value::Int64(0))])
            .primary_key(vec!["id".into()])
            .ttl_settings(TtlSettings {
                run_interval_seconds: 1,
                mode: TtlMode::ValueSinceUnixEpoch(ValueSinceUnixEpochTtl {
                    column_name: "ts".into(),
                    column_unit: unit,
                    expire_after_seconds: 1,
                }),
            })
            .build()
            .unwrap();

        assert!(proto_from_options(options).ttl_settings.is_some());
    }
}

#[test]
fn create_table_index_type_mapping() {
    let index = CreateTableIndex::new("idx", vec!["c".into()], IndexType::GlobalUnique);
    let options = CreateTableOptionsBuilder::default()
        .columns(vec![
            TableColumn::required("id", Value::Int64(0)),
            TableColumn::required("c", Value::Text(String::new())),
        ])
        .primary_key(vec!["id".into()])
        .indexes(vec![index])
        .build()
        .unwrap();

    let proto = proto_from_options(options);
    assert!(matches!(
        proto.indexes[0].r#type,
        Some(ydb_grpc::ydb_proto::table::table_index::Type::GlobalUniqueIndex(_))
    ));
}

#[test]
fn proto_mapping_index_types() {
    for (index_type, expected) in [
        (IndexType::Unspecified, "GlobalIndex"),
        (IndexType::Global, "GlobalIndex"),
        (IndexType::GlobalAsync, "GlobalAsyncIndex"),
        (IndexType::GlobalUnique, "GlobalUniqueIndex"),
    ] {
        let options = CreateTableOptionsBuilder::default()
            .columns(vec![
                TableColumn::required("id", Value::Int64(0)),
                TableColumn::required("c", Value::Text(String::new())),
            ])
            .primary_key(vec!["id".into()])
            .indexes(vec![CreateTableIndex::new(
                "idx",
                vec!["c".into()],
                index_type,
            )])
            .build()
            .unwrap();

        let index_type_name = format!("{:?}", proto_from_options(options).indexes[0].r#type);
        assert!(
            index_type_name.contains(expected),
            "index type {index_type:?} mapped to {index_type_name}"
        );
    }
}

#[test]
fn table_partitions_at_keys() {
    let options = CreateTableOptionsBuilder::default()
        .columns(vec![TableColumn::required("id", Value::Uint64(0))])
        .primary_key(vec!["id".into()])
        .partitions(TablePartitions::AtKeys(vec![Value::Uint64(10)]))
        .build()
        .unwrap();

    match proto_from_options(options).partitions {
        Some(ydb_grpc::ydb_proto::table::create_table_request::Partitions::PartitionAtKeys(
            keys,
        )) => assert_eq!(keys.split_points.len(), 1),
        other => panic!("unexpected partitions: {other:?}"),
    }
}

#[test]
fn proto_mapping_storage_settings_and_column_families() {
    let pool = sample_storage_pool();
    let options = CreateTableOptionsBuilder::default()
        .columns(vec![TableColumn::required("id", Value::Int64(0))])
        .primary_key(vec!["id".into()])
        .storage_settings(StorageSettings {
            tablet_commit_log0: Some(pool.clone()),
            tablet_commit_log1: Some(pool.clone()),
            external: Some(pool.clone()),
            store_external_blobs: FeatureFlag::Enabled,
        })
        .column_families(vec![
            ColumnFamily {
                name: "default".into(),
                data: Some(pool.clone()),
                compression: ColumnFamilyCompression::Lz4,
                keep_in_memory: FeatureFlag::Disabled,
            },
            ColumnFamily {
                name: "cold".into(),
                data: None,
                compression: ColumnFamilyCompression::None,
                keep_in_memory: FeatureFlag::Unspecified,
            },
        ])
        .build()
        .unwrap();

    let proto = proto_from_options(options);
    let storage = proto.storage_settings.unwrap();
    assert!(storage.tablet_commit_log0.is_some());
    assert_eq!(storage.store_external_blobs, 1);
    assert_eq!(proto.column_families.len(), 2);
    assert_eq!(proto.column_families[0].compression, 2);
}

#[test]
fn proto_mapping_table_profile() {
    let pool = sample_storage_pool();
    let options = CreateTableOptionsBuilder::default()
        .columns(vec![TableColumn::required("id", Value::Uint64(0))])
        .primary_key(vec!["id".into()])
        .profile(TableProfile {
            preset_name: "default".into(),
            storage_policy: Some(StoragePolicy {
                preset_name: "storage".into(),
                syslog: Some(pool.clone()),
                log: Some(pool.clone()),
                data: Some(pool.clone()),
                external: Some(pool.clone()),
                keep_in_memory: FeatureFlag::Enabled,
                column_families: vec![ColumnFamilyPolicy {
                    name: "default".into(),
                    data: Some(pool.clone()),
                    external: Some(pool.clone()),
                    keep_in_memory: FeatureFlag::Disabled,
                    compression: ColumnFamilyPolicyCompression::Compressed,
                }],
            }),
            compaction_policy: Some(CompactionPolicy {
                preset_name: "compaction".into(),
            }),
            partitioning_policy: Some(PartitioningPolicy {
                preset_name: "partitioning".into(),
                auto_partitioning: AutoPartitioningPolicy::AutoSplitMerge,
                uniform_partitions: Some(4),
                partition_at_keys: vec![Value::Uint64(100)],
            }),
            execution_policy: Some(ExecutionPolicy {
                preset_name: "execution".into(),
            }),
            replication_policy: Some(ReplicationPolicy {
                preset_name: "replication".into(),
                replicas_count: 3,
                create_per_availability_zone: FeatureFlag::Enabled,
                allow_promotion: FeatureFlag::Disabled,
            }),
            caching_policy: Some(CachingPolicy {
                preset_name: "caching".into(),
            }),
        })
        .build()
        .unwrap();

    let profile = proto_from_options(options).profile.unwrap();
    assert_eq!(profile.preset_name, "default");
    assert!(profile.storage_policy.is_some());
    assert!(profile.compaction_policy.is_some());
    assert!(profile.partitioning_policy.is_some());
    assert!(profile.execution_policy.is_some());
    assert!(profile.replication_policy.is_some());
    assert!(profile.caching_policy.is_some());
}

#[test]
fn proto_mapping_profile_partitioning_at_keys() {
    let options = CreateTableOptionsBuilder::default()
        .columns(vec![TableColumn::required("id", Value::Uint64(0))])
        .primary_key(vec!["id".into()])
        .profile(TableProfile {
            preset_name: "p".into(),
            storage_policy: None,
            compaction_policy: None,
            partitioning_policy: Some(PartitioningPolicy {
                preset_name: "part".into(),
                auto_partitioning: AutoPartitioningPolicy::Disabled,
                uniform_partitions: None,
                partition_at_keys: vec![Value::Uint64(1), Value::Uint64(2)],
            }),
            execution_policy: None,
            replication_policy: None,
            caching_policy: None,
        })
        .build()
        .unwrap();

    let partitioning = proto_from_options(options)
        .profile
        .unwrap()
        .partitioning_policy
        .unwrap();
    assert!(matches!(
        partitioning.partitions,
        Some(ydb_grpc::ydb_proto::table::partitioning_policy::Partitions::ExplicitPartitions(_))
    ));
}

#[test]
fn proto_mapping_auto_partitioning_policies() {
    for policy in [
        AutoPartitioningPolicy::Unspecified,
        AutoPartitioningPolicy::Disabled,
        AutoPartitioningPolicy::AutoSplit,
        AutoPartitioningPolicy::AutoSplitMerge,
    ] {
        let options = CreateTableOptionsBuilder::default()
            .columns(vec![TableColumn::required("id", Value::Uint64(0))])
            .primary_key(vec!["id".into()])
            .profile(TableProfile {
                preset_name: "p".into(),
                storage_policy: None,
                compaction_policy: None,
                partitioning_policy: Some(PartitioningPolicy {
                    preset_name: "part".into(),
                    auto_partitioning: policy,
                    uniform_partitions: None,
                    partition_at_keys: vec![],
                }),
                execution_policy: None,
                replication_policy: None,
                caching_policy: None,
            })
            .build()
            .unwrap();

        assert!(proto_from_options(options)
            .profile
            .unwrap()
            .partitioning_policy
            .is_some());
    }
}

#[test]
fn proto_mapping_partitioning_settings() {
    let options = CreateTableOptionsBuilder::default()
        .columns(vec![TableColumn::required("id", Value::Uint64(0))])
        .primary_key(vec!["id".into()])
        .partitioning_settings(TablePartitioningSettings {
            partition_by: vec!["id".into()],
            partitioning_by_size: FeatureFlag::Enabled,
            partition_size_mb: 128,
            partitioning_by_load: FeatureFlag::Disabled,
            min_partitions_count: 1,
            max_partitions_count: 100,
        })
        .build()
        .unwrap();

    let settings = proto_from_options(options).partitioning_settings.unwrap();
    assert_eq!(settings.partition_by, vec!["id"]);
    assert_eq!(settings.partition_size_mb, 128);
    assert_eq!(settings.min_partitions_count, 1);
}

#[test]
fn proto_mapping_misc_table_options() {
    let mut attributes = HashMap::new();
    attributes.insert("owner".into(), "test".into());

    let options = CreateTableOptionsBuilder::default()
        .columns(vec![TableColumn::required("id", Value::Int64(0))])
        .primary_key(vec!["id".into()])
        .attributes(attributes)
        .compaction_policy("small_table".into())
        .key_bloom_filter(FeatureFlag::Enabled)
        .tiering("tier1".into())
        .temporary(true)
        .store_type(StoreType::Column)
        .build()
        .unwrap();

    let proto = proto_from_options(options);
    assert_eq!(
        proto.attributes.get("owner").map(String::as_str),
        Some("test")
    );
    assert_eq!(proto.compaction_policy, "small_table");
    assert_eq!(proto.key_bloom_filter, 1);
    assert_eq!(proto.tiering, "tier1");
    assert!(proto.temporary);
    assert_eq!(proto.store_type, 2);
}

#[test]
fn proto_mapping_store_types() {
    for (store_type, expected) in [
        (StoreType::Unspecified, 0),
        (StoreType::Row, 1),
        (StoreType::Column, 2),
    ] {
        let options = CreateTableOptionsBuilder::default()
            .columns(vec![TableColumn::required("id", Value::Int64(0))])
            .primary_key(vec!["id".into()])
            .store_type(store_type)
            .build()
            .unwrap();

        assert_eq!(proto_from_options(options).store_type, expected);
    }
}

#[test]
fn proto_mapping_column_family_policy_compression_variants() {
    for compression in [
        ColumnFamilyPolicyCompression::Unspecified,
        ColumnFamilyPolicyCompression::Uncompressed,
        ColumnFamilyPolicyCompression::Compressed,
    ] {
        let options = CreateTableOptionsBuilder::default()
            .columns(vec![TableColumn::required("id", Value::Int64(0))])
            .primary_key(vec!["id".into()])
            .profile(TableProfile {
                preset_name: "p".into(),
                storage_policy: Some(StoragePolicy {
                    preset_name: "s".into(),
                    syslog: None,
                    log: None,
                    data: None,
                    external: None,
                    keep_in_memory: FeatureFlag::Unspecified,
                    column_families: vec![ColumnFamilyPolicy {
                        name: "default".into(),
                        data: None,
                        external: None,
                        keep_in_memory: FeatureFlag::Unspecified,
                        compression,
                    }],
                }),
                compaction_policy: None,
                partitioning_policy: None,
                execution_policy: None,
                replication_policy: None,
                caching_policy: None,
            })
            .build()
            .unwrap();

        let families = proto_from_options(options)
            .profile
            .unwrap()
            .storage_policy
            .unwrap()
            .column_families;
        assert_eq!(families.len(), 1);
    }
}

#[test]
fn proto_mapping_full_options_smoke() {
    let pool = sample_storage_pool();
    let options = CreateTableOptionsBuilder::default()
        .columns(vec![
            TableColumn::required("id", Value::Uint64(0)),
            TableColumn::nullable("ts", Value::Timestamp(UNIX_EPOCH)).unwrap(),
        ])
        .primary_key(vec!["id".into()])
        .indexes(vec![CreateTableIndex::global_async(
            "idx_ts",
            vec!["ts".into()],
        )
        .with_data_columns(vec!["id".into()])])
        .profile(TableProfile {
            preset_name: "default".into(),
            storage_policy: None,
            compaction_policy: Some(CompactionPolicy {
                preset_name: "default".into(),
            }),
            partitioning_policy: Some(PartitioningPolicy {
                preset_name: "default".into(),
                auto_partitioning: AutoPartitioningPolicy::AutoSplit,
                uniform_partitions: Some(2),
                partition_at_keys: vec![],
            }),
            execution_policy: None,
            replication_policy: None,
            caching_policy: None,
        })
        .ttl_settings(TtlSettings {
            run_interval_seconds: 10,
            mode: TtlMode::DateTypeColumn(DateTypeColumnTtl {
                column_name: "ts".into(),
                expire_after_seconds: 3600,
            }),
        })
        .storage_settings(StorageSettings {
            tablet_commit_log0: Some(pool.clone()),
            tablet_commit_log1: None,
            external: None,
            store_external_blobs: FeatureFlag::Disabled,
        })
        .column_families(vec![ColumnFamily {
            name: "default".into(),
            data: Some(pool),
            compression: ColumnFamilyCompression::Unspecified,
            keep_in_memory: FeatureFlag::Unspecified,
        }])
        .attributes(HashMap::from([("k".into(), "v".into())]))
        .compaction_policy("default".into())
        .partitioning_settings(TablePartitioningSettings {
            partition_by: vec!["id".into()],
            partitioning_by_size: FeatureFlag::Unspecified,
            partition_size_mb: 64,
            partitioning_by_load: FeatureFlag::Unspecified,
            min_partitions_count: 1,
            max_partitions_count: 10,
        })
        .uniform_partitions(2)
        .key_bloom_filter(FeatureFlag::Disabled)
        .read_replicas_settings(ReadReplicasSettings::AnyAzReadReplicasCount(1))
        .tiering("default".into())
        .temporary(false)
        .store_type(StoreType::Row)
        .build()
        .unwrap();

    let proto = proto_from_options(options);
    assert_eq!(proto.columns.len(), 2);
    assert!(!proto.temporary);
    assert_eq!(proto.store_type, 1);
}
