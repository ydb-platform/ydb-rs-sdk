use crate::create_table_types::{
    CreateTableIndex, CreateTableOptions, CreateTableOptionsBuilder, ReadReplicasSettings,
    TableColumn, TablePartitions, TtlMode, TtlSettings, ValueSinceUnixEpochTtl,
};
use crate::grpc_wrapper::raw_table_service::create_table::RawCreateTableRequest;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use crate::table_service_types::IndexType;
use crate::Value;

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
fn proto_mapping_basic_table() {
    let options = basic_options();
    let req = RawCreateTableRequest {
        session_id: "session".into(),
        path: "db/table".into(),
        options,
        operation_params: RawOperationParams::new_with_timeouts(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(1),
        ),
    };

    let proto = req.try_into_proto().unwrap();
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

    let options = builder.build().unwrap();
    let req = RawCreateTableRequest {
        session_id: "s".into(),
        path: "db/t".into(),
        options,
        operation_params: RawOperationParams::new_with_timeouts(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(1),
        ),
    };

    let proto = req.try_into_proto().unwrap();
    match proto.partitions {
        Some(ydb_grpc::ydb_proto::table::create_table_request::Partitions::UniformPartitions(
            count,
        )) => assert_eq!(count, 8),
        other => panic!("unexpected partitions: {other:?}"),
    }
}

#[test]
fn proto_mapping_read_replicas() {
    let options = CreateTableOptionsBuilder::default()
        .columns(vec![TableColumn::required("id", Value::Int64(0))])
        .primary_key(vec!["id".into()])
        .read_replicas_settings(ReadReplicasSettings::PerAzReadReplicasCount(3))
        .build()
        .unwrap();

    let req = RawCreateTableRequest {
        session_id: "s".into(),
        path: "db/t".into(),
        options,
        operation_params: RawOperationParams::new_with_timeouts(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(1),
        ),
    };

    let proto = req.try_into_proto().unwrap();
    let settings = proto.read_replicas_settings.unwrap().settings.unwrap();
    match settings {
        ydb_grpc::ydb_proto::table::read_replicas_settings::Settings::PerAzReadReplicasCount(
            count,
        ) => assert_eq!(count, 3),
        other => panic!("unexpected read replicas settings: {other:?}"),
    }
}

#[test]
fn proto_mapping_ttl_settings() {
    let options = CreateTableOptionsBuilder::default()
        .columns(vec![TableColumn::required("id", Value::Int64(0))])
        .primary_key(vec!["id".into()])
        .ttl_settings(TtlSettings {
            run_interval_seconds: 60,
            mode: TtlMode::ValueSinceUnixEpoch(ValueSinceUnixEpochTtl {
                column_name: "ts".into(),
                column_unit: crate::create_table_types::UnixEpochUnit::Seconds,
                expire_after_seconds: 3600,
            }),
        })
        .build()
        .unwrap();

    let req = RawCreateTableRequest {
        session_id: "s".into(),
        path: "db/t".into(),
        options,
        operation_params: RawOperationParams::new_with_timeouts(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(1),
        ),
    };

    let proto = req.try_into_proto().unwrap();
    let ttl = proto.ttl_settings.unwrap();
    assert_eq!(ttl.run_interval_seconds, 60);
    assert!(matches!(
        ttl.mode,
        Some(ydb_grpc::ydb_proto::table::ttl_settings::Mode::ValueSinceUnixEpoch(_))
    ));
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

    let req = RawCreateTableRequest {
        session_id: "s".into(),
        path: "db/t".into(),
        options,
        operation_params: RawOperationParams::new_with_timeouts(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(1),
        ),
    };

    let proto = req.try_into_proto().unwrap();
    assert!(matches!(
        proto.indexes[0].r#type,
        Some(ydb_grpc::ydb_proto::table::table_index::Type::GlobalUniqueIndex(_))
    ));
}

#[test]
fn table_partitions_at_keys() {
    let options = CreateTableOptionsBuilder::default()
        .columns(vec![TableColumn::required("id", Value::Uint64(0))])
        .primary_key(vec!["id".into()])
        .partitions(TablePartitions::AtKeys(vec![Value::Uint64(10)]))
        .build()
        .unwrap();

    let req = RawCreateTableRequest {
        session_id: "s".into(),
        path: "db/t".into(),
        options,
        operation_params: RawOperationParams::new_with_timeouts(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(1),
        ),
    };

    let proto = req.try_into_proto().unwrap();
    match proto.partitions {
        Some(ydb_grpc::ydb_proto::table::create_table_request::Partitions::PartitionAtKeys(
            keys,
        )) => assert_eq!(keys.split_points.len(), 1),
        other => panic!("unexpected partitions: {other:?}"),
    }
}
