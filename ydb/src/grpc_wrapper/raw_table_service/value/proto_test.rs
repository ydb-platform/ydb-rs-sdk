use super::*;
use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::grpc_wrapper::raw_table_service::value::r#type::{
    DictType, RawType, StructMember, StructType, TupleType,
};
use std::collections::HashSet;
use strum::EnumCount;
use ydb_grpc::ydb_proto::Value as ProtoValue;

#[test]
fn convert_raw_protobuf() -> RawResult<()> {
    use RawValue::*;

    let test_values = vec![
        Bool(true),
        Bool(false),
        Int32(-1),
        Int32(2),
        UInt32(1),
        Int64(-1),
        Int64(1),
        UInt64(1),
        HighLow128(1, 2),
        Float(0.4),
        Double(0.3),
        Bytes(vec![]),
        Bytes(vec![1, 2, 3]),
        Text("".to_string()),
        Text("asd".to_string()),
        NullFlag,
        Items(vec![Bool(true), Int32(9)]),
        Pairs(vec![RawValuePair {
            key: Int32(2),
            payload: Float(0.1),
        }]),
        Variant(Box::new(RawVariantValue {
            value: Int64(4),
            index: 5,
        })),
    ];
    let mut discriminants = HashSet::new();

    for v in test_values {
        discriminants.insert(std::mem::discriminant(&v));
        let proto: ydb_grpc::ydb_proto::Value = v.clone().into();
        let reverse_v: RawValue = proto.try_into()?;
        assert_eq!(v, reverse_v);
    }

    assert_eq!(discriminants.len(), RawValue::COUNT);

    Ok(())
}

/// The protobuf value of an empty container: no field set.
fn empty() -> ProtoValue {
    ProtoValue::default()
}

fn list_of(item_type: RawType) -> RawType {
    RawType::List(Box::new(item_type))
}

fn struct_of(members: Vec<(&str, RawType)>) -> RawType {
    RawType::Struct(StructType {
        members: members
            .into_iter()
            .map(|(name, member_type)| StructMember {
                name: name.to_string(),
                member_type,
            })
            .collect(),
    })
}

#[test]
fn an_empty_list_decodes_by_its_type() -> RawResult<()> {
    assert_eq!(
        RawValue::from_proto(empty(), &list_of(RawType::Uint64))?,
        RawValue::Items(vec![])
    );
    Ok(())
}

#[test]
fn an_empty_container_decodes_as_its_type_says() -> RawResult<()> {
    use RawValue::*;

    let cases = vec![
        (struct_of(vec![]), Items(vec![])),
        (
            RawType::Tuple(TupleType { elements: vec![] }),
            Items(vec![]),
        ),
        (
            RawType::Dict(Box::new(DictType {
                key: RawType::UTF8,
                payload: RawType::Int64,
            })),
            Pairs(vec![]),
        ),
        // A present optional value is encoded as the value itself.
        (
            RawType::Optional(Box::new(list_of(RawType::Uint64))),
            Items(vec![]),
        ),
    ];
    for (t, expected) in cases {
        assert_eq!(RawValue::from_proto(empty(), &t)?, expected, "{t:?}");
    }
    Ok(())
}

#[test]
fn an_empty_list_nested_in_a_container_decodes() -> RawResult<()> {
    use RawValue::*;

    let t = struct_of(vec![
        ("id", RawType::Int32),
        ("refs", list_of(RawType::Uint64)),
        ("nested", list_of(list_of(RawType::Uint64))),
    ]);
    let value = ProtoValue {
        items: vec![
            Int32(7).into(),
            empty(),
            ProtoValue {
                items: vec![empty(), Items(vec![UInt64(1)]).into()],
                ..ProtoValue::default()
            },
        ],
        ..ProtoValue::default()
    };

    assert_eq!(
        RawValue::from_proto(value, &t)?,
        Items(vec![
            Int32(7),
            Items(vec![]),
            Items(vec![Items(vec![]), Items(vec![UInt64(1)])]),
        ])
    );
    Ok(())
}

#[test]
fn an_empty_value_of_a_scalar_type_is_refused() {
    assert!(RawValue::from_proto(empty(), &RawType::Uint64).is_err());
}

#[test]
fn items_not_matching_the_type_are_refused() {
    let t = struct_of(vec![("a", RawType::Int32), ("b", RawType::Int32)]);
    let value = ProtoValue {
        items: vec![RawValue::Int32(1).into()],
        ..ProtoValue::default()
    };

    assert!(RawValue::from_proto(value, &t).is_err());
}

#[test]
fn a_result_set_decodes_each_cell_by_its_column_type() -> RawResult<()> {
    use crate::types::Value;

    let column = |name: &str, t: RawType| ydb_grpc::ydb_proto::Column {
        name: name.to_string(),
        r#type: Some(t.into()),
    };
    let result_set = ydb_grpc::ydb_proto::ResultSet {
        columns: vec![
            column("id", RawType::Int64),
            column("refs", list_of(RawType::Uint64)),
        ],
        rows: vec![ProtoValue {
            items: vec![RawValue::Int64(1).into(), empty()],
            ..ProtoValue::default()
        }],
        ..ydb_grpc::ydb_proto::ResultSet::default()
    };

    let raw = RawResultSet::try_from(result_set)?;
    assert_eq!(
        raw.rows,
        vec![vec![RawValue::Int64(1), RawValue::Items(vec![])]]
    );

    let refs: Value = RawTypedValue {
        r#type: list_of(RawType::Uint64),
        value: RawValue::Items(vec![]),
    }
    .try_into()?;
    assert_eq!(
        refs,
        Value::list_from(Value::Uint64(0), vec![]).map_err(|e| RawError::custom(e.to_string()))?
    );
    Ok(())
}

#[test]
fn a_continuation_part_decodes_its_rows_with_the_known_columns() -> RawResult<()> {
    let known = vec![RawColumn {
        name: "refs".to_string(),
        column_type: list_of(RawType::Uint64),
    }];
    let part = ydb_grpc::ydb_proto::ResultSet {
        rows: vec![ProtoValue {
            items: vec![empty()],
            ..ProtoValue::default()
        }],
        ..ydb_grpc::ydb_proto::ResultSet::default()
    };

    let raw = RawResultSet::from_proto_part(part, &known)?;
    assert!(raw.columns.is_empty());
    assert_eq!(raw.rows, vec![vec![RawValue::Items(vec![])]]);
    Ok(())
}
