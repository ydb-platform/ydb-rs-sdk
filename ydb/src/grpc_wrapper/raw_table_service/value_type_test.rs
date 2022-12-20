use super::*;
use std::collections::HashSet;
use strum::EnumCount;

#[test]
fn consistent_conversion() -> RawResult<()> {
    use RawType::*;

    let values = vec![
        Bool,
        Int8,
        Uint8,
        Int16,
        Uint16,
        Int32,
        Uint32,
        Int64,
        Uint64,
        Float,
        Double,
        Date,
        DateTime,
        Timestamp,
        Interval,
        TzDate,
        TzDatetime,
        TzTimestamp,
        Bytes, // String
        UTF8,
        YSON,
        JSON,
        UUID,
        JSONDocument,
        DyNumber,
        Void,
        Null,
        EmptyList,
        EmptyDict,
        Decimal(DecimalType {
            precision: 5,
            scale: 19,
        }),
        Optional(Box::new(Bytes)),
        Optional(Box::new(Optional(Box::new(List(Box::new(JSONDocument)))))),
        List(Box::new(UTF8)),
        List(Box::new(List(Box::new(UUID)))),
        Tuple(TupleType { elements: vec![] }),
        Tuple(TupleType {
            elements: vec![List(Box::new(Bytes)), Int64],
        }),
        Struct(StructType { members: vec![] }),
        Struct(StructType {
            members: vec![
                StructMember {
                    name: "qwe".to_string(),
                    member_type: Bool,
                },
                StructMember {
                    name: "dfg".to_string(),
                    member_type: Int32,
                },
            ],
        }),
        Dict(Box::new(DictType {
            key: Int32,
            payload: Bytes,
        })),
        Variant(VariantType::Tuple(TupleType {
            elements: vec![Bool, Int8],
        })),
        Variant(VariantType::Struct(StructType {
            members: vec![StructMember {
                name: "field".to_string(),
                member_type: Bytes,
            }],
        })),
        Tagged(Box::new(TaggedType {
            tag: "tag_name".to_string(),
            item_type: Uint32,
        })),
    ];
    let mut discriminants = HashSet::new();

    for v in values.into_iter() {
        let proto: ydb_grpc::ydb_proto::Type = v.clone().into();
        let reverse_internal: RawType = proto.try_into()?;
        assert_eq!(v, reverse_internal);
        discriminants.insert(std::mem::discriminant(&v));
    }

    assert_eq!(discriminants.len(), RawType::COUNT);

    Ok(())
}
