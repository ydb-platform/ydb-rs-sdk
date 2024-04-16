use super::*;
use crate::grpc_wrapper::raw_errors::RawResult;
use std::collections::HashSet;
use strum::EnumCount;

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
