use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_table_service::value_type::Type;
use ydb_grpc::ydb_proto::value::Value as Primitive;
use ydb_grpc::ydb_proto::Value as ProtoValue;

#[cfg(test)]
#[path = "value_test.rs"]
mod value_test;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TypedValue {
    pub r#type: Type,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq, strum::EnumCount)]
pub(crate) enum Value {
    Bool(bool),
    Int32(i32),
    UInt32(u32),
    Int64(i64),
    UInt64(u64),
    HighLow128(u64, u64), // high, low
    Float(f32),
    Double(f64),
    Bytes(Vec<u8>),
    Text(String),
    NullFlag,
    NestedValue(Box<Value>),
    Items(Vec<Value>),
    Pairs(Vec<ValuePair>),
    Variant(Box<VariantValue>),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ValuePair {
    key: Value,
    payload: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct VariantValue {
    value: Value,
    index: u32,
}

impl TryFrom<ProtoValue> for Value {
    type Error = RawError;

    fn try_from(value: ProtoValue) -> Result<Self, Self::Error> {
        todo!()
    }
}

//
// internal to protobuf
//

impl From<Value> for ProtoValue {
    fn from(v: Value) -> Self {
        match v {
            Value::Bool(v) => ProtoValue {
                value: Some(Primitive::BoolValue(v)),
                ..ProtoValue::default()
            },
            Value::Int32(v) => ProtoValue {
                value: Some(Primitive::Int32Value(v)),
                ..ProtoValue::default()
            },
            Value::UInt32(v) => ProtoValue {
                value: Some(Primitive::Uint32Value(v)),
                ..ProtoValue::default()
            },
            Value::Int64(v) => ProtoValue {
                value: Some(Primitive::Int64Value(v)),
                ..ProtoValue::default()
            },
            Value::UInt64(v) => ProtoValue {
                value: Some(Primitive::Uint64Value(v)),
                ..ProtoValue::default()
            },
            Value::HighLow128(h, l) => ProtoValue {
                value: Some(Primitive::Low128(l)),
                high_128: h,
                ..ProtoValue::default()
            },
            Value::Float(v) => ProtoValue {
                value: Some(Primitive::FloatValue(v)),
                ..ProtoValue::default()
            },
            Value::Double(v) => ProtoValue {
                value: Some(Primitive::DoubleValue(v)),
                ..ProtoValue::default()
            },
            Value::Bytes(v) => ProtoValue {
                value: Some(Primitive::BytesValue(v)),
                ..ProtoValue::default()
            },
            Value::Text(v) => ProtoValue {
                value: Some(Primitive::TextValue(v)),
                ..ProtoValue::default()
            },
            Value::NullFlag => ProtoValue {
                value: Some(Primitive::NullFlagValue(0)),
                ..ProtoValue::default()
            },
            Value::NestedValue(v) => ProtoValue {
                value: Some(Primitive::NestedValue(Box::new((*v).into()))),
                ..ProtoValue::default()
            },
            Value::Items(v) => ProtoValue {
                items: v.into_iter().map(|item| item.into()).collect(),
                ..ProtoValue::default()
            },
            Value::Pairs(v) => ProtoValue {
                pairs: v.into_iter().map(|item| item.into()).collect(),
                ..ProtoValue::default()
            },
            Value::Variant(v) => ProtoValue {
                value: Some(v.value.into()),
                variant_index: v.index,
                ..ProtoValue::default()
            },
        }
    }
}

impl From<ValuePair> for ydb_grpc::ydb_proto::ValuePair {
    fn from(v: ValuePair) -> Self {
        Self {
            key: Some(v.key.into()),
            payload: Some(v.payload.into()),
        }
    }
}
