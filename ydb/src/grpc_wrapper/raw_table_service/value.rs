use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_table_service::value_type::Type;

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

impl TryFrom<ydb_grpc::ydb_proto::Value> for Value {
    type Error = RawError;

    fn try_from(value: ydb_grpc::ydb_proto::Value) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl From<Value> for ydb_grpc::ydb_proto::Value {
    fn from(_: Value) -> Self {
        todo!()
    }
}
