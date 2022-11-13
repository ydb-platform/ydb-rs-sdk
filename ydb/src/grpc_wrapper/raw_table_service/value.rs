use crate::grpc_wrapper::raw_table_service::value_type::Type;

pub(crate) struct TypedValue {
    r#type: Type,
}

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
    VariantIndex(Box<VariantValue>),
}

pub(crate) struct ValuePair {
    key: Value,
    payload: Value,
}

pub(crate) struct VariantValue {
    value: Value,
    index: u32,
}
