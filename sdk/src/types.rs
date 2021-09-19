use crate::errors::{Error, Result};
use ydb_protobuf::generated::ydb::Value;

/// Represent value, send or received from ydb
/// That enum will be grow, when add support of new types
#[derive(Debug)]
pub enum YdbValue {
    NULL,
    BOOL(bool),
    INT32(i32),
    UINT32(u32),
    INT64(i64),
    UINT64(u64),
    FLOAT32(f32),
    FLOAT64(f64),
    BYTES(Vec<u8>),
    TEXT(String),
    YdbValue(Box<YdbValue>),
}

impl YdbValue {
    pub(crate) fn from_proto(proto_value: Value) -> Result<Self> {
        use ydb_protobuf::generated::ydb::value::Value::*;
        println!("from proto item: {:?}", proto_value);
        let val = match proto_value.value {
            None => return Err(Error::from("null value in proto value item")),
            Some(val) => match val {
                BoolValue(val) => YdbValue::BOOL(val),
                Int32Value(val) => YdbValue::INT32(val),
                Uint32Value(val) => YdbValue::UINT32(val),
                Int64Value(val) => YdbValue::INT64(val),
                Uint64Value(val) => YdbValue::UINT64(val),
                FloatValue(val) => YdbValue::FLOAT32(val),
                DoubleValue(val) => YdbValue::FLOAT64(val),
                BytesValue(val) => YdbValue::BYTES(val),
                TextValue(val) => YdbValue::TEXT(val),
                NullFlagValue(_) => YdbValue::NULL,
                NestedValue(val) => YdbValue::YdbValue(Box::new(Self::from_proto(*val)?)),
                Low128(_) => return Err(Error::from("not implemented read i128")),
            },
        };
        return Ok(val);
    }
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
}
