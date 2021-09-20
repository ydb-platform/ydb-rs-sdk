use crate::errors::{Error, Result};
use ydb_protobuf::generated::ydb;

/// Represent value, send or received from ydb
/// That enum will be grow, when add support of new types
#[derive(Debug, PartialEq)]
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
}

impl YdbValue {
    pub(crate) fn from_proto(proto_value: ydb::Value) -> Result<Self> {
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
                NestedValue(_) => return Err(Error::from("not implemented read nested")),
                Low128(_) => return Err(Error::from("not implemented read i128")),
            },
        };
        return Ok(val);
    }

    pub(crate) fn get_proto_type_id(
        &self,
    ) -> Option<ydb_protobuf::generated::ydb::r#type::PrimitiveTypeId> {
        use ydb_protobuf::generated::ydb::r#type::PrimitiveTypeId as ydb_id;

        match self {
            YdbValue::INT32(_) => Some(ydb_id::Int32),
            _ => panic!("todo"),
        }
    }

    pub(crate) fn to_typed_value(self) -> ydb::TypedValue {
        match self {
            Self::INT32(val) => ydb::TypedValue {
                r#type: Some(ydb::Type {
                    r#type: Some(ydb::r#type::Type::TypeId(
                        ydb::r#type::PrimitiveTypeId::Int32.into(),
                    )),
                }),
                value: Some(ydb::Value {
                    value: Some(ydb::value::Value::Int32Value(val)),
                    ..ydb::Value::default()
                }),
            },
            _ => panic!("todo"),
        }
    }

    pub(crate) fn to_ydb_value(self) -> ydb::Value {
        use YdbValue::*;

        match self {
            INT32(val) => ydb::Value {
                value: Some(ydb::value::Value::Int32Value(val)),
                ..ydb::Value::default()
            },
            _ => panic!("todo"),
        }
    }
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
}
