use crate::errors::{Error, Result};
use chrono::{Date, NaiveDate, Utc};
use ydb_protobuf::generated::ydb;

/// Represent value, send or received from ydb
/// That enum will be grow, when add support of new types
#[derive(Debug, PartialEq)]
pub enum YdbValue {
    NULL,
    Bool(bool),
    Int8(i8),
    Uint8(u8),
    Int16(i16),
    Uint16(u16),
    Int32(i32),
    Uint32(u32),
    Int64(i64),
    Uint64(u64),
    Float(f32),
    Double(f64),
    Date(NaiveDate),
    DateTime(chrono::NaiveDate),
    Timestamp(chrono::NaiveDate),
    Interval(std::time::Duration),
    TzDate(chrono::Date<Utc>),
    TzDateTime(chrono::DateTime<Utc>),
    String(Vec<u8>), // Bytes
    Utf8(String),
    Yson(String),
    Json(String),
    Uuid(Vec<u8>),
    JsonDocument(String),
    DyNumber(Vec<u8>),
    Decimal(rust_decimal::Decimal),
    Optional(Option<Box<YdbValue>>),
}

impl YdbValue {
    pub(crate) fn from_proto(proto_value: ydb::Value) -> Result<Self> {
        use ydb_protobuf::generated::ydb::value::Value::*;
        println!("from proto item: {:?}", proto_value);

        let val = match proto_value.value {
            None => return Err(Error::from("null value in proto value item")),
            Some(val) => match val {
                BoolValue(val) => YdbValue::Bool(val),
                Int32Value(val) => YdbValue::Int32(val),
                Uint32Value(val) => YdbValue::Uint32(val),
                Int64Value(val) => YdbValue::Int64(val),
                Uint64Value(val) => YdbValue::Uint64(val),
                FloatValue(val) => YdbValue::Float(val),
                DoubleValue(val) => YdbValue::Double(val),
                BytesValue(val) => YdbValue::String(val),
                TextValue(val) => YdbValue::Utf8(val),
                NullFlagValue(_) => YdbValue::NULL,
                NestedValue(_) => return Err(Error::from("not implemented read nested")),
                Low128(_) => return Err(Error::from("not implemented read i128")),
            },
        };
        return Ok(val);
    }

    // return empty value of requested type
    pub(crate) fn from_proto_type(proto_type: Option<ydb::Type>) -> Result<Self> {
        use ydb::r#type::PrimitiveTypeId as P;
        use ydb::r#type::Type as T;
        let res = if let Some(ydb::Type {
            r#type: Some(t_val),
        }) = proto_type
        {
            match t_val {
                T::TypeId(t_id) => match P::from_i32(t_id) {
                    Some(P::Bool) => Self::Bool(false),
                    Some(P::String) => Self::String(Vec::default()),
                    Some(P::Float) => Self::Float(0.0),
                    Some(P::Double) => Self::Double(0.0),
                    Some(P::Int32) => Self::Int32(0),
                    Some(P::Int64) => Self::Int64(0),
                    Some(P::Date) => unimplemented!("{:?} ({})", P::from_i32(t_id), t_id),
                    Some(P::Datetime) => unimplemented!("{:?} ({})", P::from_i32(t_id), t_id),
                    Some(P::Dynumber) => unimplemented!("{:?} ({})", P::from_i32(t_id), t_id),
                    Some(P::Interval) => unimplemented!("{:?} ({})", P::from_i32(t_id), t_id),
                    Some(P::Json) => Self::String(Vec::default()),
                    Some(P::JsonDocument) => Self::String(Vec::default()),
                    _ => unimplemented!("{:?} ({})", P::from_i32(t_id), t_id),
                },
                _ => unimplemented!("{:?}", t_val),
                // think about map to internal types as 1:1
            }
        } else {
            return Err(Error::Custom("column type is None".into()));
        };
        return Ok(res);
    }

    pub(crate) fn to_typed_value(self) -> ydb::TypedValue {
        use ydb::r#type::PrimitiveTypeId as pt;
        use ydb::value::Value as pv;

        fn to_typed(t: pt, v: pv) -> ydb::TypedValue {
            ydb::TypedValue {
                r#type: Some(ydb::Type {
                    r#type: Some(ydb::r#type::Type::TypeId(t.into())),
                }),
                value: Some(ydb::Value {
                    value: Some(v),
                    ..ydb::Value::default()
                }),
            }
        }

        match self {
            Self::NULL => panic!("unimplemented"),
            Self::Int32(val) => to_typed(pt::Int32, pv::Int32Value(val)),
            Self::Bool(val) => to_typed(pt::Bool, pv::BoolValue(val)),
            Self::Uint32(val) => to_typed(pt::Uint32, pv::Uint32Value(val)),
            Self::Int64(val) => to_typed(pt::Int64, pv::Int64Value(val)),
            Self::Uint64(val) => to_typed(pt::Uint64, pv::Uint64Value(val)),
            Self::Float(val) => to_typed(pt::Float, pv::FloatValue(val)),
            Self::Double(val) => to_typed(pt::Double, pv::DoubleValue(val)),
            Self::String(val) => to_typed(pt::String, pv::BytesValue(val)),
            Self::Utf8(val) => to_typed(pt::Utf8, pv::TextValue(val)),
        }
    }
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub(crate) v_type: YdbValue,
}
