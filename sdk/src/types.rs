use crate::errors::{Error, Result};
use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use std::ops::Deref;
use std::time::Duration;
use strum::EnumIter;
use ydb_protobuf::generated::ydb;

const SECONDS_PER_DAY: u64 = 60 * 60 * 24;

/// Represent value, send or received from ydb
/// That enum will be grow, when add support of new types
#[derive(Clone, Debug, EnumIter, PartialEq)]
#[allow(dead_code)]
pub enum YdbValue {
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
    Date(std::time::Duration), // seconds from UNIX_EPOCH to start of day in UTC.
    DateTime(std::time::Duration),
    Timestamp(std::time::Duration),
    Interval(std::time::Duration),
    String(Vec<u8>), // Bytes
    Utf8(String),
    Yson(String),
    Json(String),
    Uuid(Vec<u8>),
    JsonDocument(String),
    DyNumber(Vec<u8>),
    Decimal(rust_decimal::Decimal),
    Optional(Box<YdbOptional>),

    List(Vec<YdbValue>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct YdbOptional {
    T: YdbValue,
    Value: Option<YdbValue>,
}

impl Default for Box<YdbOptional> {
    fn default() -> Self {
        Box::new(YdbOptional {
            T: YdbValue::Bool(false),
            Value: None,
        })
    }
}

impl YdbValue {
    pub(crate) fn from_proto(t: &YdbValue, proto_value: ydb::Value) -> Result<Self> {
        use ydb_protobuf::generated::ydb::value::Value as pv;

        let res = match (t, proto_value) {
            (
                YdbValue::Bool(_),
                ydb::Value {
                    value: Some(pv::BoolValue(val)),
                    ..
                },
            ) => YdbValue::Bool(val),
            (
                YdbValue::Int8(_),
                ydb::Value {
                    value: Some(pv::Int32Value(val)),
                    ..
                },
            ) => YdbValue::Int8(val.try_into()?),
            (
                YdbValue::Uint8(_),
                ydb::Value {
                    value: Some(pv::Uint32Value(val)),
                    ..
                },
            ) => YdbValue::Uint8(val.try_into()?),
            (YdbValue::Int16(_), ydb::Value{
                value: Some(pv::Int32Value(val)),
                ..
            }) => YdbValue::Int16(val.try_into()?),
            (YdbValue::Uint16(_), ydb::Value{
                value: Some(pv::Uint32Value(val)),
                ..
            }) => YdbValue::Uint16(val.try_into()?),
            (
                YdbValue::Int32(_),
                ydb::Value {
                    value: Some(pv::Int32Value(val)),
                    ..
                },
            ) => YdbValue::Int32(val),
            (
                YdbValue::Uint32(_),
                ydb::Value {
                    value: Some(pv::Uint32Value(val)),
                    ..
                },
            ) => YdbValue::Uint32(val),
            (
                YdbValue::Int64(_),
                ydb::Value {
                    value: Some(pv::Int64Value(val)),
                    ..
                },
            ) => YdbValue::Int64(val),
            (
                YdbValue::Uint64(_),
                ydb::Value {
                    value: Some(pv::Uint64Value(val)),
                    ..
                },
            ) => YdbValue::Uint64(val),
            (
                YdbValue::Float(_),
                ydb::Value {
                    value: Some(pv::FloatValue(val)),
                    ..
                },
            ) => YdbValue::Float(val),
            (
                YdbValue::Double(_),
                ydb::Value {
                    value: Some(pv::DoubleValue(val)),
                    ..
                },
            ) => YdbValue::Double(val),
            (YdbValue::Date(_), ydb::Value{
                value: Some(pv::Uint32Value(val)),
                ..
            }) => {
                YdbValue::Date(std::time::Duration::from_secs( SECONDS_PER_DAY * val as u64))
            },
            (YdbValue::DateTime(_), _) => unimplemented!(),
            (YdbValue::Timestamp(_), _) => unimplemented!(),
            (YdbValue::Interval(_), _) => unimplemented!(),
            (
                YdbValue::String(_),
                ydb::Value {
                    value: Some(pv::BytesValue(val)),
                    ..
                },
            ) => YdbValue::String(val),
            (
                YdbValue::Utf8(_),
                ydb::Value {
                    value: Some(pv::TextValue(val)),
                    ..
                },
            ) => YdbValue::Utf8(val),
            (YdbValue::Yson(_), _) => unimplemented!(),
            (YdbValue::Json(_), _) => unimplemented!(),
            (YdbValue::Uuid(_), _) => unimplemented!(),
            (YdbValue::JsonDocument(_), _) => unimplemented!(),
            (YdbValue::DyNumber(_), _) => unimplemented!(),
            (YdbValue::Decimal(_), _) => unimplemented!(),
            (YdbValue::Optional(_), _) => unimplemented!(),
            (YdbValue::List(item_type_vec), ydb::Value { items, .. }) => {
                let items_type = &item_type_vec[0];
                let mut values = Vec::with_capacity(items.len());
                items.into_iter().try_for_each(|item| {
                    values.push(Self::from_proto(items_type, item)?);
                    Result::<()>::Ok(())
                })?;
                YdbValue::List(values)
            }
            (t, proto_value) => return return Err(Error::Custom(
                format!(
                    "unsupported YdbValue and proto_value combination: t: '{:?}', proto_value: '{:?}'",
                    t, proto_value
                )
                    .into(),
            )),
        };
        return Ok(res);
    }

    // return empty value of requested type
    pub(crate) fn from_proto_type(proto_type: &Option<ydb::Type>) -> Result<Self> {
        use ydb::r#type::PrimitiveTypeId as P;
        use ydb::r#type::Type as T;
        let res = if let Some(ydb::Type {
            r#type: Some(t_val),
        }) = proto_type
        {
            match t_val {
                T::TypeId(t_id) => match P::from_i32(*t_id) {
                    Some(P::Bool) => Self::Bool(false),
                    Some(P::String) => Self::String(Vec::default()),
                    Some(P::Float) => Self::Float(0.0),
                    Some(P::Double) => Self::Double(0.0),
                    Some(P::Int8) => Self::Int8(0),
                    Some(P::Uint8) => Self::Uint8(0),
                    Some(P::Int16) => Self::Int16(0),
                    Some(P::Uint16) => Self::Uint16(0),
                    Some(P::Int32) => Self::Int32(0),
                    Some(P::Uint32) => Self::Uint32(0),
                    Some(P::Int64) => Self::Int64(0),
                    Some(P::Uint64) => Self::Uint64(0),
                    Some(P::Date) => Self::Date(Duration::new(0, 0)),
                    Some(P::Datetime) => unimplemented!("{:?} ({})", P::from_i32(*t_id), *t_id),
                    Some(P::Dynumber) => unimplemented!("{:?} ({})", P::from_i32(*t_id), *t_id),
                    Some(P::Interval) => unimplemented!("{:?} ({})", P::from_i32(*t_id), *t_id),
                    Some(P::Json) => Self::String(Vec::default()),
                    Some(P::JsonDocument) => Self::String(Vec::default()),
                    _ => unimplemented!("{:?} ({})", P::from_i32(*t_id), *t_id),
                },
                T::ListType(oblt) => {
                    let item = if let Some(blt) = &oblt.item {
                        Self::from_proto_type(&Some(blt.deref().clone()))?
                    } else {
                        unimplemented!()
                    };
                    Self::List(Vec::from([item]))
                }
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

        fn proto_typed_value(t: pt, v: pv) -> ydb::TypedValue {
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

        #[allow(unreachable_patterns)]
        match self {
            Self::Bool(val) => proto_typed_value(pt::Bool, pv::BoolValue(val)),
            Self::Int8(val) => proto_typed_value(pt::Int8, pv::Int32Value(val.into())),
            Self::Uint8(val) => proto_typed_value(pt::Uint8, pv::Uint32Value(val.into())),
            Self::Int16(val) => proto_typed_value(pt::Int16, pv::Int32Value(val.into())),
            Self::Uint16(val) => proto_typed_value(pt::Uint16, pv::Uint32Value(val.into())),
            Self::Int32(val) => proto_typed_value(pt::Int32, pv::Int32Value(val)),
            Self::Uint32(val) => proto_typed_value(pt::Uint32, pv::Uint32Value(val)),
            Self::Int64(val) => proto_typed_value(pt::Int64, pv::Int64Value(val)),
            Self::Uint64(val) => proto_typed_value(pt::Uint64, pv::Uint64Value(val)),
            Self::Float(val) => proto_typed_value(pt::Float, pv::FloatValue(val)),
            Self::Double(val) => proto_typed_value(pt::Double, pv::DoubleValue(val)),
            Self::Date(val) => proto_typed_value(
                pt::Date,
                pv::Uint32Value((val.as_secs() / SECONDS_PER_DAY).try_into().unwrap()), // panic if out of range
            ),
            Self::DateTime(_) => unimplemented!(),
            Self::Timestamp(_) => unimplemented!(),
            Self::Interval(_) => unimplemented!(),
            Self::String(val) => proto_typed_value(pt::String, pv::BytesValue(val)),
            Self::Utf8(val) => proto_typed_value(pt::Utf8, pv::TextValue(val)),
            Self::Yson(_) => unimplemented!(),
            Self::Json(_) => unimplemented!(),
            Self::Uuid(_) => unimplemented!(),
            Self::JsonDocument(_) => unimplemented!(),
            Self::DyNumber(_) => unimplemented!(),
            Self::Decimal(_) => unimplemented!(),
            Self::Optional(_) => unimplemented!(),
            Self::List(items) => Self::to_typed_value_list(items),
        }
    }

    fn to_typed_value_list(items: Vec<YdbValue>) -> ydb::TypedValue {
        let proto_items: Vec<ydb::TypedValue> = items
            .into_iter()
            .map(|item| item.to_typed_value())
            .collect();
        if proto_items.len() == 0 {
            unimplemented!()
        };
        ydb::TypedValue {
            r#type: Some(ydb::Type {
                r#type: Some(ydb::r#type::Type::ListType(Box::new(ydb::ListType {
                    item: Some(Box::new(proto_items[0].r#type.clone().unwrap())),
                }))),
            }),
            value: Some(ydb::Value {
                items: proto_items
                    .into_iter()
                    .map(|item| item.value.unwrap())
                    .collect(),
                ..ydb::Value::default()
            }),
        }
    }
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub(crate) v_type: YdbValue,
}

#[cfg(test)]
mod test {
    use crate::errors::{UnitResult, UNIT_OK};
    use crate::types::YdbValue;
    use std::collections::HashSet;
    use std::convert::TryInto;
    use std::ops::Add;
    use strum::IntoEnumIterator;

    #[test]
    fn serialize() -> UnitResult {
        // test zero, one, minimum and maximum values
        macro_rules! num_tests {
            ($values:ident, $en_name:path, $type_name:ty) => {
                $values.push($en_name(0 as $type_name));
                $values.push($en_name(1 as $type_name));
                $values.push($en_name(<$type_name>::MIN));
                $values.push($en_name(<$type_name>::MAX));
            };
        }

        let mut discriminants = HashSet::new();
        let mut values = vec![YdbValue::Bool(false), YdbValue::Bool(true)];

        num_tests!(values, YdbValue::Int8, i8);
        num_tests!(values, YdbValue::Uint8, u8);
        num_tests!(values, YdbValue::Int16, i16);
        num_tests!(values, YdbValue::Uint16, u16);
        num_tests!(values, YdbValue::Int32, i32);
        num_tests!(values, YdbValue::Uint32, u32);
        num_tests!(values, YdbValue::Int64, i64);
        num_tests!(values, YdbValue::Uint64, u64);
        num_tests!(values, YdbValue::Float, f32);
        num_tests!(values, YdbValue::Double, f64);

        values.push(YdbValue::Date(std::time::Duration::from_secs(1633996800))); //Tue Oct 12 00:00:00 UTC 2021

        for v in values.into_iter() {
            discriminants.insert(std::mem::discriminant(&v));
            let proto = v.clone().to_typed_value();
            let t = YdbValue::from_proto_type(&proto.r#type)?;
            let v2 = YdbValue::from_proto(&t, proto.value.unwrap())?;
            assert_eq!(&v, &v2);
        }

        let mut non_tested = Vec::new();
        for v in YdbValue::iter() {
            if !discriminants.contains(&std::mem::discriminant(&v)) {
                non_tested.push(format!("{:?}", &v));
            }
        }

        assert_eq!(Vec::<String>::new(), non_tested);

        return UNIT_OK;
    }
}
