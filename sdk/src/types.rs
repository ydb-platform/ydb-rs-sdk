use crate::errors::{Error, Result};

use std::convert::TryInto;
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
#[non_exhaustive]
pub enum YdbValue {
    Void,
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
    Interval(SignedInterval),
    String(Vec<u8>), // Bytes
    Utf8(String),
    Yson(Vec<u8>),
    Json(Vec<u8>),
    JsonDocument(Vec<u8>),

    Optional(Box<YdbOptional>),
    List(Box<YdbList>),
    Struct(YdbStruct),
}

#[derive(Clone, Debug, PartialEq)]
pub struct YdbStruct {
    fields_name: Vec<String>,
    values: Vec<YdbValue>,
}

impl YdbStruct {
    pub fn insert(&mut self, name: String, v: YdbValue) {
        self.fields_name.push(name);
        self.values.push(v);
    }

    pub fn from_names_and_values(fields_name: Vec<String>, values: Vec<YdbValue>) -> Result<Self> {
        if fields_name.len() != values.len() {
            return Err(Error::Custom(format!("different len fields_name and values. fields_name len: {}, values len: {}. fields_name: {:?}, values: {:?}", fields_name.len(), values.len(), fields_name, values).into()));
        };

        return Ok(YdbStruct {
            fields_name,
            values,
        });
    }

    pub fn new() -> Self {
        return Self::with_capacity(0);
    }

    pub fn with_capacity(capacity: usize) -> Self {
        return YdbStruct {
            fields_name: Vec::with_capacity(capacity),
            values: Vec::with_capacity(capacity),
        };
    }
}

impl Default for YdbStruct {
    fn default() -> Self {
        return Self::new();
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct YdbList {
    pub t: YdbValue,
    pub values: Vec<YdbValue>,
}

impl Default for Box<YdbList> {
    fn default() -> Self {
        Box::new(YdbList {
            t: YdbValue::Bool(false),
            values: Vec::default(),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct YdbOptional {
    pub t: YdbValue,
    pub value: Option<YdbValue>,
}

impl Default for Box<YdbOptional> {
    fn default() -> Self {
        Box::new(YdbOptional {
            t: YdbValue::Bool(false),
            value: None,
        })
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Sign {
    Plus,
    Minus,
}

impl Default for Sign {
    fn default() -> Self {
        return Sign::Plus;
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct SignedInterval {
    pub sign: Sign,
    pub duration: Duration,
}

impl SignedInterval {
    pub(crate) fn as_nanos(self) -> Result<i64> {
        let nanos: i64 = self.duration.as_nanos().try_into()?;
        let res = match self.sign {
            Sign::Plus => nanos,
            Sign::Minus => -nanos,
        };
        return Ok(res);
    }

    pub(crate) fn from_nanos(nanos: i64) -> Self {
        let (sign, nanos) = if nanos >= 0 {
            (Sign::Plus, nanos as u64)
        } else {
            (Sign::Minus, (-nanos) as u64)
        };

        return Self {
            sign,
            duration: Duration::from_nanos(nanos),
        };
    }
}

impl YdbValue {
    pub fn list_from(t: YdbValue, values: Vec<YdbValue>) -> Result<Self> {
        for (index, value) in values.iter().enumerate() {
            if std::mem::discriminant(&t) != std::mem::discriminant(value) {
                return Err(Error::Custom(format!("failed list_from: type and value has different enum-types. index: {}, type: '{:?}', value: '{:?}'", index, t, value)));
            }
        }

        return Ok(YdbValue::List(Box::new(YdbList { t, values })));
    }

    pub fn optional_from(t: YdbValue, value: Option<YdbValue>) -> Result<Self> {
        if let Some(value) = &value {
            if std::mem::discriminant(&t) != std::mem::discriminant(value) {
                return Err(Error::Custom(format!("failed optional_from: type and value has different enum-types. type: '{:?}', value: '{:?}'", t, value)));
            }
        }
        Ok(YdbValue::Optional(Box::new(YdbOptional { t, value })))
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
                    Some(P::Utf8) => Self::Utf8(String::default()),
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
                    Some(P::Timestamp) => Self::Timestamp(Duration::default()),
                    Some(P::Interval) => Self::Interval(SignedInterval::default()),
                    Some(P::Date) => Self::Date(Duration::default()),
                    Some(P::Datetime) => Self::DateTime(Duration::default()),
                    Some(P::Dynumber) => unimplemented!("{:?} ({})", P::from_i32(*t_id), *t_id),
                    Some(P::Json) => Self::Json(Vec::default()),
                    Some(P::Yson) => Self::Yson(Vec::default()),
                    Some(P::JsonDocument) => Self::JsonDocument(Vec::default()),
                    _ => unimplemented!("{:?} ({})", P::from_i32(*t_id), *t_id),
                },
                T::VoidType(_) => YdbValue::Void,
                T::OptionalType(val) => {
                    let t = if let Some(item) = &val.item {
                        Some(*item.clone())
                    } else {
                        return Err(Error::Custom("none item in optional type".into()));
                    };
                    return Self::optional_from(Self::from_proto_type(&t)?, None);
                }
                T::ListType(oblt) => {
                    let item = if let Some(blt) = &oblt.item {
                        Self::from_proto_type(&Some(blt.deref().clone()))?
                    } else {
                        unimplemented!()
                    };
                    Self::List(Box::new(YdbList {
                        t: item,
                        values: Vec::default(),
                    }))
                }
                T::StructType(struct_type) => {
                    let mut s = YdbStruct::with_capacity(struct_type.members.len());
                    for field in &struct_type.members {
                        let t = Self::from_proto_type(&field.r#type)?;
                        s.insert(field.name.clone(), t);
                    }
                    Self::Struct(s)
                }
                _ => unimplemented!("{:?}", t_val),
                // think about map to internal types as 1:1
            }
        } else {
            return Err(Error::Custom("column type is None".into()));
        };
        return Ok(res);
    }

    pub(crate) fn from_proto(t: &YdbValue, proto_value: ydb::Value) -> Result<Self> {
        let res = match (t, proto_value) {
            (YdbValue::Void, _) => YdbValue::Void,
            (
                t,
                ydb::Value {
                    value: Some(val), ..
                },
            ) => Self::from_proto_value(t, val)?,
            (YdbValue::List(item_type_vec), ydb::Value { items, .. }) => {
                let items_type = &item_type_vec.t;
                let mut values = Vec::with_capacity(items.len());
                items.into_iter().try_for_each(|item| {
                    values.push(Self::from_proto(items_type, item)?);
                    Result::<()>::Ok(())
                })?;
                YdbValue::List(Box::new(YdbList {
                    t: items_type.clone(),
                    values,
                }))
            }
            (YdbValue::Struct(struct_t), ydb::Value { items, .. }) => {
                Self::from_proto_struct(struct_t, items)?
            }
            (t, proto_value) => {
                return Err(Error::Custom(
                    format!(
                        "unsupported from_proto combination: t: '{:?}', proto_value: '{:?}'",
                        t, proto_value
                    )
                    .into(),
                ))
            }
        };
        return Ok(res);
    }

    fn from_proto_struct(t: &YdbStruct, items: Vec<ydb::Value>) -> Result<YdbValue> {
        if t.fields_name.len() != items.len() {
            return Err(Error::Custom(
                format!(
                    "struct description and items has diferrent length. t: {:?}, items: {:?}",
                    t, items
                )
                .into(),
            ));
        };

        let mut res = YdbStruct::with_capacity(t.fields_name.len());
        for (index, item) in items.into_iter().enumerate() {
            let v = YdbValue::from_proto(&t.values[index], item)?;
            res.insert(t.fields_name[index].clone(), v);
        }
        return Ok(YdbValue::Struct(res));
    }

    fn from_proto_value(
        t: &YdbValue,
        v: ydb_protobuf::generated::ydb::value::Value,
    ) -> Result<YdbValue> {
        use ydb_protobuf::generated::ydb::value::Value as pv;

        let res = match (t, v) {
            (YdbValue::Bool(_), pv::BoolValue(val)) => YdbValue::Bool(val),
            (YdbValue::Int8(_), pv::Int32Value(val)) => YdbValue::Int8(val.try_into()?),
            (YdbValue::Uint8(_), pv::Uint32Value(val)) => YdbValue::Uint8(val.try_into()?),
            (YdbValue::Int16(_), pv::Int32Value(val)) => YdbValue::Int16(val.try_into()?),
            (YdbValue::Uint16(_), pv::Uint32Value(val)) => YdbValue::Uint16(val.try_into()?),
            (YdbValue::Int32(_), pv::Int32Value(val)) => YdbValue::Int32(val),
            (YdbValue::Uint32(_), pv::Uint32Value(val)) => YdbValue::Uint32(val),
            (YdbValue::Int64(_), pv::Int64Value(val)) => YdbValue::Int64(val),
            (YdbValue::Uint64(_), pv::Uint64Value(val)) => YdbValue::Uint64(val),
            (YdbValue::Float(_), pv::FloatValue(val)) => YdbValue::Float(val),
            (YdbValue::Double(_), pv::DoubleValue(val)) => YdbValue::Double(val),
            (YdbValue::Date(_), pv::Uint32Value(val)) => {
                YdbValue::Date(std::time::Duration::from_secs(SECONDS_PER_DAY * val as u64))
            }
            (YdbValue::DateTime(_), pv::Uint32Value(val)) => {
                YdbValue::DateTime(std::time::Duration::from_secs(val as u64))
            }
            (YdbValue::Timestamp(_), pv::Uint64Value(val)) => {
                YdbValue::Timestamp(Duration::from_micros(val))
            }
            (YdbValue::Interval(_), pv::Int64Value(val)) => {
                YdbValue::Interval(SignedInterval::from_nanos(val))
            }
            (YdbValue::String(_), pv::BytesValue(val)) => YdbValue::String(val),
            (YdbValue::Utf8(_), pv::TextValue(val)) => YdbValue::Utf8(val),
            (YdbValue::Yson(_), pv::TextValue(val)) => YdbValue::Yson(Vec::from(val)),
            (YdbValue::Json(_), pv::TextValue(val)) => YdbValue::Json(Vec::from(val)),
            (YdbValue::JsonDocument(_), pv::TextValue(val)) => {
                YdbValue::JsonDocument(Vec::from(val))
            }
            (YdbValue::Optional(ydb_optional), val) => {
                Self::from_proto_value_optional(ydb_optional, val)?
            }
            (t, val) => {
                return Err(Error::Custom(format!(
                    "unexpected from_proto_value. t: '{:?}', val: '{:?}'",
                    t, val
                )))
            }
        };
        return Ok(res);
    }

    fn from_proto_value_optional(
        t: &Box<YdbOptional>,
        val: ydb_protobuf::generated::ydb::value::Value,
    ) -> Result<Self> {
        use ydb_protobuf::generated::ydb::value::Value as pv;

        let res = match val {
            pv::NullFlagValue(_) => Self::optional_from(t.t.clone(), None)?,
            val => Self::optional_from(t.t.clone(), Some(Self::from_proto_value(&t.t, val)?))?,
        };
        return Ok(res);
    }

    pub(crate) fn to_typed_value(self) -> Result<ydb::TypedValue> {
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
        let res = match self {
            Self::Void => ydb::TypedValue {
                r#type: Some(ydb::Type {
                    r#type: Some(ydb::r#type::Type::VoidType(
                        prost_types::NullValue::NullValue.into(),
                    )),
                }),
                value: Some(ydb::Value {
                    value: Some(ydb::value::Value::NullFlagValue(
                        prost_types::NullValue::NullValue.into(),
                    )),
                    ..ydb::Value::default()
                }),
            },
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
                pv::Uint32Value((val.as_secs() / SECONDS_PER_DAY).try_into()?),
            ),
            Self::DateTime(val) => {
                proto_typed_value(pt::Datetime, pv::Uint32Value(val.as_secs().try_into()?))
            }
            Self::Timestamp(val) => {
                proto_typed_value(pt::Timestamp, pv::Uint64Value(val.as_micros().try_into()?))
            }
            Self::Interval(val) => proto_typed_value(pt::Interval, pv::Int64Value(val.as_nanos()?)),
            Self::String(val) => proto_typed_value(pt::String, pv::BytesValue(val)),
            Self::Utf8(val) => proto_typed_value(pt::Utf8, pv::TextValue(val)),
            Self::Yson(val) => proto_typed_value(pt::Yson, pv::TextValue(String::from_utf8(val)?)),
            Self::Json(val) => proto_typed_value(pt::Json, pv::TextValue(String::from_utf8(val)?)),
            Self::JsonDocument(val) => {
                proto_typed_value(pt::JsonDocument, pv::TextValue(String::from_utf8(val)?))
            }
            Self::Optional(val) => Self::to_typed_optional(val)?,
            Self::List(items) => Self::to_typed_value_list(items)?,
            YdbValue::Struct(s) => { Self::to_typed_struct(s) }?,
        };
        return Ok(res);
    }

    fn to_typed_optional(optional: Box<YdbOptional>) -> Result<ydb::TypedValue> {
        if let YdbValue::Optional(_opt) = optional.t {
            unimplemented!("nested optional")
        }

        let val = match optional.value {
            Some(val) => val.to_typed_value()?.value.unwrap(),
            None => ydb::Value {
                value: Some(ydb::value::Value::NullFlagValue(0)),
                ..ydb::Value::default()
            },
        };
        Ok(ydb::TypedValue {
            r#type: Some(ydb::Type {
                r#type: Some(ydb::r#type::Type::OptionalType(Box::new(
                    ydb::OptionalType {
                        item: Some(Box::new(optional.t.to_typed_value()?.r#type.unwrap())),
                    },
                ))),
            }),
            value: Some(val),
        })
    }

    fn to_typed_struct(s: YdbStruct) -> Result<ydb::TypedValue> {
        let mut members: Vec<ydb::StructMember> = Vec::with_capacity(s.fields_name.len());
        let mut items: Vec<ydb::Value> = Vec::with_capacity(s.fields_name.len());
        for (index, v) in s.values.into_iter().enumerate() {
            let typed_val = v.to_typed_value()?;
            members.push(ydb::StructMember {
                name: s.fields_name[index].clone(),
                r#type: typed_val.r#type,
            });
            items.push(typed_val.value.unwrap());
        }

        return Ok(ydb::TypedValue {
            r#type: Some(ydb::Type {
                r#type: Some(ydb::r#type::Type::StructType(ydb::StructType { members })),
            }),
            value: Some(ydb::Value {
                items,
                ..ydb::Value::default()
            }),
        });
    }

    fn to_typed_value_list(ydb_list: Box<YdbList>) -> Result<ydb::TypedValue> {
        let ydb_list_type = ydb_list.t;
        let proto_items_result: Vec<Result<ydb::TypedValue>> = ydb_list
            .values
            .into_iter()
            .map(|item| item.to_typed_value())
            .collect();

        let mut proto_items = Vec::with_capacity(proto_items_result.len());
        for item in proto_items_result.into_iter() {
            proto_items.push(item?);
        }

        Ok(ydb::TypedValue {
            r#type: Some(ydb::Type {
                r#type: Some(ydb::r#type::Type::ListType(Box::new(ydb::ListType {
                    item: Some(Box::new(ydb_list_type.to_typed_value()?.r#type.unwrap())),
                }))),
            }),
            value: Some(ydb::Value {
                items: proto_items
                    .into_iter()
                    .map(|item| item.value.unwrap())
                    .collect(),
                ..ydb::Value::default()
            }),
        })
    }
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub(crate) v_type: YdbValue,
}

#[cfg(test)]
mod test {
    use crate::errors::Result;
    use crate::types::{Sign, SignedInterval, YdbStruct, YdbValue};
    use std::collections::HashSet;

    use std::time::Duration;
    use strum::IntoEnumIterator;

    #[test]
    fn serialize() -> Result<()> {
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
        let mut values = vec![
            YdbValue::Bool(false),
            YdbValue::Bool(true),
            YdbValue::String(Vec::from("asd")),
            YdbValue::Utf8("asd".into()),
            YdbValue::Utf8("фыв".into()),
            YdbValue::Json("{}".into()),
            YdbValue::JsonDocument("{}".into()),
            YdbValue::Yson("1;2;3;".into()),
        ];

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

        values.push(YdbValue::Void);

        values.push(YdbValue::Date(std::time::Duration::from_secs(1633996800))); //Tue Oct 12 00:00:00 UTC 2021
        values.push(YdbValue::DateTime(std::time::Duration::from_secs(
            1634000523,
        ))); //Tue Oct 12 01:02:03 UTC 2021

        values.push(YdbValue::Timestamp(std::time::Duration::from_micros(
            16340005230000123,
        ))); //Tue Oct 12 00:00:00.000123 UTC 2021

        values.push(YdbValue::Interval(SignedInterval {
            sign: Sign::Plus,
            duration: Duration::from_secs(1),
        })); // 1 second interval

        values.push(YdbValue::Interval(SignedInterval {
            sign: Sign::Minus,
            duration: Duration::from_secs(1),
        })); // -1 second interval

        values.push(YdbValue::optional_from(YdbValue::Int8(0), None)?);
        values.push(YdbValue::optional_from(
            YdbValue::Int8(0),
            Some(YdbValue::Int8(1)),
        )?);

        values.push(YdbValue::list_from(
            YdbValue::Int8(0),
            vec![YdbValue::Int8(1), YdbValue::Int8(2), YdbValue::Int8(3)],
        )?);

        values.push(YdbValue::Struct(YdbStruct {
            fields_name: vec!["a".into(), "b".into()],
            values: vec![
                YdbValue::Int32(1),
                YdbValue::list_from(
                    YdbValue::Int32(0),
                    vec![YdbValue::Int32(1), YdbValue::Int32(2), YdbValue::Int32(3)],
                )?,
            ],
        }));

        for v in values.into_iter() {
            discriminants.insert(std::mem::discriminant(&v));
            let proto = v.clone().to_typed_value()?;
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

        assert_eq!(non_tested.len(), 0, "{:?}", non_tested);

        return Ok(());
    }
}
