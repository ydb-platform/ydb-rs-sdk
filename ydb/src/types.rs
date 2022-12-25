use crate::errors::{YdbError, YdbResult};
use std::collections::HashMap;

use std::convert::TryInto;
use std::fmt::Debug;
use std::num::TryFromIntError;
use std::ops::Deref;
use std::time::Duration;
use strum::{EnumDiscriminants, EnumIter, IntoStaticStr};
use ydb_grpc::ydb_proto;

pub(crate) const SECONDS_PER_DAY: u64 = 60 * 60 * 24;

/// Internal represent database value for send to or received from database.
///
/// That enum will be grow, when add support of new types
///
/// ## Convert from Value to native types
///
/// ### Primitive values
///
/// #### From Value to native types
/// Convert from Value to primitive rust types do by TryFrom trait
/// Try need because Value can contain any DB value and it can' check at compile time.
/// ```rust
/// # use ydb::YdbResult;
/// # fn main()->YdbResult<()> {
/// # use ydb::{Value, YdbError, YdbResult};
///
/// // Simple convert to native type
/// let v: i16 = Value::Int16(123).try_into()?;
/// assert_eq!(123 as i16, v);
///
/// // Simple types can be extended while convert to native type
/// let v: i32 = Value::Int16(123).try_into()?;
/// assert_eq!(123 as i32, v);
/// # return Ok(())
/// # }
/// ```
///
/// #### From native type to Value
/// ```rust
/// # use ydb::YdbResult;
/// # fn main()->YdbResult<()> {
/// # use ydb::{Value, YdbError, YdbResult};
/// // while convert to Value - value internal type exact same as source type - without auto-extended
/// // because real target type doesn't known in compile time
/// let v: Value = (123 as i16).into();
/// assert_eq!(Value::Int16(123), v);
/// # return Ok(())
/// # }
/// ```
///
/// #### Possible native convertions
///
#[derive(Clone, Debug, EnumDiscriminants, EnumIter, PartialEq)]
#[strum_discriminants(vis())] // private
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(dead_code)]
#[non_exhaustive]
pub enum Value {
    Void,
    Null,
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
    DateTime(std::time::Duration), // seconds from UNIX_EPOCH to start of day in UTC.
    Timestamp(std::time::Duration), // seconds from UNIX_EPOCH to start of day in UTC.
    Interval(SignedInterval),

    /// Store native bytes array, similary to binary/blob in other databases. It named string by history reason only.
    /// Use Utf8 type for store text.
    String(Bytes),

    /// Text data, encoded to valid utf8
    Text(String),
    Yson(String),
    Json(String),
    JsonDocument(String),

    Optional(Box<ValueOptional>),
    List(Box<ValueList>),
    Struct(ValueStruct),
}

impl Value {
    pub(crate) fn kind_static(&self) -> &'static str {
        let discriminant: ValueDiscriminants = self.into();
        discriminant.into()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ValueStruct {
    pub(crate) fields_name: Vec<String>,
    pub(crate) values: Vec<Value>,
}

impl ValueStruct {
    pub(crate) fn insert(&mut self, name: String, v: Value) {
        self.fields_name.push(name);
        self.values.push(v);
    }

    pub(crate) fn from_fields(fields: Vec<(String, Value)>)->ValueStruct{
        let fields_len = fields.len();
        let (names, values) = fields.into_iter().fold(
            (Vec::with_capacity(fields_len), Vec::with_capacity(fields_len)),
            |(mut names, mut values), (name, value)| {
                names.push(name);
                values.push(value);
                (names, values)
            });

        ValueStruct{
            fields_name: names,
            values,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn from_names_and_values(
        fields_name: Vec<String>,
        values: Vec<Value>,
    ) -> YdbResult<Self> {
        if fields_name.len() != values.len() {
            return Err(YdbError::Custom(format!("different len fields_name and values. fields_name len: {}, values len: {}. fields_name: {:?}, values: {:?}", fields_name.len(), values.len(), fields_name, values)));
        };

        Ok(ValueStruct {
            fields_name,
            values,
        })
    }

    pub(crate) fn new() -> Self {
        Self::with_capacity(0)
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        ValueStruct {
            fields_name: Vec::with_capacity(capacity),
            values: Vec::with_capacity(capacity),
        }
    }
}

impl Default for ValueStruct {
    fn default() -> Self {
        Self::new()
    }
}

impl From<ValueStruct> for HashMap<String, Value> {
    fn from(mut from_value: ValueStruct) -> Self {
        let mut map = HashMap::with_capacity(from_value.fields_name.len());
        from_value.values.into_iter().rev().for_each(|val| {
            let key = from_value.fields_name.pop().unwrap();
            map.insert(key, val);
        });
        map
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ValueList {
    pub(crate) t: Value,
    pub(crate) values: Vec<Value>,
}

impl Default for Box<ValueList> {
    fn default() -> Self {
        Box::new(ValueList {
            t: Value::Bool(false),
            values: Vec::default(),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ValueOptional {
    pub(crate) t: Value,
    pub(crate) value: Option<Value>,
}

impl Default for Box<ValueOptional> {
    fn default() -> Self {
        Box::new(ValueOptional {
            t: Value::Bool(false),
            value: None,
        })
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Sign {
    Plus,
    Minus,
}

impl Default for Sign {
    fn default() -> Self {
        Sign::Plus
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct SignedInterval {
    pub sign: Sign,
    pub duration: Duration,
}

impl SignedInterval {
    pub(crate) fn as_nanos(self) -> std::result::Result<i64, TryFromIntError> {
        let nanos: i64 = self.duration.as_nanos().try_into()?;
        let res = match self.sign {
            Sign::Plus => nanos,
            Sign::Minus => -nanos,
        };
        Ok(res)
    }

    pub(crate) fn from_nanos(nanos: i64) -> Self {
        let (sign, nanos) = if nanos >= 0 {
            (Sign::Plus, nanos as u64)
        } else {
            (Sign::Minus, (-nanos) as u64)
        };

        Self {
            sign,
            duration: Duration::from_nanos(nanos),
        }
    }
}

impl Value {
    #[allow(dead_code)]
    pub(crate) fn list_from(t: Value, values: Vec<Value>) -> YdbResult<Self> {
        for (index, value) in values.iter().enumerate() {
            if std::mem::discriminant(&t) != std::mem::discriminant(value) {
                return Err(YdbError::Custom(format!("failed list_from: type and value has different enum-types. index: {}, type: '{:?}', value: '{:?}'", index, t, value)));
            }
        }

        Ok(Value::List(Box::new(ValueList { t, values })))
    }

    pub(crate) fn optional_from(t: Value, value: Option<Value>) -> YdbResult<Self> {
        if let Some(value) = &value {
            if std::mem::discriminant(&t) != std::mem::discriminant(value) {
                return Err(YdbError::Custom(format!("failed optional_from: type and value has different enum-types. type: '{:?}', value: '{:?}'", t, value)));
            }
        }
        Ok(Value::Optional(Box::new(ValueOptional { t, value })))
    }

    pub fn struct_from_fields(fields: Vec<(String,Value)>)->Value{
        Value::Struct(ValueStruct::from_fields(fields))
    }

    // return empty value of requested type
    pub(crate) fn from_proto_type(proto_type: &Option<ydb_proto::Type>) -> YdbResult<Self> {
        use ydb_proto::r#type::PrimitiveTypeId as P;
        use ydb_proto::r#type::Type as T;
        let res = if let Some(ydb_proto::Type {
            r#type: Some(t_val),
        }) = proto_type
        {
            match t_val {
                T::TypeId(t_id) => match P::from_i32(*t_id) {
                    Some(P::Bool) => Self::Bool(false),
                    Some(P::String) => Self::String(Bytes::default()),
                    Some(P::Utf8) => Self::Text(String::default()),
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
                    Some(P::Json) => Self::Json(String::default()),
                    Some(P::Yson) => Self::Yson(String::default()),
                    Some(P::JsonDocument) => Self::JsonDocument(String::default()),
                    _ => unimplemented!("{:?} ({})", P::from_i32(*t_id), *t_id),
                },
                T::VoidType(_) => Value::Void,
                T::OptionalType(val) => {
                    let t = if let Some(item) = &val.item {
                        Some(*item.clone())
                    } else {
                        return Err(YdbError::Custom("none item in optional type".into()));
                    };
                    return Self::optional_from(Self::from_proto_type(&t)?, None);
                }
                T::ListType(oblt) => {
                    let item = if let Some(blt) = &oblt.item {
                        Self::from_proto_type(&Some(blt.deref().clone()))?
                    } else {
                        unimplemented!()
                    };
                    Self::List(Box::new(ValueList {
                        t: item,
                        values: Vec::default(),
                    }))
                }
                T::StructType(struct_type) => {
                    let mut s = ValueStruct::with_capacity(struct_type.members.len());
                    for field in &struct_type.members {
                        let t = Self::from_proto_type(&field.r#type)?;
                        s.insert(field.name.clone(), t);
                    }
                    Self::Struct(s)
                }
                T::NullType(_) => Self::Null,
                _ => unimplemented!("{:?}", t_val),
                // think about map to internal types as 1:1
            }
        } else {
            return Err(YdbError::Custom("column type is None".into()));
        };
        Ok(res)
    }

    pub(crate) fn from_proto(t: &Value, proto_value: ydb_proto::Value) -> YdbResult<Self> {
        let res = match (t, proto_value) {
            (Value::Void, _) => Value::Void,
            (
                t,
                ydb_proto::Value {
                    value: Some(val), ..
                },
            ) => Self::from_proto_value(t, val)?,
            (Value::List(item_type_vec), ydb_proto::Value { items, .. }) => {
                let items_type = &item_type_vec.t;
                let mut values = Vec::with_capacity(items.len());
                items.into_iter().try_for_each(|item| {
                    values.push(Self::from_proto(items_type, item)?);
                    YdbResult::<()>::Ok(())
                })?;
                Value::List(Box::new(ValueList {
                    t: items_type.clone(),
                    values,
                }))
            }
            (Value::Struct(struct_t), ydb_proto::Value { items, .. }) => {
                Self::from_proto_struct(struct_t, items)?
            }
            (t, proto_value) => {
                return Err(YdbError::Custom(format!(
                    "unsupported from_proto combination: t: '{:?}', proto_value: '{:?}'",
                    t, proto_value
                )))
            }
        };
        Ok(res)
    }

    fn from_proto_struct(t: &ValueStruct, items: Vec<ydb_proto::Value>) -> YdbResult<Value> {
        if t.fields_name.len() != items.len() {
            return Err(YdbError::Custom(format!(
                "struct description and items has diferrent length. t: {:?}, items: {:?}",
                t, items
            )));
        };

        let mut res = ValueStruct::with_capacity(t.fields_name.len());
        for (index, item) in items.into_iter().enumerate() {
            let v = Value::from_proto(&t.values[index], item)?;
            res.insert(t.fields_name[index].clone(), v);
        }
        Ok(Value::Struct(res))
    }

    fn from_proto_value(t: &Value, v: ydb_proto::value::Value) -> YdbResult<Value> {
        use ydb_proto::value::Value as pv;

        let res = match (t, v) {
            (Value::Bool(_), pv::BoolValue(val)) => Value::Bool(val),
            (Value::Int8(_), pv::Int32Value(val)) => Value::Int8(val.try_into()?),
            (Value::Uint8(_), pv::Uint32Value(val)) => Value::Uint8(val.try_into()?),
            (Value::Int16(_), pv::Int32Value(val)) => Value::Int16(val.try_into()?),
            (Value::Uint16(_), pv::Uint32Value(val)) => Value::Uint16(val.try_into()?),
            (Value::Int32(_), pv::Int32Value(val)) => Value::Int32(val),
            (Value::Uint32(_), pv::Uint32Value(val)) => Value::Uint32(val),
            (Value::Int64(_), pv::Int64Value(val)) => Value::Int64(val),
            (Value::Uint64(_), pv::Uint64Value(val)) => Value::Uint64(val),
            (Value::Float(_), pv::FloatValue(val)) => Value::Float(val),
            (Value::Double(_), pv::DoubleValue(val)) => Value::Double(val),
            (Value::Date(_), pv::Uint32Value(val)) => {
                Value::Date(std::time::Duration::from_secs(SECONDS_PER_DAY * val as u64))
            }
            (Value::DateTime(_), pv::Uint32Value(val)) => {
                Value::DateTime(std::time::Duration::from_secs(val as u64))
            }
            (Value::Timestamp(_), pv::Uint64Value(val)) => {
                Value::Timestamp(Duration::from_micros(val))
            }
            (Value::Interval(_), pv::Int64Value(val)) => {
                Value::Interval(SignedInterval::from_nanos(val))
            }
            (Value::String(_), pv::BytesValue(val)) => Value::String(val.into()),
            (Value::Text(_), pv::TextValue(val)) => Value::Text(val),
            (Value::Yson(_), pv::TextValue(val)) => Value::Yson(val),
            (Value::Json(_), pv::TextValue(val)) => Value::Json(val),
            (Value::JsonDocument(_), pv::TextValue(val)) => Value::JsonDocument(val),
            (Value::Optional(ydb_optional), val) => {
                Self::from_proto_value_optional(ydb_optional, val)?
            }
            (Value::Null, _) => Value::Null,
            (t, val) => {
                return Err(YdbError::Custom(format!(
                    "unexpected from_proto_value. t: '{:?}', val: '{:?}'",
                    t, val
                )))
            }
        };
        Ok(res)
    }

    fn from_proto_value_optional(
        t: &ValueOptional,
        val: ydb_proto::value::Value,
    ) -> YdbResult<Self> {
        use ydb_proto::value::Value as pv;

        let res = match val {
            pv::NullFlagValue(_) => Self::optional_from(t.t.clone(), None)?,
            val => Self::optional_from(t.t.clone(), Some(Self::from_proto_value(&t.t, val)?))?,
        };
        Ok(res)
    }

    #[allow(clippy::wrong_self_convention)]
    pub(crate) fn to_typed_value(self) -> YdbResult<ydb_proto::TypedValue> {
        use ydb_proto::r#type::PrimitiveTypeId as pt;
        use ydb_proto::value::Value as pv;

        fn proto_typed_value(t: pt, v: pv) -> ydb_proto::TypedValue {
            ydb_proto::TypedValue {
                r#type: Some(ydb_proto::Type {
                    r#type: Some(ydb_proto::r#type::Type::TypeId(t.into())),
                }),
                value: Some(ydb_proto::Value {
                    value: Some(v),
                    ..ydb_proto::Value::default()
                }),
            }
        }

        #[allow(unreachable_patterns)]
        let res = match self {
            Self::Void => ydb_proto::TypedValue {
                r#type: Some(ydb_proto::Type {
                    r#type: Some(ydb_proto::r#type::Type::VoidType(
                        prost_types::NullValue::NullValue.into(),
                    )),
                }),
                value: Some(ydb_proto::Value {
                    value: Some(ydb_proto::value::Value::NullFlagValue(
                        prost_types::NullValue::NullValue.into(),
                    )),
                    ..ydb_proto::Value::default()
                }),
            },
            Self::Null => ydb_proto::TypedValue {
                r#type: Some(ydb_proto::Type {
                    r#type: Some(ydb_proto::r#type::Type::NullType(0)),
                }),
                value: Some(ydb_proto::Value {
                    value: Some(ydb_proto::value::Value::NullFlagValue(
                        prost_types::NullValue::NullValue.into(),
                    )),
                    ..ydb_proto::Value::default()
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
            Self::String(val) => proto_typed_value(pt::String, pv::BytesValue(val.into())),
            Self::Text(val) => proto_typed_value(pt::Utf8, pv::TextValue(val)),
            Self::Yson(val) => proto_typed_value(pt::Yson, pv::TextValue(val)),
            Self::Json(val) => proto_typed_value(pt::Json, pv::TextValue(val)),
            Self::JsonDocument(val) => proto_typed_value(pt::JsonDocument, pv::TextValue(val)),
            Self::Optional(val) => Self::to_typed_optional(*val)?,
            Self::List(items) => Self::to_typed_value_list(*items)?,
            Value::Struct(s) => { Self::to_typed_struct(s) }?,
        };
        Ok(res)
    }

    fn to_typed_optional(optional: ValueOptional) -> YdbResult<ydb_proto::TypedValue> {
        if let Value::Optional(_opt) = optional.t {
            unimplemented!("nested optional")
        }

        let val = match optional.value {
            Some(val) => val.to_typed_value()?.value.unwrap(),
            None => ydb_proto::Value {
                value: Some(ydb_proto::value::Value::NullFlagValue(0)),
                ..ydb_proto::Value::default()
            },
        };
        Ok(ydb_proto::TypedValue {
            r#type: Some(ydb_proto::Type {
                r#type: Some(ydb_proto::r#type::Type::OptionalType(Box::new(
                    ydb_proto::OptionalType {
                        item: Some(Box::new(optional.t.to_typed_value()?.r#type.unwrap())),
                    },
                ))),
            }),
            value: Some(val),
        })
    }

    fn to_typed_struct(s: ValueStruct) -> YdbResult<ydb_proto::TypedValue> {
        let mut members: Vec<ydb_proto::StructMember> = Vec::with_capacity(s.fields_name.len());
        let mut items: Vec<ydb_proto::Value> = Vec::with_capacity(s.fields_name.len());
        for (index, v) in s.values.into_iter().enumerate() {
            let typed_val = v.to_typed_value()?;
            members.push(ydb_proto::StructMember {
                name: s.fields_name[index].clone(),
                r#type: typed_val.r#type,
            });
            items.push(typed_val.value.unwrap());
        }

        Ok(ydb_proto::TypedValue {
            r#type: Some(ydb_proto::Type {
                r#type: Some(ydb_proto::r#type::Type::StructType(ydb_proto::StructType {
                    members,
                })),
            }),
            value: Some(ydb_proto::Value {
                items,
                ..ydb_proto::Value::default()
            }),
        })
    }

    #[allow(clippy::boxed_local)]
    fn to_typed_value_list(ydb_list: ValueList) -> YdbResult<ydb_proto::TypedValue> {
        let ydb_list_type = ydb_list.t;
        let proto_items_result: Vec<YdbResult<ydb_proto::TypedValue>> = ydb_list
            .values
            .into_iter()
            .map(|item| item.to_typed_value())
            .collect();

        let mut proto_items = Vec::with_capacity(proto_items_result.len());
        for item in proto_items_result.into_iter() {
            proto_items.push(item?);
        }

        Ok(ydb_proto::TypedValue {
            r#type: Some(ydb_proto::Type {
                r#type: Some(ydb_proto::r#type::Type::ListType(Box::new(
                    ydb_proto::ListType {
                        item: Some(Box::new(ydb_list_type.to_typed_value()?.r#type.unwrap())),
                    },
                ))),
            }),
            value: Some(ydb_proto::Value {
                items: proto_items
                    .into_iter()
                    .map(|item| item.value.unwrap())
                    .collect(),
                ..ydb_proto::Value::default()
            }),
        })
    }
}

#[derive(Debug)]
pub(crate) struct Column {
    pub(crate) name: String,
    pub(crate) v_type: Value,
}

#[cfg(test)]
mod test {
    use crate::errors::YdbResult;
    use crate::types::{Bytes, Sign, SignedInterval, Value, ValueStruct};
    use std::collections::HashSet;

    use std::time::Duration;
    use strum::IntoEnumIterator;

    #[test]
    fn serialize() -> YdbResult<()> {
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
            Value::Null,
            Value::Bool(false),
            Value::Bool(true),
            Value::String(Bytes::from("asd".to_string())),
            Value::Text("asd".into()),
            Value::Text("фыв".into()),
            Value::Json("{}".into()),
            Value::JsonDocument("{}".into()),
            Value::Yson("1;2;3;".into()),
        ];

        num_tests!(values, Value::Int8, i8);
        num_tests!(values, Value::Uint8, u8);
        num_tests!(values, Value::Int16, i16);
        num_tests!(values, Value::Uint16, u16);
        num_tests!(values, Value::Int32, i32);
        num_tests!(values, Value::Uint32, u32);
        num_tests!(values, Value::Int64, i64);
        num_tests!(values, Value::Uint64, u64);
        num_tests!(values, Value::Float, f32);
        num_tests!(values, Value::Double, f64);

        values.push(Value::Void);

        values.push(Value::Date(std::time::Duration::from_secs(1633996800))); //Tue Oct 12 00:00:00 UTC 2021
        values.push(Value::DateTime(std::time::Duration::from_secs(1634000523))); //Tue Oct 12 01:02:03 UTC 2021

        values.push(Value::Timestamp(std::time::Duration::from_micros(
            16340005230000123,
        ))); //Tue Oct 12 00:00:00.000123 UTC 2021

        values.push(Value::Interval(SignedInterval {
            sign: Sign::Plus,
            duration: Duration::from_secs(1),
        })); // 1 second interval

        values.push(Value::Interval(SignedInterval {
            sign: Sign::Minus,
            duration: Duration::from_secs(1),
        })); // -1 second interval

        values.push(Value::optional_from(Value::Int8(0), None)?);
        values.push(Value::optional_from(Value::Int8(0), Some(Value::Int8(1)))?);

        values.push(Value::list_from(
            Value::Int8(0),
            vec![Value::Int8(1), Value::Int8(2), Value::Int8(3)],
        )?);

        values.push(Value::Struct(ValueStruct {
            fields_name: vec!["a".into(), "b".into()],
            values: vec![
                Value::Int32(1),
                Value::list_from(
                    Value::Int32(0),
                    vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)],
                )?,
            ],
        }));

        for v in values.into_iter() {
            discriminants.insert(std::mem::discriminant(&v));
            let proto = v.clone().to_typed_value()?;
            let t = Value::from_proto_type(&proto.r#type)?;
            let v2 = Value::from_proto(&t, proto.value.unwrap())?;
            assert_eq!(&v, &v2);
        }

        let mut non_tested = Vec::new();
        for v in Value::iter() {
            if !discriminants.contains(&std::mem::discriminant(&v)) {
                non_tested.push(format!("{:?}", &v));
            }
        }

        assert_eq!(non_tested.len(), 0, "{:?}", non_tested);

        Ok(())
    }
}

// Container fot bytes for prevent conflict Vec<u8> - List of values u8 or String type (bytes)
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Bytes {
    vec: Vec<u8>,
}

impl From<Vec<u8>> for Bytes {
    fn from(vec: Vec<u8>) -> Self {
        Bytes { vec }
    }
}

impl From<Bytes> for Vec<u8> {
    fn from(val: Bytes) -> Self {
        val.vec
    }
}

impl From<String> for Bytes {
    fn from(val: String) -> Self {
        Self { vec: val.into() }
    }
}
