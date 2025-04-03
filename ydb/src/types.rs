use crate::errors::{YdbError, YdbResult};
use std::collections::HashMap;

use crate::grpc_wrapper::raw_table_service::value::r#type::RawType;
use crate::grpc_wrapper::raw_table_service::value::RawColumn;
use std::convert::TryInto;
use std::fmt::Debug;
use std::num::TryFromIntError;
use std::time::{Duration, SystemTime};
use strum::{EnumCount, EnumDiscriminants, EnumIter, IntoStaticStr};
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
#[derive(Clone, Debug, EnumCount, EnumDiscriminants, PartialEq)]
#[strum_discriminants(vis(pub(crate)))] // private
#[strum_discriminants(derive(IntoStaticStr, EnumIter, Hash))]
#[strum_discriminants(name(ValueDiscriminants))]
#[allow(dead_code)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
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
    Date(std::time::SystemTime),
    DateTime(std::time::SystemTime),
    Timestamp(std::time::SystemTime),
    Interval(SignedInterval),

    // It named String at server, but server String type contains binary data https://ydb.tech/docs/en/yql/reference/types/primitive#string
    Bytes(Bytes),

    /// Text data, encoded to valid utf8
    Text(String),
    Yson(Bytes),
    Json(String),
    JsonDocument(String),

    Optional(Box<ValueOptional>),
    List(Box<ValueList>),
    Struct(ValueStruct),

    Decimal(decimal_rs::Decimal),
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

    pub(crate) fn from_fields(fields: Vec<(String, Value)>) -> ValueStruct {
        let fields_len = fields.len();
        let (names, values) = fields.into_iter().fold(
            (
                Vec::with_capacity(fields_len),
                Vec::with_capacity(fields_len),
            ),
            |(mut names, mut values), (name, value)| {
                names.push(name);
                values.push(value);
                (names, values)
            },
        );

        ValueStruct {
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

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum Sign {
    #[default]
    Plus,
    Minus,
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
    /// list_from create Value from example of item and values
    /// example value must be same type as items in value
    /// it used for describe type in query.
    ///
    /// It can't use one of values because values can be empty.
    /// Example:
    /// ```
    ///  # use ydb::{Value, YdbResult};
    ///  # fn example() -> YdbResult<()>{
    ///  let v = Value::list_from(0.into(), vec![1.into(), 2.into(), 3.into()])?;
    ///  # Ok(())
    /// }
    /// ```
    pub fn list_from(example_value: Value, values: Vec<Value>) -> YdbResult<Self> {
        for (index, value) in values.iter().enumerate() {
            if std::mem::discriminant(&example_value) != std::mem::discriminant(value) {
                return Err(YdbError::Custom(format!("failed list_from: type and value has different enum-types. index: {}, type: '{:?}', value: '{:?}'", index, example_value, value)));
            }
        }

        Ok(Value::List(Box::new(ValueList {
            t: example_value,
            values,
        })))
    }

    pub(crate) fn optional_from(t: Value, value: Option<Value>) -> YdbResult<Self> {
        if let Some(value) = &value {
            if std::mem::discriminant(&t) != std::mem::discriminant(value) {
                return Err(YdbError::Custom(format!("failed optional_from: type and value has different enum-types. type: '{:?}', value: '{:?}'", t, value)));
            }
        }
        Ok(Value::Optional(Box::new(ValueOptional { t, value })))
    }

    /// Create struct value from fields in form name, value.
    ///
    /// Example:
    /// ```
    /// # use ydb::Value;
    /// let v = Value::struct_from_fields(vec![
    ///     ("id".to_string(), 1.into()),
    ///     ("value".to_string(), "test-value".into()),
    /// ]);
    /// ```
    pub fn struct_from_fields(fields: Vec<(String, Value)>) -> Value {
        Value::Struct(ValueStruct::from_fields(fields))
    }

    ///  Return true if the Value is optional
    pub fn is_optional(&self) -> bool {
        matches!(self, Self::Optional(_))
    }

    /// present current value as Option
    /// if value is Optional - return inner unwrapper value.
    /// else - return self, wrapped to Option.
    pub fn to_option(self) -> Option<Value> {
        match self {
            Value::Optional(inner_box) => inner_box.value,
            other => Some(other),
        }
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
                pv::Uint32Value(
                    (val.duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                        / SECONDS_PER_DAY)
                        .try_into()?,
                ),
            ),
            Self::DateTime(val) => proto_typed_value(
                pt::Datetime,
                pv::Uint32Value(
                    val.duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                        .try_into()?,
                ),
            ),
            Self::Timestamp(val) => proto_typed_value(
                pt::Timestamp,
                pv::Uint64Value(
                    val.duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_micros()
                        .try_into()?,
                ),
            ),
            Self::Interval(val) => proto_typed_value(pt::Interval, pv::Int64Value(val.as_nanos()?)),
            Self::Bytes(val) => proto_typed_value(pt::String, pv::BytesValue(val.into())),
            Self::Text(val) => proto_typed_value(pt::Utf8, pv::TextValue(val)),
            Self::Yson(val) => proto_typed_value(pt::Yson, pv::BytesValue(val.into())),
            Self::Json(val) => proto_typed_value(pt::Json, pv::TextValue(val)),
            Self::JsonDocument(val) => proto_typed_value(pt::JsonDocument, pv::TextValue(val)),
            Self::Optional(val) => Self::to_typed_optional(*val)?,
            Self::List(items) => Self::to_typed_value_list(*items)?,
            Value::Struct(s) => { Self::to_typed_struct(s) }?,
            Self::Decimal(val) => Self::to_typed_decimal(val)?,
        };
        Ok(res)
    }

    fn to_typed_decimal(val: decimal_rs::Decimal) -> YdbResult<ydb_proto::TypedValue> {
        Ok(ydb_proto::TypedValue {
            r#type: Some(ydb_proto::Type {
                r#type: Some(ydb_proto::r#type::Type::DecimalType(
                    ydb_proto::DecimalType {
                        precision: val.precision().into(),
                        scale: val.scale().try_into()?,
                    },
                )),
            }),
            value: Some(ydb_proto::Value {
                value: Some(ydb_proto::value::Value::TextValue(val.to_string())),
                ..ydb_proto::Value::default()
            }),
        })
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

    #[cfg(test)]
    pub(crate) fn examples_for_test() -> Vec<Value> {
        use std::{collections::HashSet, ops::Add};

        // test zero, one, minimum and maximum values
        macro_rules! num_tests {
            ($values:ident, $en_name:path, $type_name:ty) => {
                $values.push($en_name(0 as $type_name));
                $values.push($en_name(1 as $type_name));
                $values.push($en_name(<$type_name>::MIN));
                $values.push($en_name(<$type_name>::MAX));
            };
        }

        let mut values = vec![
            Value::Null,
            Value::Bool(false),
            Value::Bool(true),
            Value::Bytes(Bytes::from("asd".to_string())),
            Value::Text("asd".into()),
            Value::Text("фыв".into()),
            Value::Json("{}".into()),
            Value::JsonDocument("{}".into()),
            Value::Yson("1;2;3;".into()),
            Value::Decimal(
                "123456789.987654321"
                    .parse::<decimal_rs::Decimal>()
                    .unwrap(),
            ),
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

        values.push(Value::Date(
            SystemTime::UNIX_EPOCH.add(std::time::Duration::from_secs(1633996800)),
        )); //Tue Oct 12 00:00:00 UTC 2021
        values.push(Value::DateTime(
            SystemTime::UNIX_EPOCH.add(std::time::Duration::from_secs(1634000523)),
        )); //Tue Oct 12 01:02:03 UTC 2021

        values.push(Value::Timestamp(
            SystemTime::UNIX_EPOCH.add(std::time::Duration::from_micros(16340005230000123)),
        )); //Tue Oct 12 00:00:00.000123 UTC 2021

        values.push(Value::Interval(SignedInterval {
            sign: Sign::Plus,
            duration: Duration::from_secs(1),
        })); // 1 second interval

        values.push(Value::Interval(SignedInterval {
            sign: Sign::Minus,
            duration: Duration::from_secs(1),
        })); // -1 second interval

        values.push(Value::optional_from(Value::Int8(0), None).unwrap());
        values.push(Value::optional_from(Value::Int8(0), Some(Value::Int8(1))).unwrap());

        values.push(
            Value::list_from(
                Value::Int8(0),
                vec![Value::Int8(1), Value::Int8(2), Value::Int8(3)],
            )
            .unwrap(),
        );

        values.push(Value::Struct(ValueStruct {
            fields_name: vec!["a".into(), "b".into()],
            values: vec![
                Value::Int32(1),
                Value::list_from(
                    Value::Int32(0),
                    vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)],
                )
                .unwrap(),
            ],
        }));

        let mut discriminants = HashSet::new();
        for item in values.iter() {
            discriminants.insert(std::mem::discriminant(item));
        }
        assert_eq!(discriminants.len(), Value::COUNT);

        values
    }
}

#[derive(Debug)]
pub(crate) struct Column {
    #[allow(dead_code)]
    pub(crate) name: String,
    pub(crate) v_type: RawType,
}

impl TryFrom<RawColumn> for Column {
    type Error = YdbError;

    fn try_from(value: RawColumn) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name,
            v_type: value.column_type,
        })
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

impl From<&str> for Bytes {
    fn from(val: &str) -> Self {
        Self { vec: val.into() }
    }
}
