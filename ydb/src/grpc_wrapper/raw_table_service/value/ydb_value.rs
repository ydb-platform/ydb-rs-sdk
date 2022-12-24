use itertools::Itertools;
use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_table_service::value::{RawTypedValue, RawValue};
use crate::grpc_wrapper::raw_table_service::value::value_type::{RawType, StructMember, StructType};
use crate::types::SECONDS_PER_DAY;
use crate::Value;

impl TryFrom<crate::Value> for RawTypedValue{
    type Error = RawError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let res = match value {
            Value::Void => RawTypedValue{
                r#type: RawType::Void,
                value: RawValue::NullFlag,
            },
            Value::Null => RawTypedValue{
                r#type: RawType::Null,
                value: RawValue::NullFlag,
            },
            Value::Bool(v) => RawTypedValue{
                r#type: RawType::Bool,
                value: RawValue::Bool(v),
            },
            Value::Int8(v) => RawTypedValue{
                r#type: RawType::Int8,
                value: RawValue::Int32(v as i32),
            },
            Value::Uint8(v) => RawTypedValue{
                r#type: RawType::Uint8,
                value: RawValue::Int32(v as i32),
            },
            Value::Int16(v) => RawTypedValue{
                r#type: RawType::Uint16,
                value: RawValue::Int32(v as i32),
            },
            Value::Uint16(v) => RawTypedValue{
                r#type: RawType::Uint16,
                value: RawValue::Int32(v as i32),
            },
            Value::Int32(v) => RawTypedValue{
                r#type: RawType::Uint32,
                value: RawValue::Int32(v),
            },
            Value::Uint32(v) => RawTypedValue{
                r#type: RawType::Uint32,
                value: RawValue::UInt32(v),
            },
            Value::Int64(v) => RawTypedValue{
                r#type: RawType::Int64,
                value: RawValue::Int64(v),
            },
            Value::Uint64(v) => RawTypedValue{
                r#type: RawType::Uint64,
                value: RawValue::UInt64(v),
            },
            Value::Float(v) => RawTypedValue{
                r#type: RawType::Float,
                value: RawValue::Float(v),
            },
            Value::Double(v) => RawTypedValue{
                r#type: RawType::Double,
                value: RawValue::Double(v),
            },
            Value::Date(v) => RawTypedValue{
                r#type: RawType::Date,
                value: RawValue::UInt32((v.as_secs() / SECONDS_PER_DAY).try_into()?),
            },
            Value::DateTime(v) => RawTypedValue{
                r#type: RawType::DateTime,
                value: RawValue::UInt32(v.as_secs().try_into()?),
            },
            Value::Timestamp(v) => RawTypedValue{
                r#type: RawType::Timestamp,
                value: RawValue::UInt64(v.as_micros().try_into()?),
            },
            Value::Interval(v) => RawTypedValue{
                r#type: RawType::Interval,
                value: RawValue::Int64(v.as_nanos()?),
            },
            Value::String(v) => RawTypedValue{
                r#type: RawType::Bytes,
                value: RawValue::Bytes(v.into()),
            },
            Value::Text(v) => RawTypedValue{
                r#type: RawType::UTF8,
                value: RawValue::Text(v),
            },
            Value::Yson(v) => RawTypedValue{
                r#type: RawType::YSON,
                value: RawValue::Text(v),
            },
            Value::Json(v) => RawTypedValue{
                r#type: RawType::JSON,
                value: RawValue::Text(v),
            },
            Value::JsonDocument(v) => RawTypedValue{
                r#type: RawType::JSONDocument,
                value: RawValue::Text(v),
            },
            Value::Optional(v) => {
                let type_example: RawTypedValue = v.t.try_into()?;
                if let Some(v) = v.value{
                    let value: RawTypedValue = v.try_into()?;
                    RawTypedValue{
                        r#type: RawType::Optional(Box::new(type_example.r#type)),
                        value: value.value,
                    }
                } else {
                    RawTypedValue{
                        r#type: RawType::Optional(Box::new(type_example.r#type)),
                        value: RawValue::NullFlag,
                    }
                }
            },
            Value::List(v) => {
                let type_example: RawTypedValue = v.t.try_into()?;
                let items_res: Result<Vec<RawTypedValue>,_> = v.values.into_iter().map(|item|item.try_into()).collect();

                RawTypedValue{
                    r#type: type_example.r#type,
                    value: RawValue::Items(items_res?.into_iter().map(|item|item.value).collect()),
                }
            },
            Value::Struct(v) => {
                if v.values.len() != v.fields_name.len() {
                    return Err(RawError::custom(format!("struct fields len: {} not equals with values len: {}", v.fields_name.len(), v.values.len())))
                }

                let items_res: Result<Vec<RawTypedValue>, _> = v.values.into_iter().map(|item|RawTypedValue::try_from(item)).collect();
                let items = items_res?;

                let mut raw_members = Vec::with_capacity(items.len());
                let mut raw_items = Vec::with_capacity(items.len());

                for (name, raw_typed_value) in v.fields_name.into_iter().zip_eq(items) {
                    raw_members.push(StructMember{
                        name,
                        member_type: raw_typed_value.r#type,
                    });
                    raw_items.push(raw_typed_value.value);
                }

                RawTypedValue{
                    r#type: RawType::Struct(StructType {
                        members: raw_members,
                    }),
                    value: RawValue::Items(raw_items),
                }
            }
        };
        Ok(res)
    }
}