#[cfg(test)]
#[path = "value_ydb_test.rs"]
mod value_ydb_test;

use std::time::{Duration, SystemTime};

use itertools::Itertools;

use crate::{Bytes, SignedInterval, Value};
use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::grpc_wrapper::raw_table_service::value::{RawTypedValue, RawValue};
use crate::grpc_wrapper::raw_table_service::value::r#type::{RawType, StructMember, StructType};
use crate::types::SECONDS_PER_DAY;
use super::r#type::DecimalType;
impl TryFrom<crate::Value> for RawTypedValue {
    type Error = RawError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let res = match value {
            Value::Void => RawTypedValue {
                r#type: RawType::Void,
                value: RawValue::NullFlag,
            },
            Value::Null => RawTypedValue {
                r#type: RawType::Null,
                value: RawValue::NullFlag,
            },
            Value::Bool(v) => RawTypedValue {
                r#type: RawType::Bool,
                value: RawValue::Bool(v),
            },
            Value::Int8(v) => RawTypedValue {
                r#type: RawType::Int8,
                value: RawValue::Int32(v as i32),
            },
            Value::Uint8(v) => RawTypedValue {
                r#type: RawType::Uint8,
                value: RawValue::Int32(v as i32),
            },
            Value::Int16(v) => RawTypedValue {
                r#type: RawType::Int16,
                value: RawValue::Int32(v as i32),
            },
            Value::Uint16(v) => RawTypedValue {
                r#type: RawType::Uint16,
                value: RawValue::Int32(v as i32),
            },
            Value::Int32(v) => RawTypedValue {
                r#type: RawType::Int32,
                value: RawValue::Int32(v),
            },
            Value::Uint32(v) => RawTypedValue {
                r#type: RawType::Uint32,
                value: RawValue::UInt32(v),
            },
            Value::Int64(v) => RawTypedValue {
                r#type: RawType::Int64,
                value: RawValue::Int64(v),
            },
            Value::Uint64(v) => RawTypedValue {
                r#type: RawType::Uint64,
                value: RawValue::UInt64(v),
            },
            Value::Float(v) => RawTypedValue {
                r#type: RawType::Float,
                value: RawValue::Float(v),
            },
            Value::Double(v) => RawTypedValue {
                r#type: RawType::Double,
                value: RawValue::Double(v),
            },
            Value::Date(v) => RawTypedValue {
                r#type: RawType::Date,
                value: RawValue::UInt32((v.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() / SECONDS_PER_DAY).try_into()?),
            },
            Value::DateTime(v) => RawTypedValue {
                r#type: RawType::DateTime,
                value: RawValue::UInt32(v.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs().try_into()?),
            },
            Value::Timestamp(v) => RawTypedValue {
                r#type: RawType::Timestamp,
                value: RawValue::UInt64(v.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros().try_into()?),
            },
            Value::Interval(v) => RawTypedValue {
                r#type: RawType::Interval,
                value: RawValue::Int64(v.as_nanos()?),
            },
            Value::Bytes(v) => RawTypedValue {
                r#type: RawType::Bytes,
                value: RawValue::Bytes(v.into()),
            },
            Value::Text(v) => RawTypedValue {
                r#type: RawType::UTF8,
                value: RawValue::Text(v),
            },
            Value::Yson(v) => RawTypedValue {
                r#type: RawType::Yson,
                value: RawValue::Bytes(v.into()),
            },
            Value::Json(v) => RawTypedValue {
                r#type: RawType::Json,
                value: RawValue::Text(v),
            },
            Value::JsonDocument(v) => RawTypedValue {
                r#type: RawType::JSONDocument,
                value: RawValue::Text(v),
                
            },
            Value::Decimal(v) => {
                let (int_val, _scale, negative) = v.into_parts();
                let int_value= (if negative { -1 } else { 1 }) * (int_val as i128);
                let (high, low) = split_to_parts(int_value as u128);
               
                RawTypedValue {
                    r#type: RawType::Decimal(DecimalType {
                        precision: v.precision(),
                        scale: v.scale(),
                    }),
                    value: RawValue::HighLow128(high, low),
                }
            },
            Value::Optional(v) => {
                let type_example: RawTypedValue = v.t.try_into()?;
                if let Some(v) = v.value {
                    let value: RawTypedValue = v.try_into()?;
                    RawTypedValue {
                        r#type: RawType::Optional(Box::new(type_example.r#type)),
                        value: value.value,
                    }
                } else {
                    RawTypedValue {
                        r#type: RawType::Optional(Box::new(type_example.r#type)),
                        value: RawValue::NullFlag,
                    }
                }
            }
            Value::List(v) => {
                let type_example: RawTypedValue = v.t.try_into()?;
                let items_res: Result<Vec<RawTypedValue>, _> = v.values.into_iter().map(|item| item.try_into()).collect();

                RawTypedValue {
                    r#type: RawType::List(Box::new(type_example.r#type)) ,
                    value: RawValue::Items(items_res?.into_iter().map(|item| item.value).collect()),
                }
            }
            Value::Struct(v) => {
                if v.values.len() != v.fields_name.len() {
                    return Err(RawError::custom(format!("struct fields len: {} not equals with values len: {}", v.fields_name.len(), v.values.len())));
                }

                let items_res: Result<Vec<RawTypedValue>, _> = v.values.into_iter().map(RawTypedValue::try_from).collect();
                let items = items_res?;

                let mut raw_members = Vec::with_capacity(items.len());
                let mut raw_items = Vec::with_capacity(items.len());

                for (name, raw_typed_value) in v.fields_name.into_iter().zip_eq(items) {
                    raw_members.push(StructMember {
                        name,
                        member_type: raw_typed_value.r#type,
                    });
                    raw_items.push(raw_typed_value.value);
                }

                RawTypedValue {
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

pub(crate) fn split_to_parts(v: u128) -> (u64, u64) {
    let high = (v >> 64) as u64;
    let low = v as u64;
    (high, low)
}

pub(crate) fn merge_parts(high: u64, low: u64) -> u128 {
    (high as u128) << 64 | (low as u128)
}

impl TryFrom<RawTypedValue> for Value {
    type Error = RawError;

    fn try_from(value: RawTypedValue) -> Result<Self, Self::Error> {
        fn types_mismatch(t: RawType, v: RawValue) -> Result<Value, RawError> {
            Err(RawError::custom(format!("unexpected combination of type '{:?}' and value '{:?}'", t, v)))
        }

        fn type_unimplemented(t: RawType) -> Result<Value, RawError> {
            Err(RawError::custom(format!("unimplemented raw to internal rust type conversion for type: {:?}", t)))
        }

        let res = match (value.r#type, value.value) {
            (RawType::Bool, RawValue::Bool(v)) => Value::Bool(v),
            (t @ RawType::Bool, v) => return types_mismatch(t, v),
            (RawType::Int8, RawValue::Int32(v)) => Value::Int8(v.try_into()?),
            (t @ RawType::Int8, v) => return types_mismatch(t, v),
            (RawType::Uint8, RawValue::Int32(v)) => Value::Uint8(v.try_into()?),
            (t @ RawType::Uint8, v) => return types_mismatch(t, v),
            (RawType::Int16, RawValue::Int32(v)) => Value::Int16(v.try_into()?),
            (t @ RawType::Int16, v) => return types_mismatch(t, v),
            (RawType::Uint16, RawValue::Int32(v)) => Value::Uint16(v.try_into()?),
            (t @ RawType::Uint16, v) => return types_mismatch(t, v),
            (RawType::Int32, RawValue::Int32(v)) => Value::Int32(v),
            (t @ RawType::Int32, v) => return types_mismatch(t, v),
            (RawType::Uint32, RawValue::UInt32(v)) => Value::Uint32(v),
            (t @ RawType::Uint32, v) => return types_mismatch(t, v),
            (RawType::Int64, RawValue::Int64(v)) => Value::Int64(v),
            (t @ RawType::Int64, v) => return types_mismatch(t, v),
            (RawType::Uint64, RawValue::UInt64(v)) => Value::Uint64(v),
            (t @ RawType::Uint64, v) => return types_mismatch(t, v),
            (RawType::Float, RawValue::Float(v)) => Value::Float(v),
            (t @ RawType::Float, v) => return types_mismatch(t, v),
            (RawType::Double, RawValue::Double(v)) => Value::Double(v),
            (t @ RawType::Double, v) => return types_mismatch(t, v),
            (RawType::Date, RawValue::UInt32(v)) => Value::Date(SystemTime::UNIX_EPOCH + Duration::from_secs((v as u64) * SECONDS_PER_DAY)),
            (t @ RawType::Date, v) => return types_mismatch(t, v),
            (RawType::DateTime, RawValue::UInt32(v)) => Value::DateTime(SystemTime::UNIX_EPOCH + Duration::from_secs(v.into())),
            (t @ RawType::DateTime, v) => return types_mismatch(t, v),
            (RawType::Timestamp, RawValue::UInt64(v)) => Value::Timestamp(SystemTime::UNIX_EPOCH + Duration::from_micros(v)),
            (t @ RawType::Timestamp, v) => return types_mismatch(t, v),
            (RawType::Interval, RawValue::Int64(v)) => Value::Interval(SignedInterval::from_nanos(v)),
            (t @ RawType::Interval, v) => return types_mismatch(t, v),
            (t @ RawType::TzDate, _) => return type_unimplemented(t),
            (t @ RawType::TzDatetime, _) => return type_unimplemented(t),
            (t @ RawType::TzTimestamp, _) => return type_unimplemented(t),
            (RawType::Bytes, RawValue::Bytes(v)) => Value::Bytes(Bytes::from(v)),
            (t @ RawType::Bytes, v) => return types_mismatch(t, v),
            (RawType::UTF8, RawValue::Text(v)) => Value::Text(v),
            (t @ RawType::UTF8, v) => return types_mismatch(t, v),
            (RawType::Yson, RawValue::Bytes(v)) => Value::Yson(Bytes::from(v)),
            (t @ RawType::Yson, v) => return types_mismatch(t, v),
            (RawType::Json, RawValue::Text(v)) => Value::Json(v),
            (t @ RawType::Json, v) => return types_mismatch(t, v),
            (t @ RawType::Uuid, _) => return type_unimplemented(t),
            (RawType::JSONDocument, RawValue::Text(v)) => Value::JsonDocument(v),
            (t @ RawType::JSONDocument, v) => return types_mismatch(t, v),
            (t @ RawType::DyNumber, _) => return type_unimplemented(t),
            (RawType::Decimal(t), RawValue::HighLow128(high, low)) => {
               
                let int_val = merge_parts(high, low) as i128;

                let value =
                    decimal_rs::Decimal::from_parts(int_val.abs() as u128, t.scale, int_val < 0)
                        .map_err(|e| RawError::decode_error(e.to_string()))?;
                return Ok(Value::Decimal(value));
            }
            (t @ RawType::Decimal(_), v) => return types_mismatch(t, v),
            (RawType::Optional(inner_type), v) => {
                let opt_value: Option<Value> = if let RawValue::NullFlag = v {
                    None
                } else {
                    let val: Value = RawTypedValue {
                        r#type: (*inner_type).clone(),
                        value: v,
                    }.try_into()?;
                    Some(val)
                };

                let type_example: Value = (*inner_type).into_value_example()?;

                match Value::optional_from(type_example, opt_value) {
                    Ok(val) => val,
                    Err(err) => return Err(RawError::custom(
                        format!("can't create optional value from rawtype: {}", err))
                    )
                }
            }
            ( RawType::List(inner_type), v) => {
                let values= match v {
                    RawValue::NullFlag => Vec::default(),
                    RawValue::Items(items)=>{

                        let values_res: Result<Vec<_>,_> = items.into_iter().map(|item| {
                            RawTypedValue{
                                r#type: (*inner_type).clone(),
                                value: item,
                            }.try_into()
                        }).collect();

                        values_res?
                    },
                    _ => return types_mismatch(RawType::List(inner_type), v),
                };
                let type_example = (*inner_type).into_value_example()?;
                match Value::list_from(type_example, values) {
                    Ok(val) => val,
                    Err(err)=>return Err(RawError::custom(
                        format!("can't create list value from rawtype: {}", err))
                    )
                }
            }
            (t @ RawType::Tuple(_), _) => return type_unimplemented(t),
            (RawType::Struct(struct_name_types), RawValue::Items(items)) => {
                if struct_name_types.members.len()  != items.len() {
                    return Err(RawError::custom(format!(
                        "mismatch struct field len description: '{:?}' and value items while decode raw types values with len: {}",
                        struct_name_types,
                        items.len()
                    )))
                };

                let fields: Vec<_> = struct_name_types.members.into_iter().zip_eq(items).map(|(st, item)|{
                    let value_result: RawResult<Value> = RawTypedValue{
                        r#type: st.member_type,
                        value: item,
                    }.try_into();

                    match value_result {
                        Ok(v)=>Ok((st.name, v)),
                        Err(err)=>Err(err)
                    }
                }).try_collect()?;
                Value::struct_from_fields(fields)
            }
            (t @ RawType::Struct(_), v) => return types_mismatch(t, v),
            (t @ RawType::Dict(_), _) => return type_unimplemented(t),
            (t @ RawType::Variant(_), _) => return type_unimplemented(t),
            (t @ RawType::Tagged(_), _) => return type_unimplemented(t),
            ( RawType::Null, RawValue::NullFlag) => Value::Null,
            (t @ RawType::Null, v) => return types_mismatch(t, v),
            (RawType::Void, RawValue::NullFlag) => Value::Void,
            (t @ RawType::Void, v) => return types_mismatch(t, v),
            (t @ RawType::EmptyList, v) => return types_mismatch(t, v),
            (t @ RawType::EmptyDict, v) => return types_mismatch(t, v),
        };
        Ok(res)
    }
}
