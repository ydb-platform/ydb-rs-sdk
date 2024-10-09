use std::time::SystemTime;
use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use ydb_grpc::ydb_proto::r#type::{PrimitiveTypeId, Type as ProtoType};
use crate::{Bytes, SignedInterval, Value, ValueList, ValueOptional, ValueStruct};

#[cfg(test)]
#[path = "type_test.rs"]
mod type_test;

#[derive(Clone, Debug, Eq, PartialEq, strum::EnumCount, serde::Serialize)]
pub(crate) enum RawType {
    // Unspecified, skip unspecified type into internal code
    Bool,
    Int8,
    Uint8,
    Int16,
    Uint16,
    Int32,
    Uint32,
    Int64,
    Uint64,
    Float,
    Double,
    Date,
    DateTime,
    Timestamp,
    Interval,
    TzDate,
    TzDatetime,
    TzTimestamp,
    Bytes, // String
    UTF8,
    Yson,
    Json,
    Uuid,
    JSONDocument,
    DyNumber,
    Decimal(DecimalType),
    Optional(Box<RawType>),
    List(Box<RawType>),
    Tuple(TupleType),
    Struct(StructType),
    Dict(Box<DictType>),
    Variant(VariantType),
    Tagged(Box<TaggedType>),
    Void,
    Null,
    EmptyList,
    EmptyDict,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub(crate) struct DecimalType {
    pub precision: u8,
    pub scale: i16,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub(crate) struct TupleType {
    pub elements: Vec<RawType>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub(crate) struct StructType {
    pub members: Vec<StructMember>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub(crate) struct StructMember {
    pub name: String,
    pub member_type: RawType,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub(crate) struct DictType {
    pub key: RawType,
    pub payload: RawType,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub(crate) enum VariantType {
    Tuple(TupleType),
    Struct(StructType),
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub(crate) struct TaggedType {
    pub tag: String,
    pub item_type: RawType,
}

impl RawType {
    fn try_from_primitive_type_id(int_type_id: i32) -> RawResult<Self> {
        let type_id = PrimitiveTypeId::from_i32(int_type_id);
        let type_id = if let Some(type_id) = type_id {
            type_id
        } else {
            return Err(RawError::decode_error(format!(
                "Unexpected primitive type_id: {}",
                int_type_id
            )));
        };

        let res = match type_id {
            PrimitiveTypeId::Unspecified => {
                return Err(RawError::decode_error("got unspecified primitive_type_id"))
            }
            PrimitiveTypeId::Bool => RawType::Bool,
            PrimitiveTypeId::Int8 => RawType::Int8,
            PrimitiveTypeId::Uint8 => RawType::Uint8,
            PrimitiveTypeId::Int16 => RawType::Int16,
            PrimitiveTypeId::Uint16 => RawType::Uint16,
            PrimitiveTypeId::Int32 => RawType::Int32,
            PrimitiveTypeId::Uint32 => RawType::Uint32,
            PrimitiveTypeId::Int64 => RawType::Int64,
            PrimitiveTypeId::Uint64 => RawType::Uint64,
            PrimitiveTypeId::Float => RawType::Float,
            PrimitiveTypeId::Double => RawType::Double,
            PrimitiveTypeId::Date => RawType::Date,
            PrimitiveTypeId::Datetime => RawType::DateTime,
            PrimitiveTypeId::Timestamp => RawType::Timestamp,
            PrimitiveTypeId::Interval => RawType::Interval,
            PrimitiveTypeId::TzDate => RawType::TzDate,
            PrimitiveTypeId::TzDatetime => RawType::TzDatetime,
            PrimitiveTypeId::TzTimestamp => RawType::TzTimestamp,
            PrimitiveTypeId::String => RawType::Bytes,
            PrimitiveTypeId::Utf8 => RawType::UTF8,
            PrimitiveTypeId::Yson => RawType::Yson,
            PrimitiveTypeId::Json => RawType::Json,
            PrimitiveTypeId::Uuid => RawType::Uuid,
            PrimitiveTypeId::JsonDocument => RawType::JSONDocument,
            PrimitiveTypeId::Dynumber => RawType::DyNumber,
        };

        Ok(res)
    }

    pub fn into_value_example(self) ->RawResult<Value>{
        fn unimplemented_type(t: RawType)->RawResult<Value>{
            Err(RawError::custom(format!("unimplemented example value for type: {:?}", t)))
        }

        let res = match self {
            RawType::Bool => Value::Bool(false),
            RawType::Int8 => Value::Int8(0),
            RawType::Uint8 => Value::Uint8(0),
            RawType::Int16 => Value::Int16(0),
            RawType::Uint16 => Value::Uint16(0),
            RawType::Int32 => Value::Int32(0),
            RawType::Uint32 => Value::Uint32(0),
            RawType::Int64 => Value::Int64(0),
            RawType::Uint64 => Value::Uint64(0),
            RawType::Float => Value::Float(0.0),
            RawType::Double => Value::Double(0.0),
            RawType::Date => Value::Date(SystemTime::UNIX_EPOCH),
            RawType::DateTime => Value::DateTime(SystemTime::UNIX_EPOCH),
            RawType::Timestamp => Value::Timestamp(SystemTime::UNIX_EPOCH),
            RawType::Interval => Value::Interval(SignedInterval::default()),
            t @ RawType::TzDate => return unimplemented_type(t),
            t@RawType::TzDatetime => return unimplemented_type(t),
            t@RawType::TzTimestamp => return unimplemented_type(t),
            RawType::Bytes => Value::Bytes(Bytes::default()),
            RawType::UTF8 => Value::Text(String::default()),
            RawType::Yson => Value::Yson(Bytes::default()),
            RawType::Json => Value::Json(String::default()),
            t @ RawType::Uuid => return unimplemented_type(t),
            RawType::JSONDocument => Value::JsonDocument(String::default()),
            t @ RawType::DyNumber => return unimplemented_type(t),
            RawType::Decimal(_) => Value::Decimal(decimal_rs::Decimal::default()),
            RawType::Optional(inner_type) => Value::Optional(Box::new(ValueOptional{
                t: (*inner_type).into_value_example()?,
                value: None,
            })),
            RawType::List(inner_type) => Value::List(Box::new(ValueList{
                t: inner_type.into_value_example()?,
                values: Vec::default(),
            })),
            t @ RawType::Tuple(_) => return unimplemented_type(t),
            RawType::Struct(fields) => {
                let mut value_struct = ValueStruct::with_capacity(fields.members.len());
                for field in fields.members.into_iter() {
                    value_struct.insert(field.name, field.member_type.into_value_example()?)
                }
                Value::Struct(value_struct)
            }
            t@RawType::Dict(_) => return unimplemented_type(t),
            t@RawType::Variant(_) => return unimplemented_type(t),
            t@RawType::Tagged(_) => return unimplemented_type(t),
            t@RawType::Void => return unimplemented_type(t),
            RawType::Null => Value::Null,
            t@RawType::EmptyList => return unimplemented_type(t),
            t@RawType::EmptyDict => return unimplemented_type(t),
        };
        Ok(res)
    }
}

//
// From protobuf to internal
//

impl TryFrom<ydb_grpc::ydb_proto::Type> for RawType {
    type Error = RawError;

    fn try_from(src: ydb_grpc::ydb_proto::Type) -> Result<Self, Self::Error> {
        let t: ProtoType = if let Some(t) = src.r#type {
            t
        } else {
            return decode_err("empty type field in Type message");
        };

        let res: Self = match t {
            ProtoType::TypeId(type_id) => return RawType::try_from_primitive_type_id(type_id),
            ProtoType::DecimalType(decimal) => RawType::Decimal(DecimalType {
                precision: u8::try_from(decimal.precision)?,
                scale: i16::try_from(decimal.scale)?,
            }),
            ProtoType::OptionalType(optional_type) => {
                if let Some(item) = optional_type.item {
                    RawType::Optional(Box::new(RawType::try_from(*item)?))
                } else {
                    return decode_err("empty optional type");
                }
            }
            ProtoType::ListType(list_type) => {
                if let Some(item) = list_type.item {
                    RawType::List(Box::new(RawType::try_from(*item)?))
                } else {
                    return decode_err("empty list type");
                }
            }
            ProtoType::TupleType(tuple_type) => RawType::Tuple(TupleType::try_from(tuple_type)?),
            ProtoType::StructType(struct_type) => {
                RawType::Struct(StructType::try_from(struct_type)?)
            }
            ProtoType::DictType(dict_type) => {
                let key = if let Some(key) = dict_type.key {
                    key
                } else {
                    return decode_err("empty key type in dict_type");
                };

                let payload = if let Some(payload) = dict_type.payload {
                    payload
                } else {
                    return decode_err("empty payload in dict_type");
                };

                RawType::Dict(Box::new(DictType {
                    key: RawType::try_from(*key)?,
                    payload: RawType::try_from(*payload)?,
                }))
            }
            ProtoType::VariantType(variant_type) => {
                let t = if let Some(t) = variant_type.r#type {
                    t
                } else {
                    return decode_err("empty type in variant_type");
                };

                match t {
                    ydb_grpc::ydb_proto::variant_type::Type::TupleItems(tuple_items) => {
                        RawType::Variant(VariantType::Tuple(TupleType::try_from(tuple_items)?))
                    }

                    ydb_grpc::ydb_proto::variant_type::Type::StructItems(struct_items) => {
                        RawType::Variant(VariantType::Struct(StructType::try_from(struct_items)?))
                    }
                }
            }
            ProtoType::TaggedType(tagged_type) => {
                let t = if let Some(t) = tagged_type.r#type {
                    t
                } else {
                    return decode_err("empty type in tagged_type");
                };

                RawType::Tagged(Box::new(TaggedType {
                    tag: tagged_type.tag,
                    item_type: RawType::try_from(*t)?,
                }))
            }
            ProtoType::PgType(_pg_type) => {
                return decode_err("pg type unimplemented yet");
            }
            ProtoType::VoidType(_) => RawType::Void,
            ProtoType::NullType(_) => RawType::Null,
            ProtoType::EmptyListType(_) => RawType::EmptyList,
            ProtoType::EmptyDictType(_) => RawType::EmptyDict,
        };

        Ok(res)
    }
}

impl TryFrom<ydb_grpc::ydb_proto::StructMember> for StructMember {
    type Error = RawError;

    fn try_from(value: ydb_grpc::ydb_proto::StructMember) -> Result<Self, Self::Error> {
        let res = if let Some(t) = value.r#type {
            StructMember {
                name: value.name,
                member_type: RawType::try_from(t)?,
            }
        } else {
            return decode_err("struct member type empty");
        };

        Ok(res)
    }
}

impl TryFrom<ydb_grpc::ydb_proto::StructType> for StructType {
    type Error = RawError;

    fn try_from(value: ydb_grpc::ydb_proto::StructType) -> Result<Self, Self::Error> {
        let results: Result<Vec<_>, _> = value
            .members
            .into_iter()
            .map(StructMember::try_from)
            .collect();

        Ok(StructType { members: results? })
    }
}

impl TryFrom<ydb_grpc::ydb_proto::TupleType> for TupleType {
    type Error = RawError;

    fn try_from(value: ydb_grpc::ydb_proto::TupleType) -> Result<Self, Self::Error> {
        let results: Result<Vec<_>, _> = value
            .elements
            .into_iter()
            .map(RawType::try_from)
            .collect();

        Ok(TupleType { elements: results? })
    }
}

//
// From internal to protobuf
//

impl From<RawType> for ydb_grpc::ydb_proto::Type {
    fn from(v: RawType) -> Self {
        let t: ydb_grpc::ydb_proto::r#type::Type = match v {
            RawType::Bool => ProtoType::TypeId(PrimitiveTypeId::Bool as i32),
            RawType::Int8 => ProtoType::TypeId(PrimitiveTypeId::Int8 as i32),
            RawType::Uint8 => ProtoType::TypeId(PrimitiveTypeId::Uint8 as i32),
            RawType::Int16 => ProtoType::TypeId(PrimitiveTypeId::Int16 as i32),
            RawType::Uint16 => ProtoType::TypeId(PrimitiveTypeId::Uint16 as i32),
            RawType::Int32 => ProtoType::TypeId(PrimitiveTypeId::Int32 as i32),
            RawType::Uint32 => ProtoType::TypeId(PrimitiveTypeId::Uint32 as i32),
            RawType::Int64 => ProtoType::TypeId(PrimitiveTypeId::Int64 as i32),
            RawType::Uint64 => ProtoType::TypeId(PrimitiveTypeId::Uint64 as i32),
            RawType::Float => ProtoType::TypeId(PrimitiveTypeId::Float as i32),
            RawType::Double => ProtoType::TypeId(PrimitiveTypeId::Double as i32),
            RawType::Date => ProtoType::TypeId(PrimitiveTypeId::Date as i32),
            RawType::DateTime => ProtoType::TypeId(PrimitiveTypeId::Datetime as i32),
            RawType::Timestamp => ProtoType::TypeId(PrimitiveTypeId::Timestamp as i32),
            RawType::Interval => ProtoType::TypeId(PrimitiveTypeId::Interval as i32),
            RawType::TzDate => ProtoType::TypeId(PrimitiveTypeId::TzDate as i32),
            RawType::TzDatetime => ProtoType::TypeId(PrimitiveTypeId::TzDatetime as i32),
            RawType::TzTimestamp => ProtoType::TypeId(PrimitiveTypeId::TzTimestamp as i32),
            RawType::Bytes => ProtoType::TypeId(PrimitiveTypeId::String as i32),
            RawType::UTF8 => ProtoType::TypeId(PrimitiveTypeId::Utf8 as i32),
            RawType::Yson => ProtoType::TypeId(PrimitiveTypeId::Yson as i32),
            RawType::Json => ProtoType::TypeId(PrimitiveTypeId::Json as i32),
            RawType::Uuid => ProtoType::TypeId(PrimitiveTypeId::Uuid as i32),
            RawType::JSONDocument => ProtoType::TypeId(PrimitiveTypeId::JsonDocument as i32),
            RawType::DyNumber => ProtoType::TypeId(PrimitiveTypeId::Dynumber as i32),
            RawType::Decimal(decimal) => ProtoType::DecimalType(ydb_grpc::ydb_proto::DecimalType {
                precision: decimal.precision as u32,
                scale: decimal.scale as u32,
            }),
            RawType::Optional(nested) => {
                ProtoType::OptionalType(Box::new(ydb_grpc::ydb_proto::OptionalType {
                    item: Some(Box::new((*nested).into())),
                }))
            }
            RawType::List(item_type) => {
                ProtoType::ListType(Box::new(ydb_grpc::ydb_proto::ListType {
                    item: Some(Box::new((*item_type).into())),
                }))
            }
            RawType::Tuple(tuple) => ProtoType::TupleType(tuple.into()),
            RawType::Struct(struct_t) => ProtoType::StructType(struct_t.into()),
            RawType::Dict(dict) => ProtoType::DictType(Box::new(ydb_grpc::ydb_proto::DictType {
                key: Some(Box::new(dict.key.into())),
                payload: Some(Box::new(dict.payload.into())),
            })),
            RawType::Variant(variant) => {
                let res = match variant {
                    VariantType::Tuple(tuple) => {
                        ydb_grpc::ydb_proto::variant_type::Type::TupleItems(tuple.into())
                    }
                    VariantType::Struct(struct_t) => {
                        ydb_grpc::ydb_proto::variant_type::Type::StructItems(struct_t.into())
                    }
                };

                ProtoType::VariantType(ydb_grpc::ydb_proto::VariantType { r#type: Some(res) })
            }
            RawType::Tagged(tagged) => {
                ProtoType::TaggedType(Box::new(ydb_grpc::ydb_proto::TaggedType {
                    tag: tagged.tag,
                    r#type: Some(Box::new(tagged.item_type.into())),
                }))
            }
            RawType::Void => ProtoType::VoidType(0),
            RawType::Null => ProtoType::NullType(0),
            RawType::EmptyList => ProtoType::EmptyListType(0),
            RawType::EmptyDict => ProtoType::EmptyDictType(0),
        };

        Self { r#type: Some(t) }
    }
}

impl From<TupleType> for ydb_grpc::ydb_proto::TupleType {
    fn from(v: TupleType) -> Self {
        Self {
            elements: v.elements.into_iter().map(|item| item.into()).collect(),
        }
    }
}

impl From<StructType> for ydb_grpc::ydb_proto::StructType {
    fn from(v: StructType) -> Self {
        Self {
            members: v.members.into_iter().map(|item| item.into()).collect(),
        }
    }
}

impl From<StructMember> for ydb_grpc::ydb_proto::StructMember {
    fn from(v: StructMember) -> Self {
        Self {
            name: v.name,
            r#type: Some(v.member_type.into()),
        }
    }
}

pub(super) fn decode_err<S: Into<String>, V>(text: S) -> Result<V, RawError> {
    Err(RawError::decode_error(text))
}
