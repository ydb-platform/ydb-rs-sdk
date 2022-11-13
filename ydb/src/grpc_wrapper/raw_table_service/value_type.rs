use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use ydb_grpc::ydb_proto::r#type::{PrimitiveTypeId, Type as ProtoType};

#[cfg(test)]
#[path = "value_type_test.rs"]
mod value_type_test;

#[derive(Clone, Debug, Eq, PartialEq, strum::EnumCount)]
pub(crate) enum Type {
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
    YSON,
    JSON,
    UUID,
    JSONDocument,
    DyNumber,
    Decimal(DecimalType),
    Optional(Box<Type>),
    List(Box<Type>),
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DecimalType {
    pub precision: u32,
    pub scale: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TupleType {
    pub elements: Vec<Type>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StructType {
    pub members: Vec<StructMember>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StructMember {
    pub name: String,
    pub member_type: Type,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DictType {
    pub key: Type,
    pub payload: Type,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum VariantType {
    Tuple(TupleType),
    Struct(StructType),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TaggedType {
    pub tag: String,
    pub item_type: Type,
}

impl Type {
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
            PrimitiveTypeId::Bool => Type::Bool,
            PrimitiveTypeId::Int8 => Type::Int8,
            PrimitiveTypeId::Uint8 => Type::Uint8,
            PrimitiveTypeId::Int16 => Type::Int16,
            PrimitiveTypeId::Uint16 => Type::Uint16,
            PrimitiveTypeId::Int32 => Type::Int32,
            PrimitiveTypeId::Uint32 => Type::Uint32,
            PrimitiveTypeId::Int64 => Type::Int64,
            PrimitiveTypeId::Uint64 => Type::Uint64,
            PrimitiveTypeId::Float => Type::Float,
            PrimitiveTypeId::Double => Type::Double,
            PrimitiveTypeId::Date => Type::Date,
            PrimitiveTypeId::Datetime => Type::DateTime,
            PrimitiveTypeId::Timestamp => Type::Timestamp,
            PrimitiveTypeId::Interval => Type::Interval,
            PrimitiveTypeId::TzDate => Type::TzDate,
            PrimitiveTypeId::TzDatetime => Type::TzDatetime,
            PrimitiveTypeId::TzTimestamp => Type::TzTimestamp,
            PrimitiveTypeId::String => Type::Bytes,
            PrimitiveTypeId::Utf8 => Type::UTF8,
            PrimitiveTypeId::Yson => Type::YSON,
            PrimitiveTypeId::Json => Type::JSON,
            PrimitiveTypeId::Uuid => Type::UUID,
            PrimitiveTypeId::JsonDocument => Type::JSONDocument,
            PrimitiveTypeId::Dynumber => Type::DyNumber,
        };

        Ok(res)
    }
}

//
// From protobuf to internal
//

impl TryFrom<ydb_grpc::ydb_proto::Type> for Type {
    type Error = RawError;

    fn try_from(src: ydb_grpc::ydb_proto::Type) -> Result<Self, Self::Error> {
        let t: ProtoType = if let Some(t) = src.r#type {
            t
        } else {
            return decode_err("empty type field in Type message");
        };

        let res: Self = match t {
            ProtoType::TypeId(type_id) => return Type::try_from_primitive_type_id(type_id),
            ProtoType::DecimalType(decimal) => Type::Decimal(DecimalType {
                precision: decimal.precision,
                scale: decimal.scale,
            }),
            ProtoType::OptionalType(optional_type) => {
                if let Some(item) = optional_type.item {
                    Type::Optional(Box::new(Type::try_from(*item)?))
                } else {
                    return decode_err("empty optional type");
                }
            }
            ProtoType::ListType(list_type) => {
                if let Some(item) = list_type.item {
                    Type::List(Box::new(Type::try_from(*item)?))
                } else {
                    return decode_err("empty list type");
                }
            }
            ProtoType::TupleType(tuple_type) => Type::Tuple(TupleType::try_from(tuple_type)?),
            ProtoType::StructType(struct_type) => Type::Struct(StructType::try_from(struct_type)?),
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

                Type::Dict(Box::new(DictType {
                    key: Type::try_from(*key)?,
                    payload: Type::try_from(*payload)?,
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
                        Type::Variant(VariantType::Tuple(TupleType::try_from(tuple_items)?))
                    }

                    ydb_grpc::ydb_proto::variant_type::Type::StructItems(struct_items) => {
                        Type::Variant(VariantType::Struct(StructType::try_from(struct_items)?))
                    }
                }
            }
            ProtoType::TaggedType(tagged_type) => {
                let t = if let Some(t) = tagged_type.r#type {
                    t
                } else {
                    return decode_err("empty type in tagged_type");
                };

                Type::Tagged(Box::new(TaggedType {
                    tag: tagged_type.tag,
                    item_type: Type::try_from(*t)?,
                }))
            }
            ProtoType::VoidType(_) => Type::Void,
            ProtoType::NullType(_) => Type::Null,
            ProtoType::EmptyListType(_) => Type::EmptyList,
            ProtoType::EmptyDictType(_) => Type::EmptyDict,
        };

        return Ok(res);
    }
}

impl TryFrom<ydb_grpc::ydb_proto::StructMember> for StructMember {
    type Error = RawError;

    fn try_from(value: ydb_grpc::ydb_proto::StructMember) -> Result<Self, Self::Error> {
        let res = if let Some(t) = value.r#type {
            StructMember {
                name: value.name,
                member_type: Type::try_from(t)?,
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
            .map(|item| StructMember::try_from(item))
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
            .map(|item| Type::try_from(item))
            .collect();

        Ok(TupleType { elements: results? })
    }
}

//
// From internal to protobuf
//

impl From<Type> for ydb_grpc::ydb_proto::Type {
    fn from(v: Type) -> Self {
        let t: ydb_grpc::ydb_proto::r#type::Type = match v {
            Type::Bool => ProtoType::TypeId(PrimitiveTypeId::Bool as i32),
            Type::Int8 => ProtoType::TypeId(PrimitiveTypeId::Int8 as i32),
            Type::Uint8 => ProtoType::TypeId(PrimitiveTypeId::Uint8 as i32),
            Type::Int16 => ProtoType::TypeId(PrimitiveTypeId::Int16 as i32),
            Type::Uint16 => ProtoType::TypeId(PrimitiveTypeId::Uint16 as i32),
            Type::Int32 => ProtoType::TypeId(PrimitiveTypeId::Int32 as i32),
            Type::Uint32 => ProtoType::TypeId(PrimitiveTypeId::Uint32 as i32),
            Type::Int64 => ProtoType::TypeId(PrimitiveTypeId::Int64 as i32),
            Type::Uint64 => ProtoType::TypeId(PrimitiveTypeId::Uint64 as i32),
            Type::Float => ProtoType::TypeId(PrimitiveTypeId::Float as i32),
            Type::Double => ProtoType::TypeId(PrimitiveTypeId::Double as i32),
            Type::Date => ProtoType::TypeId(PrimitiveTypeId::Date as i32),
            Type::DateTime => ProtoType::TypeId(PrimitiveTypeId::Datetime as i32),
            Type::Timestamp => ProtoType::TypeId(PrimitiveTypeId::Timestamp as i32),
            Type::Interval => ProtoType::TypeId(PrimitiveTypeId::Interval as i32),
            Type::TzDate => ProtoType::TypeId(PrimitiveTypeId::TzDate as i32),
            Type::TzDatetime => ProtoType::TypeId(PrimitiveTypeId::TzDatetime as i32),
            Type::TzTimestamp => ProtoType::TypeId(PrimitiveTypeId::TzTimestamp as i32),
            Type::Bytes => ProtoType::TypeId(PrimitiveTypeId::String as i32),
            Type::UTF8 => ProtoType::TypeId(PrimitiveTypeId::Utf8 as i32),
            Type::YSON => ProtoType::TypeId(PrimitiveTypeId::Yson as i32),
            Type::JSON => ProtoType::TypeId(PrimitiveTypeId::Json as i32),
            Type::UUID => ProtoType::TypeId(PrimitiveTypeId::Uuid as i32),
            Type::JSONDocument => ProtoType::TypeId(PrimitiveTypeId::JsonDocument as i32),
            Type::DyNumber => ProtoType::TypeId(PrimitiveTypeId::Dynumber as i32),
            Type::Decimal(decimal) => ProtoType::DecimalType(ydb_grpc::ydb_proto::DecimalType {
                precision: decimal.precision,
                scale: decimal.scale,
            }),
            Type::Optional(nested) => {
                ProtoType::OptionalType(Box::new(ydb_grpc::ydb_proto::OptionalType {
                    item: Some(Box::new((*nested).into())),
                }))
            }
            Type::List(item_type) => ProtoType::ListType(Box::new(ydb_grpc::ydb_proto::ListType {
                item: Some(Box::new((*item_type).into())),
            })),
            Type::Tuple(tuple) => ProtoType::TupleType(tuple.into()),
            Type::Struct(struct_t) => ProtoType::StructType(struct_t.into()),
            Type::Dict(dict) => ProtoType::DictType(Box::new(ydb_grpc::ydb_proto::DictType {
                key: Some(Box::new(dict.key.into())),
                payload: Some(Box::new(dict.payload.into())),
            })),
            Type::Variant(variant) => {
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
            Type::Tagged(tagged) => {
                ProtoType::TaggedType(Box::new(ydb_grpc::ydb_proto::TaggedType {
                    tag: tagged.tag,
                    r#type: Some(Box::new(tagged.item_type.into())),
                }))
            }
            Type::Void => ProtoType::VoidType(0),
            Type::Null => ProtoType::NullType(0),
            Type::EmptyList => ProtoType::EmptyListType(0),
            Type::EmptyDict => ProtoType::EmptyDictType(0),
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

fn decode_err<S: Into<String>, V>(text: S) -> Result<V, RawError> {
    Err(RawError::decode_error(text))
}
