pub(crate) enum Type {
    Unspecified,
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

pub(crate) struct DecimalType {
    precision: u32,
    scale: u32,
}

pub(crate) struct TupleType {
    elements: Vec<Type>,
}

pub(crate) struct StructType {
    members: Vec<StructMember>,
}

pub(crate) struct StructMember {
    name: String,
    member_type: Type,
}

pub(crate) struct DictType {
    key: Type,
    payload: Type,
}

pub(crate) enum VariantType {
    Tuple(TupleType),
    Struct(StructType),
}

pub(crate) struct TaggedType {
    tag: String,
    item_type: Type,
}
