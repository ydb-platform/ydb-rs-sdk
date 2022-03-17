#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DecimalType {
    #[prost(uint32, tag = "1")]
    pub precision: u32,
    #[prost(uint32, tag = "2")]
    pub scale: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OptionalType {
    #[prost(message, optional, boxed, tag = "1")]
    pub item: ::core::option::Option<::prost::alloc::boxed::Box<Type>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListType {
    #[prost(message, optional, boxed, tag = "1")]
    pub item: ::core::option::Option<::prost::alloc::boxed::Box<Type>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VariantType {
    #[prost(oneof = "variant_type::Type", tags = "1, 2")]
    pub r#type: ::core::option::Option<variant_type::Type>,
}
/// Nested message and enum types in `VariantType`.
pub mod variant_type {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Type {
        #[prost(message, tag = "1")]
        TupleItems(super::TupleType),
        #[prost(message, tag = "2")]
        StructItems(super::StructType),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TupleType {
    #[prost(message, repeated, tag = "1")]
    pub elements: ::prost::alloc::vec::Vec<Type>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StructMember {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub r#type: ::core::option::Option<Type>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StructType {
    #[prost(message, repeated, tag = "1")]
    pub members: ::prost::alloc::vec::Vec<StructMember>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DictType {
    #[prost(message, optional, boxed, tag = "1")]
    pub key: ::core::option::Option<::prost::alloc::boxed::Box<Type>>,
    #[prost(message, optional, boxed, tag = "2")]
    pub payload: ::core::option::Option<::prost::alloc::boxed::Box<Type>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Type {
    #[prost(
        oneof = "r#type::Type",
        tags = "1, 2, 101, 102, 103, 104, 105, 106, 201"
    )]
    pub r#type: ::core::option::Option<r#type::Type>,
}
/// Nested message and enum types in `Type`.
pub mod r#type {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum PrimitiveTypeId {
        Unspecified = 0,
        Bool = 6,
        Int8 = 7,
        Uint8 = 5,
        Int16 = 8,
        Uint16 = 9,
        Int32 = 1,
        Uint32 = 2,
        Int64 = 3,
        Uint64 = 4,
        Float = 33,
        Double = 32,
        Date = 48,
        Datetime = 49,
        Timestamp = 50,
        Interval = 51,
        TzDate = 52,
        TzDatetime = 53,
        TzTimestamp = 54,
        String = 4097,
        Utf8 = 4608,
        Yson = 4609,
        Json = 4610,
        Uuid = 4611,
        JsonDocument = 4612,
        Dynumber = 4866,
    }
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Type {
        /// Data types
        #[prost(enumeration = "PrimitiveTypeId", tag = "1")]
        TypeId(i32),
        #[prost(message, tag = "2")]
        DecimalType(super::DecimalType),
        /// Container types
        #[prost(message, tag = "101")]
        OptionalType(::prost::alloc::boxed::Box<super::OptionalType>),
        #[prost(message, tag = "102")]
        ListType(::prost::alloc::boxed::Box<super::ListType>),
        #[prost(message, tag = "103")]
        TupleType(super::TupleType),
        #[prost(message, tag = "104")]
        StructType(super::StructType),
        #[prost(message, tag = "105")]
        DictType(::prost::alloc::boxed::Box<super::DictType>),
        #[prost(message, tag = "106")]
        VariantType(super::VariantType),
        /// Special types
        #[prost(enumeration = "::prost_types::NullValue", tag = "201")]
        VoidType(i32),
    }
}
//*
// Holds a pair to represent Dict type

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValuePair {
    #[prost(message, optional, tag = "1")]
    pub key: ::core::option::Option<Value>,
    #[prost(message, optional, tag = "2")]
    pub payload: ::core::option::Option<Value>,
}
//*
// This message represents any of the supported by transport value types.
// Note, this is not actually a Ydb types. See NYql.NProto.TypeIds for Ydb types.
//
// For scalar types, just oneof value used.
// For composite types repeated Items or Pairs used. See below.
//
// The idea is, we do not represent explicitly Optional<T> if value is not null (most common case)
// - just represents value of T. Numbers of Optional levels we can get from type.
// Variant<T> type always represent explicitly

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Value {
    /// Used for List, Tuple, Struct types
    #[prost(message, repeated, tag = "12")]
    pub items: ::prost::alloc::vec::Vec<Value>,
    /// Used for Dict type
    #[prost(message, repeated, tag = "13")]
    pub pairs: ::prost::alloc::vec::Vec<ValuePair>,
    /// Used for Variant type
    #[prost(uint32, tag = "14")]
    pub variant_index: u32,
    #[prost(fixed64, tag = "16")]
    pub high_128: u64,
    #[prost(oneof = "value::Value", tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15")]
    pub value: ::core::option::Option<value::Value>,
}
/// Nested message and enum types in `Value`.
pub mod value {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(bool, tag = "1")]
        BoolValue(bool),
        #[prost(sfixed32, tag = "2")]
        Int32Value(i32),
        #[prost(fixed32, tag = "3")]
        Uint32Value(u32),
        #[prost(sfixed64, tag = "4")]
        Int64Value(i64),
        #[prost(fixed64, tag = "5")]
        Uint64Value(u64),
        #[prost(float, tag = "6")]
        FloatValue(f32),
        #[prost(double, tag = "7")]
        DoubleValue(f64),
        #[prost(bytes, tag = "8")]
        BytesValue(::prost::alloc::vec::Vec<u8>),
        #[prost(string, tag = "9")]
        TextValue(::prost::alloc::string::String),
        /// Set if current TValue is terminal Null
        #[prost(enumeration = "::prost_types::NullValue", tag = "10")]
        NullFlagValue(i32),
        /// Represents nested TValue for Optional<Optional<T>>(Null), or Variant<T> types
        #[prost(message, tag = "11")]
        NestedValue(::prost::alloc::boxed::Box<super::Value>),
        #[prost(fixed64, tag = "15")]
        Low128(u64),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TypedValue {
    #[prost(message, optional, tag = "1")]
    pub r#type: ::core::option::Option<Type>,
    #[prost(message, optional, tag = "2")]
    pub value: ::core::option::Option<Value>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Column {
    /// Name of column
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    /// Type of column
    #[prost(message, optional, tag = "2")]
    pub r#type: ::core::option::Option<Type>,
}
/// Represents table-like structure with ordered set of rows and columns
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResultSet {
    /// Metadata of columns
    #[prost(message, repeated, tag = "1")]
    pub columns: ::prost::alloc::vec::Vec<Column>,
    /// Rows of table
    #[prost(message, repeated, tag = "2")]
    pub rows: ::prost::alloc::vec::Vec<Value>,
    /// Flag indicates the result was truncated
    #[prost(bool, tag = "3")]
    pub truncated: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatusIds {}
/// Nested message and enum types in `StatusIds`.
pub mod status_ids {
    /// reserved range [400000, 400999]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum StatusCode {
        Unspecified = 0,
        Success = 400000,
        BadRequest = 400010,
        Unauthorized = 400020,
        InternalError = 400030,
        Aborted = 400040,
        Unavailable = 400050,
        Overloaded = 400060,
        SchemeError = 400070,
        GenericError = 400080,
        Timeout = 400090,
        BadSession = 400100,
        PreconditionFailed = 400120,
        AlreadyExists = 400130,
        NotFound = 400140,
        SessionExpired = 400150,
        Cancelled = 400160,
        Undetermined = 400170,
        Unsupported = 400180,
        SessionBusy = 400190,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FeatureFlag {}
/// Nested message and enum types in `FeatureFlag`.
pub mod feature_flag {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Unspecified = 0,
        Enabled = 1,
        Disabled = 2,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CostInfo {
    /// Total amount of request units (RU), consumed by the operation.
    #[prost(double, tag = "1")]
    pub consumed_units: f64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Limit {
    #[prost(oneof = "limit::Kind", tags = "1, 2, 3, 4, 5, 6")]
    pub kind: ::core::option::Option<limit::Kind>,
}
/// Nested message and enum types in `Limit`.
pub mod limit {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Range {
        #[prost(uint32, tag = "1")]
        pub min: u32,
        #[prost(uint32, tag = "2")]
        pub max: u32,
    }
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Kind {
        #[prost(message, tag = "1")]
        Range(Range),
        #[prost(uint32, tag = "2")]
        Lt(u32),
        #[prost(uint32, tag = "3")]
        Le(u32),
        #[prost(uint32, tag = "4")]
        Eq(u32),
        #[prost(uint32, tag = "5")]
        Ge(u32),
        #[prost(uint32, tag = "6")]
        Gt(u32),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MapKey {
    #[prost(message, optional, tag = "1")]
    pub length: ::core::option::Option<Limit>,
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
}
