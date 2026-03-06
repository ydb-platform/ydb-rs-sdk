use crate::grpc_wrapper::raw_table_service::copy_table::RawCopyTableItem;

#[derive(Clone)]
pub struct CopyTableItem {
    inner: RawCopyTableItem,
}

impl CopyTableItem {
    #[allow(dead_code)]
    pub fn new(source_path: String, destination_path: String, omit_indexes: bool) -> Self {
        Self {
            inner: RawCopyTableItem {
                source_path,
                destination_path,
                omit_indexes,
            },
        }
    }
}

impl From<CopyTableItem> for RawCopyTableItem {
    fn from(value: CopyTableItem) -> Self {
        value.inner
    }
}

use crate::grpc_wrapper::raw_table_service::value::r#type::{
    RawType, VariantType as RawVariantType,
};

#[derive(Debug, Clone)]
pub struct TableDescription {
    /// Full path to the table
    pub path: String,
    /// List of table columns
    pub columns: Vec<ColumnDescription>,
    /// List of primary key column names
    pub primary_key: Vec<String>,
    /// List of Table indexes
    pub indexes: Vec<IndexDescription>,
    /// YDB table storage type (Row/Column)
    pub store_type: StoreType,
}

#[derive(Debug, Clone)]
pub struct ColumnDescription {
    pub name: String,
    pub column_type: ColumnType,
    pub not_null: bool,
    pub family: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
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
    Bytes,
    Utf8,
    Yson,
    Json,
    Uuid,
    JsonDocument,
    DyNumber,
    Decimal {
        precision: u8,
        scale: i16,
    },
    Optional(Box<ColumnType>),
    List(Box<ColumnType>),
    Tuple(Vec<ColumnType>),
    Struct(Vec<StructField>),
    Dict {
        key: Box<ColumnType>,
        value: Box<ColumnType>,
    },
    Variant(Box<VariantType>),
    Tagged {
        tag: String,
        item_type: Box<ColumnType>,
    },
    Void,
    Null,
    EmptyList,
    EmptyDict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructField {
    pub name: String,
    pub field_type: ColumnType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VariantType {
    Tuple(Vec<ColumnType>),
    Struct(Vec<StructField>),
}

impl From<RawType> for ColumnType {
    fn from(raw: RawType) -> Self {
        match raw {
            RawType::Bool => ColumnType::Bool,
            RawType::Int8 => ColumnType::Int8,
            RawType::Uint8 => ColumnType::Uint8,
            RawType::Int16 => ColumnType::Int16,
            RawType::Uint16 => ColumnType::Uint16,
            RawType::Int32 => ColumnType::Int32,
            RawType::Uint32 => ColumnType::Uint32,
            RawType::Int64 => ColumnType::Int64,
            RawType::Uint64 => ColumnType::Uint64,
            RawType::Float => ColumnType::Float,
            RawType::Double => ColumnType::Double,
            RawType::Date => ColumnType::Date,
            RawType::DateTime => ColumnType::DateTime,
            RawType::Timestamp => ColumnType::Timestamp,
            RawType::Interval => ColumnType::Interval,
            RawType::TzDate => ColumnType::TzDate,
            RawType::TzDatetime => ColumnType::TzDatetime,
            RawType::TzTimestamp => ColumnType::TzTimestamp,
            RawType::Bytes => ColumnType::Bytes,
            RawType::UTF8 => ColumnType::Utf8,
            RawType::Yson => ColumnType::Yson,
            RawType::Json => ColumnType::Json,
            RawType::Uuid => ColumnType::Uuid,
            RawType::JSONDocument => ColumnType::JsonDocument,
            RawType::DyNumber => ColumnType::DyNumber,
            RawType::Decimal(dec) => ColumnType::Decimal {
                precision: dec.precision,
                scale: dec.scale,
            },
            RawType::Optional(inner) => ColumnType::Optional(Box::new((*inner).into())),
            RawType::List(inner) => ColumnType::List(Box::new((*inner).into())),
            RawType::Tuple(tuple) => {
                ColumnType::Tuple(tuple.elements.into_iter().map(|t| t.into()).collect())
            }
            RawType::Struct(st) => ColumnType::Struct(
                st.members
                    .into_iter()
                    .map(|m| StructField {
                        name: m.name,
                        field_type: m.member_type.into(),
                    })
                    .collect(),
            ),
            RawType::Dict(dict) => ColumnType::Dict {
                key: Box::new(dict.key.into()),
                value: Box::new(dict.payload.into()),
            },
            RawType::Variant(variant) => ColumnType::Variant(Box::new(match variant {
                RawVariantType::Tuple(tuple) => {
                    VariantType::Tuple(tuple.elements.into_iter().map(|t| t.into()).collect())
                }
                RawVariantType::Struct(st) => VariantType::Struct(
                    st.members
                        .into_iter()
                        .map(|m| StructField {
                            name: m.name,
                            field_type: m.member_type.into(),
                        })
                        .collect(),
                ),
            })),
            RawType::Tagged(tagged) => ColumnType::Tagged {
                tag: tagged.tag,
                item_type: Box::new(tagged.item_type.into()),
            },
            RawType::Void => ColumnType::Void,
            RawType::Null => ColumnType::Null,
            RawType::EmptyList => ColumnType::EmptyList,
            RawType::EmptyDict => ColumnType::EmptyDict,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexDescription {
    pub name: String,
    pub index_columns: Vec<String>,
    pub data_columns: Vec<String>,
    pub status: IndexStatus,
    pub index_type: IndexType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    Unspecified,
    Global,
    GlobalAsync,
    GlobalUnique,
}

impl From<crate::grpc_wrapper::raw_table_service::describe_table::RawIndexType> for IndexType {
    fn from(raw: crate::grpc_wrapper::raw_table_service::describe_table::RawIndexType) -> Self {
        use crate::grpc_wrapper::raw_table_service::describe_table::RawIndexType;
        match raw {
            RawIndexType::Unspecified => IndexType::Unspecified,
            RawIndexType::Global => IndexType::Global,
            RawIndexType::GlobalAsync => IndexType::GlobalAsync,
            RawIndexType::GlobalUnique => IndexType::GlobalUnique,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexStatus {
    Unspecified,
    Ready,
    Building,
}

impl From<crate::grpc_wrapper::raw_table_service::describe_table::RawIndexStatus> for IndexStatus {
    fn from(raw: crate::grpc_wrapper::raw_table_service::describe_table::RawIndexStatus) -> Self {
        use crate::grpc_wrapper::raw_table_service::describe_table::RawIndexStatus;
        match raw {
            RawIndexStatus::Unspecified => IndexStatus::Unspecified,
            RawIndexStatus::Ready => IndexStatus::Ready,
            RawIndexStatus::Building => IndexStatus::Building,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreType {
    Unspecified,
    Row,
    Column,
}

impl From<crate::grpc_wrapper::raw_table_service::describe_table::RawStoreType> for StoreType {
    fn from(raw: crate::grpc_wrapper::raw_table_service::describe_table::RawStoreType) -> Self {
        use crate::grpc_wrapper::raw_table_service::describe_table::RawStoreType;
        match raw {
            RawStoreType::Unspecified => StoreType::Unspecified,
            RawStoreType::Row => StoreType::Row,
            RawStoreType::Column => StoreType::Column,
        }
    }
}
