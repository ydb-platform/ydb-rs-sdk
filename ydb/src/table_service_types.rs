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

#[derive(Debug, Clone)]
pub struct TableDescription {
    /// List of table columns
    pub columns: Vec<ColumnDescription>,
    /// List of primary key column names
    pub primary_key: Vec<String>,
    /// List of Table indexes
    pub indexes: Vec<IndexDescription>,
    /// YDB table storage type (Row/Column)
    pub store_type: StoreType,
}

/// Error description of an unknown/unsupported column type
#[derive(Debug, Clone)]
pub struct UnknownTypeDescription {
    pub error: String,
}

/// Description of a table column
#[derive(Debug, Clone)]
pub struct ColumnDescription {
    /// Column name
    pub name: String,
    /// Column type, represented as an example Value
    /// Err if the type has not been converted to Value
    pub type_value: Result<crate::Value, UnknownTypeDescription>,
    /// Column family name
    pub family: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IndexDescription {
    pub name: String,
    pub index_columns: Vec<String>,
    pub data_columns: Vec<String>,
    pub status: IndexStatus,
    pub index_type: IndexType,
}

impl From<crate::grpc_wrapper::raw_table_service::describe_table::RawIndexDescription>
    for IndexDescription
{
    fn from(
        raw: crate::grpc_wrapper::raw_table_service::describe_table::RawIndexDescription,
    ) -> Self {
        Self {
            name: raw.name,
            index_columns: raw.index_columns,
            data_columns: raw.data_columns,
            status: raw.status.into(),
            index_type: raw.index_type.into(),
        }
    }
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
