use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_table_service::value::r#type::RawType;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;

pub(crate) struct RawDescribeTableRequest {
    pub session_id: String,
    pub path: String,
    pub operation_params: RawOperationParams,
}

impl From<RawDescribeTableRequest> for ydb_grpc::ydb_proto::table::DescribeTableRequest {
    fn from(v: RawDescribeTableRequest) -> Self {
        Self {
            session_id: v.session_id,
            path: v.path,
            operation_params: Some(v.operation_params.into()),
            include_shard_key_bounds: false,
            include_table_stats: false,
            include_partition_stats: false,
            include_shard_nodes_info: false,
        }
    }
}

#[derive(Debug)]
pub(crate) struct RawDescribeTableResult {
    pub columns: Vec<RawColumnMeta>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<RawIndexDescription>,
    pub store_type: RawStoreType,
}

impl TryFrom<ydb_grpc::ydb_proto::table::DescribeTableResult> for RawDescribeTableResult {
    type Error = RawError;

    fn try_from(
        value: ydb_grpc::ydb_proto::table::DescribeTableResult,
    ) -> Result<Self, Self::Error> {
        let columns = value
            .columns
            .into_iter()
            .map(RawColumnMeta::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        let indexes = value
            .indexes
            .into_iter()
            .map(RawIndexDescription::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            columns,
            primary_key: value.primary_key,
            indexes,
            store_type: value.store_type.try_into()?,
        })
    }
}

#[derive(Debug)]
pub(crate) struct RawColumnMeta {
    pub name: String,
    pub column_type: RawType,
    pub not_null: bool,
    pub family: String,
}

impl TryFrom<ydb_grpc::ydb_proto::table::ColumnMeta> for RawColumnMeta {
    type Error = RawError;

    fn try_from(value: ydb_grpc::ydb_proto::table::ColumnMeta) -> Result<Self, Self::Error> {
        let column_type = value
            .r#type
            .ok_or_else(|| RawError::custom("column type is missing"))?
            .try_into()?;

        Ok(Self {
            name: value.name,
            column_type,
            not_null: value.not_null.unwrap_or(false),
            family: value.family,
        })
    }
}

#[derive(Debug)]
pub(crate) struct RawIndexDescription {
    pub name: String,
    pub index_columns: Vec<String>,
    pub data_columns: Vec<String>,
    pub status: RawIndexStatus,
    pub index_type: RawIndexType,
}

impl TryFrom<ydb_grpc::ydb_proto::table::TableIndexDescription> for RawIndexDescription {
    type Error = RawError;

    fn try_from(
        value: ydb_grpc::ydb_proto::table::TableIndexDescription,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name,
            index_columns: value.index_columns,
            data_columns: value.data_columns,
            status: value.status.try_into()?,
            index_type: value.r#type.into(),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum RawIndexType {
    Unspecified,
    Global,
    GlobalAsync,
    GlobalUnique,
}

impl From<Option<ydb_grpc::ydb_proto::table::table_index_description::Type>> for RawIndexType {
    fn from(value: Option<ydb_grpc::ydb_proto::table::table_index_description::Type>) -> Self {
        use ydb_grpc::ydb_proto::table::table_index_description::Type;
        match value {
            Some(Type::GlobalIndex(_)) => RawIndexType::Global,
            Some(Type::GlobalAsyncIndex(_)) => RawIndexType::GlobalAsync,
            Some(Type::GlobalUniqueIndex(_)) => RawIndexType::GlobalUnique,
            None => RawIndexType::Unspecified,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum RawIndexStatus {
    Unspecified,
    Ready,
    Building,
}

impl TryFrom<i32> for RawIndexStatus {
    type Error = RawError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        use ydb_grpc::ydb_proto::table::table_index_description::Status;
        let status = Status::try_from(value)
            .map_err(|e| RawError::ProtobufDecodeError(format!("invalid index status: {e}")))?;
        Ok(match status {
            Status::Unspecified => RawIndexStatus::Unspecified,
            Status::Ready => RawIndexStatus::Ready,
            Status::Building => RawIndexStatus::Building,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum RawStoreType {
    Unspecified,
    Row,
    Column,
}

impl TryFrom<i32> for RawStoreType {
    type Error = RawError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        use ydb_grpc::ydb_proto::table::StoreType;
        let store_type = StoreType::try_from(value)
            .map_err(|e| RawError::ProtobufDecodeError(format!("invalid store type: {e}")))?;
        Ok(match store_type {
            StoreType::Unspecified => RawStoreType::Unspecified,
            StoreType::Row => RawStoreType::Row,
            StoreType::Column => RawStoreType::Column,
        })
    }
}
