use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use itertools::Itertools;

pub(crate) struct RawCopyTableRequest {
    pub session_id: String,
    pub source_path: String,
    pub destination_path: String,
    pub operation_params: RawOperationParams,
}

impl From<RawCopyTableRequest> for ydb_grpc::ydb_proto::table::CopyTableRequest {
    fn from(value: RawCopyTableRequest) -> Self {
        Self {
            session_id: value.session_id,
            source_path: value.source_path,
            destination_path: value.destination_path,
            operation_params: Some(value.operation_params.into()),
        }
    }
}

#[derive(Clone)]
pub(crate) struct RawCopyTableItem {
    pub source_path: String,
    pub destination_path: String,
    pub omit_indexes: bool,
}

impl From<RawCopyTableItem> for ydb_grpc::ydb_proto::table::CopyTableItem {
    fn from(value: RawCopyTableItem) -> Self {
        Self {
            source_path: value.source_path,
            destination_path: value.destination_path,
            omit_indexes: value.omit_indexes,
        }
    }
}

pub(crate) struct RawCopyTablesRequest {
    pub operation_params: RawOperationParams,
    pub session_id: String,
    pub tables: Vec<RawCopyTableItem>,
}

impl From<RawCopyTablesRequest> for ydb_grpc::ydb_proto::table::CopyTablesRequest {
    fn from(value: RawCopyTablesRequest) -> Self {
        Self {
            operation_params: Some(value.operation_params.into()),
            session_id: value.session_id,
            tables: value.tables.into_iter().map_into().collect(),
        }
    }
}
