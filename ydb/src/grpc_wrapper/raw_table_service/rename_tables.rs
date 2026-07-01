use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use itertools::Itertools;

#[derive(Clone)]
pub(crate) struct RawRenameTableItem {
    pub source_path: String,
    pub destination_path: String,
    pub replace_destination: bool,
}

impl From<RawRenameTableItem> for ydb_grpc::ydb_proto::table::RenameTableItem {
    fn from(value: RawRenameTableItem) -> Self {
        Self {
            source_path: value.source_path,
            destination_path: value.destination_path,
            replace_destination: value.replace_destination,
        }
    }
}

pub(crate) struct RawRenameTablesRequest {
    pub operation_params: RawOperationParams,
    pub session_id: String,
    pub tables: Vec<RawRenameTableItem>,
}

impl From<RawRenameTablesRequest> for ydb_grpc::ydb_proto::table::RenameTablesRequest {
    fn from(value: RawRenameTablesRequest) -> Self {
        Self {
            operation_params: Some(value.operation_params.into()),
            session_id: value.session_id,
            tables: value.tables.into_iter().map_into().collect(),
        }
    }
}
