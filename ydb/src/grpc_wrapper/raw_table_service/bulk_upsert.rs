use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use ydb_grpc::ydb_proto::table::BulkUpsertRequest;

pub(crate) struct RawBulkUpsertRequest {
    pub table: String,
    pub rows: ydb_grpc::ydb_proto::TypedValue,
    pub operation_params: RawOperationParams,
}

impl From<RawBulkUpsertRequest> for BulkUpsertRequest {
    fn from(value: RawBulkUpsertRequest) -> Self {
        Self {
            table: value.table,
            rows: Some(value.rows),
            operation_params: Some(value.operation_params.into()),
            data: Default::default(),
            data_format: None,
        }
    }
}
