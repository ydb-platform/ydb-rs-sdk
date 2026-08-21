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

pub(crate) struct RawBulkUpsertArrowRequest {
    pub table: String,
    pub arrow_schema: Vec<u8>,
    pub arrow_data: Vec<u8>,
    pub operation_params: RawOperationParams,
}

impl From<RawBulkUpsertArrowRequest> for BulkUpsertRequest {
    fn from(value: RawBulkUpsertArrowRequest) -> Self {
        use ydb_grpc::ydb_proto::formats::ArrowBatchSettings;
        use ydb_grpc::ydb_proto::table::bulk_upsert_request::DataFormat;

        Self {
            table: value.table,
            rows: None,
            data: value.arrow_data,
            data_format: Some(DataFormat::ArrowBatchSettings(ArrowBatchSettings {
                schema: value.arrow_schema,
            })),
            operation_params: Some(value.operation_params.into()),
        }
    }
}
