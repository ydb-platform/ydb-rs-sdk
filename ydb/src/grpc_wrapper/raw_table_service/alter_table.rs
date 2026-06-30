use crate::grpc_wrapper::raw_table_service::create_table::RawCreateTableColumn;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use std::collections::HashMap;
use ydb_grpc::ydb_proto::table::{AlterTableRequest, ColumnMeta};

pub(crate) struct RawAlterTableRequest {
    pub session_id: String,
    pub path: String,
    pub add_columns: Vec<RawCreateTableColumn>,
    pub drop_columns: Vec<String>,
    pub alter_columns: Vec<RawCreateTableColumn>,
    pub alter_attributes: HashMap<String, String>,
    pub operation_params: RawOperationParams,
}

impl From<RawAlterTableRequest> for AlterTableRequest {
    fn from(value: RawAlterTableRequest) -> Self {
        fn to_column_meta(col: RawCreateTableColumn) -> ColumnMeta {
            ColumnMeta {
                name: col.name,
                r#type: Some(col.column_type.into()),
                family: col.family,
                not_null: Some(col.not_null),
                default_value: None,
            }
        }

        Self {
            session_id: value.session_id,
            path: value.path,
            add_columns: value.add_columns.into_iter().map(to_column_meta).collect(),
            drop_columns: value.drop_columns,
            alter_columns: value
                .alter_columns
                .into_iter()
                .map(to_column_meta)
                .collect(),
            alter_attributes: value.alter_attributes,
            operation_params: Some(value.operation_params.into()),
            ..Default::default()
        }
    }
}
