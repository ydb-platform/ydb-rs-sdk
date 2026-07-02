use crate::grpc_wrapper::raw_table_service::value::r#type::RawType;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use std::collections::HashMap;
use ydb_grpc::ydb_proto::table::{ColumnMeta, CreateTableRequest};

pub(crate) struct RawCreateTableColumn {
    pub name: String,
    pub column_type: RawType,
    pub not_null: bool,
    pub family: String,
}

pub(crate) struct RawCreateTableRequest {
    pub session_id: String,
    pub path: String,
    pub columns: Vec<RawCreateTableColumn>,
    pub primary_key: Vec<String>,
    pub attributes: HashMap<String, String>,
    pub operation_params: RawOperationParams,
}

impl From<RawCreateTableRequest> for CreateTableRequest {
    fn from(value: RawCreateTableRequest) -> Self {
        Self {
            session_id: value.session_id,
            path: value.path,
            columns: value
                .columns
                .into_iter()
                .map(|col| ColumnMeta {
                    name: col.name,
                    r#type: Some(col.column_type.into()),
                    family: col.family,
                    not_null: Some(col.not_null),
                    default_value: None,
                })
                .collect(),
            primary_key: value.primary_key,
            attributes: value.attributes,
            operation_params: Some(value.operation_params.into()),
            ..Default::default()
        }
    }
}
