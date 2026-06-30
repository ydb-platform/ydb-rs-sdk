use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_table_service::value::r#type::RawType;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use std::collections::HashMap;
use ydb_grpc::ydb_proto::table::PrepareDataQueryRequest;

pub(crate) struct RawPrepareDataQueryRequest {
    pub session_id: String,
    pub yql_text: String,
    pub operation_params: RawOperationParams,
}

impl From<RawPrepareDataQueryRequest> for PrepareDataQueryRequest {
    fn from(value: RawPrepareDataQueryRequest) -> Self {
        Self {
            session_id: value.session_id,
            yql_text: value.yql_text,
            operation_params: Some(value.operation_params.into()),
        }
    }
}

pub(crate) struct RawPrepareDataQueryResult {
    pub query_id: String,
    pub parameter_types: HashMap<String, RawType>,
}

impl TryFrom<ydb_grpc::ydb_proto::table::PrepareQueryResult> for RawPrepareDataQueryResult {
    type Error = RawError;

    fn try_from(
        value: ydb_grpc::ydb_proto::table::PrepareQueryResult,
    ) -> Result<Self, Self::Error> {
        let parameter_types = value
            .parameters_types
            .into_iter()
            .map(|(name, ty)| {
                let raw_type = ty
                    .try_into()
                    .map_err(|e: RawError| RawError::custom(format!("param {name}: {e}")))?;
                Ok((name, raw_type))
            })
            .collect::<Result<HashMap<_, _>, RawError>>()?;

        Ok(Self {
            query_id: value.query_id,
            parameter_types,
        })
    }
}
