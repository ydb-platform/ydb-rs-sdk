use std::collections::HashMap;
use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_table_service::query_stats::RawQueryStatMode;
use crate::grpc_wrapper::raw_table_service::transaction_control::RawTransactionControl;
use crate::grpc_wrapper::raw_table_service::value::{RawResultSet, RawTypedValue};
use crate::grpc_wrapper::raw_table_service::value::r#type::RawType;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;

pub(crate) struct RawExecuteDataQueryRequest {
    pub session_id: String,
    pub tx_control: RawTransactionControl,
    pub yql_text: String,
    pub operation_params: RawOperationParams,
    pub params: HashMap<String, RawTypedValue>,
    pub keep_in_cache: bool,
    pub collect_stats: RawQueryStatMode,
}

impl From<RawExecuteDataQueryRequest> for ydb_grpc::ydb_proto::table::ExecuteDataQueryRequest {
    fn from(v: RawExecuteDataQueryRequest) -> Self {
        Self {
            session_id: v.session_id,
            tx_control: Some(v.tx_control.into()),
            query: Some(ydb_grpc::ydb_proto::table::Query {
                query: Some(ydb_grpc::ydb_proto::table::query::Query::YqlText(
                    v.yql_text,
                )),
            }),
            parameters: v.params.into_iter().map(|(k, v)| (k, v.into())).collect(),
            query_cache_policy: Some(ydb_grpc::ydb_proto::table::QueryCachePolicy {
                keep_in_cache: v.keep_in_cache,
            }),
            operation_params: Some(v.operation_params.into()),
            collect_stats: ydb_grpc::ydb_proto::table::query_stats_collection::Mode::from(
                v.collect_stats,
            ) as i32,
        }
    }
}

pub(crate) struct RawExecuteDataQueryResult {
    result_sets: Vec<RawResultSet>,
    tx_meta: RawTransactionMeta,
    query_meta: RawQueryMeta,
    // query_stats: Option<RawQueryStats>, // todo
}

impl TryFrom<ydb_grpc::ydb_proto::table::ExecuteQueryResult> for RawExecuteDataQueryResult {
    type Error = RawError;

    fn try_from(
        value: ydb_grpc::ydb_proto::table::ExecuteQueryResult,
    ) -> Result<Self, Self::Error> {
        let result_sets_res: Result<_, RawError> = value
            .result_sets
            .into_iter()
            .map(|item| item.try_into())
            .collect();

        Ok(Self {
            result_sets: result_sets_res?,
            tx_meta: value
                .tx_meta
                .ok_or_else(|| RawError::custom("no tx_meta at ExecuteQueryResult"))?
                .into(),
            query_meta: value
                .query_meta
                .ok_or_else(|| RawError::custom("no query_mets at ExecuteQueryResult"))?
                .try_into()?,
        })
    }
}

pub(crate) struct RawTransactionMeta {
    pub id: String,
}

impl From<ydb_grpc::ydb_proto::table::TransactionMeta> for RawTransactionMeta {
    fn from(value: ydb_grpc::ydb_proto::table::TransactionMeta) -> Self {
        Self { id: value.id }
    }
}

pub(crate) struct RawQueryMeta {
    pub id: String,
    pub parameter_types: HashMap<String, RawType>,
}

impl TryFrom<ydb_grpc::ydb_proto::table::QueryMeta> for RawQueryMeta {
    type Error = RawError;

    fn try_from(value: ydb_grpc::ydb_proto::table::QueryMeta) -> Result<Self, Self::Error> {
        let parameter_types_res: Result<HashMap<_, _>, RawError> = value
            .parameters_types
            .into_iter()
            .map(|(key, value)| Ok((key, value.try_into()?)))
            .collect();

        Ok(Self {
            id: value.id,
            parameter_types: parameter_types_res?,
        })
    }
}
