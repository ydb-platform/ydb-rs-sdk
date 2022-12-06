use std::collections::HashMap;
use ydb_grpc::ydb_proto::table::ExecuteQueryResult;
use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_table_service::query_stats::RawQueryStatMode;
use crate::grpc_wrapper::raw_table_service::transaction_control::RawTransactionControl;
use crate::grpc_wrapper::raw_table_service::value::{RawTypedValue};
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
            query: Some(ydb_grpc::ydb_proto::table::Query{ query: Some(ydb_grpc::ydb_proto::table::query::Query::YqlText(v.yql_text)) }),
            parameters: v.params.into_iter().map(|(k, v)| { (k, v.into() )}).collect(),
            query_cache_policy: Some(ydb_grpc::ydb_proto::table::QueryCachePolicy{keep_in_cache: v.keep_in_cache}),
            operation_params: Some(v.operation_params.into()),
            collect_stats: ydb_grpc::ydb_proto::table::query_stats_collection::Mode::from(v.collect_stats) as i32,
        }
    }
}

pub(crate) struct RawExecuteDataQueryResult {

}

impl TryFrom<ydb_grpc::ydb_proto::table::ExecuteQueryResult> for RawExecuteDataQueryResult{
    type Error = RawError;

    fn try_from(value: ExecuteQueryResult) -> Result<Self, Self::Error> {
        todo!()
    }
}