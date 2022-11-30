use crate::grpc_wrapper::raw_table_service::transaction_control::RawTransactionControl;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;

pub(crate) struct RawExecuteDataQueryRequest {
    pub session_id: String,
    pub tx_control: RawTransactionControl,
    pub yql_text: String,
    pub operation_params: RawOperationParams,
}

impl From<RawExecuteDataQueryRequest> for ydb_grpc::ydb_proto::table::ExecuteDataQueryRequest {
    fn from(v: RawExecuteDataQueryRequest) -> Self {
        Self {
            session_id: v.session_id,
            tx_control: None,
            query: None,
            parameters: Default::default(),
            query_cache_policy: None,
            operation_params: None,
            collect_stats: 0,
        }
    }
}
