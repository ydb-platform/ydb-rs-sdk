use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_table_service::query_stats::RawQueryStatsMode;
use crate::grpc_wrapper::raw_table_service::transaction_control::RawTransactionControl;
use crate::grpc_wrapper::raw_table_service::value::r#type::RawType;
use crate::grpc_wrapper::raw_table_service::value::{RawResultSet, RawTypedValue};
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use std::collections::HashMap;

#[derive(serde::Serialize)]
pub(crate) struct RawExecuteDataQueryRequest {
    pub session_id: String,
    pub tx_control: RawTransactionControl,
    pub yql_text: String,
    pub operation_params: RawOperationParams,
    pub params: HashMap<String, RawTypedValue>,
    pub keep_in_cache: bool,
    pub collect_stats: RawQueryStatsMode,
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

#[derive(serde::Serialize)]
pub(crate) struct RawExecuteDataQueryResult {
    pub result_sets: Vec<RawResultSet>,
    pub tx_meta: RawTransactionMeta,
    pub query_meta: Option<RawQueryMeta>,
    pub query_stats: Option<RawQueryStats>,
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

        let query_meta = if let Some(proto_meta) = value.query_meta {
            Some(RawQueryMeta::try_from(proto_meta)?)
        } else {
            None
        };

        let query_stats = value.query_stats.map(RawQueryStats::from);

        Ok(Self {
            result_sets: result_sets_res?,
            tx_meta: value
                .tx_meta
                .ok_or_else(|| RawError::custom("no tx_meta at ExecuteQueryResult"))?
                .into(),
            query_meta,
            query_stats,
        })
    }
}

#[derive(serde::Serialize)]
pub(crate) struct RawTransactionMeta {
    pub id: String,
}

impl From<ydb_grpc::ydb_proto::table::TransactionMeta> for RawTransactionMeta {
    fn from(value: ydb_grpc::ydb_proto::table::TransactionMeta) -> Self {
        Self { id: value.id }
    }
}

#[derive(serde::Serialize)]
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

#[derive(serde::Serialize)]
pub(crate) struct RawQueryStats {
    pub query_phases: Vec<RawQueryPhaseStats>,
    pub process_cpu_time: std::time::Duration,
    pub query_plan: String,
    pub query_ast: String,
    pub total_duration: std::time::Duration,
    pub total_cpu_time: std::time::Duration,
}

impl From<ydb_grpc::ydb_proto::table_stats::QueryStats> for RawQueryStats {
    fn from(value: ydb_grpc::ydb_proto::table_stats::QueryStats) -> Self {
        Self {
            query_phases: value.query_phases.into_iter().map(Into::into).collect(),
            process_cpu_time: std::time::Duration::from_micros(value.process_cpu_time_us),
            query_plan: value.query_plan,
            query_ast: value.query_ast,
            total_duration: std::time::Duration::from_micros(value.total_duration_us),
            total_cpu_time: std::time::Duration::from_micros(value.total_cpu_time_us),
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct RawQueryPhaseStats {
    pub duration: std::time::Duration,

    pub table_access: Vec<RawTableAccessStats>,

    pub cpu_time: std::time::Duration,

    pub affected_shards: u64,

    pub literal_phase: bool,
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct RawTableAccessStats {
    pub name: String,
    pub reads: Option<RawOperationStats>,
    pub updates: Option<RawOperationStats>,
    pub deletes: Option<RawOperationStats>,
    pub partitions_count: u64,
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct RawOperationStats {
    pub rows: u64,
    pub bytes: u64,
}

impl From<ydb_grpc::ydb_proto::table_stats::OperationStats> for RawOperationStats {
    fn from(value: ydb_grpc::ydb_proto::table_stats::OperationStats) -> Self {
        Self {
            rows: value.rows,
            bytes: value.bytes,
        }
    }
}

impl From<ydb_grpc::ydb_proto::table_stats::TableAccessStats> for RawTableAccessStats {
    fn from(value: ydb_grpc::ydb_proto::table_stats::TableAccessStats) -> Self {
        let reads = value.reads.map(Into::into);
        let updates = value.updates.map(Into::into);
        let deletes = value.deletes.map(Into::into);

        Self {
            name: value.name,
            reads,
            updates,
            deletes,
            partitions_count: value.partitions_count,
        }
    }
}

impl From<ydb_grpc::ydb_proto::table_stats::QueryPhaseStats> for RawQueryPhaseStats {
    fn from(value: ydb_grpc::ydb_proto::table_stats::QueryPhaseStats) -> Self {
        Self {
            duration: std::time::Duration::from_micros(value.duration_us),
            table_access: value.table_access.into_iter().map(Into::into).collect(),
            cpu_time: std::time::Duration::from_micros(value.cpu_time_us),
            affected_shards: value.affected_shards,
            literal_phase: value.literal_phase,
        }
    }
}
