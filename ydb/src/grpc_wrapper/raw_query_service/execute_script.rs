use std::collections::HashMap;
use std::time::Duration;

use crate::grpc_wrapper::raw_common_types::Duration as RawDuration;
use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::grpc_wrapper::raw_query_service::status::check_status;
use crate::grpc_wrapper::raw_table_service::value::RawTypedValue;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use crate::types::Value;
use prost::Message;
use ydb_grpc::ydb_proto::query::{
    ExecMode, ExecuteScriptMetadata, ExecuteScriptRequest, QueryContent, StatsMode, Syntax,
};

#[derive(Clone, Debug)]
pub(crate) struct RawExecuteScriptRequest {
    pub yql_text: String,
    pub parameters: HashMap<String, Value>,
    pub results_ttl: Duration,
    pub operation_params: RawOperationParams,
    pub collect_stats: bool,
}

impl RawExecuteScriptRequest {
    pub(crate) fn into_proto(self) -> RawResult<ExecuteScriptRequest> {
        let mut parameters = HashMap::with_capacity(self.parameters.len());
        for (name, val) in self.parameters {
            let raw: RawTypedValue = val.try_into()?;
            parameters.insert(name, raw.into());
        }

        Ok(ExecuteScriptRequest {
            operation_params: Some(self.operation_params.into()),
            exec_mode: ExecMode::Execute as i32,
            script_content: Some(QueryContent {
                syntax: Syntax::YqlV1 as i32,
                text: self.yql_text,
            }),
            parameters,
            stats_mode: if self.collect_stats {
                StatsMode::Basic as i32
            } else {
                StatsMode::None as i32
            },
            results_ttl: Some(RawDuration::from(self.results_ttl).into()),
            pool_id: String::new(),
        })
    }
}

pub(crate) fn parse_execute_script_operation(
    operation: ydb_grpc::ydb_proto::operations::Operation,
) -> RawResult<(String, Option<f64>)> {
    check_status(operation.status, &operation.issues)?;
    if !operation.id.is_empty() {
        return Ok((
            operation.id,
            operation.cost_info.map(|info| info.consumed_units),
        ));
    }
    if let Some(any) = operation.metadata {
        match ExecuteScriptMetadata::decode(&*any.value) {
            Ok(meta) if !meta.execution_id.is_empty() => {
                return Ok((
                    meta.execution_id,
                    operation.cost_info.map(|info| info.consumed_units),
                ));
            }
            Err(e) => {
                return Err(RawError::custom(format!(
                    "execute script returned empty operation id and metadata decode failed: {e}"
                )));
            }
            _ => {}
        }
    }
    Err(RawError::custom(
        "execute script returned empty operation id",
    ))
}
