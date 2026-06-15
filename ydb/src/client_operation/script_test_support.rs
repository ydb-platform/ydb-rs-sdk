use std::time::Duration;

use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;
use crate::grpc_wrapper::raw_query_service::execute_script::RawExecuteScriptRequest;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;

/// Start a long-running ExecuteScript operation (for integration tests).
pub(crate) async fn start_execute_script_operation(
    connection_manager: &GrpcConnectionManager,
) -> RawResult<String> {
    let mut client = connection_manager
        .get_auth_service(RawQueryClient::new)
        .await
        .map_err(|e| RawError::custom(e.to_string()))?;

    let req = RawExecuteScriptRequest {
        yql_text: "$items = ListFromRange(1, 50000000); SELECT ListSum($items);".to_string(),
        parameters: Default::default(),
        results_ttl: Duration::from_secs(3600),
        operation_params: RawOperationParams::new_async(
            Duration::from_secs(3600),
            Duration::from_secs(3600),
        ),
        collect_stats: false,
    };

    let operation = client.execute_script(req).await?;
    if operation.id.is_empty() {
        return Err(RawError::custom(
            "execute script returned empty operation id",
        ));
    }

    Ok(operation.id)
}
