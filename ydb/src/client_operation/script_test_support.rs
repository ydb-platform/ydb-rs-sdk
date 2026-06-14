use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::grpc_limits::WithGrpcMaxMessageSize;
use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;
use ydb_grpc::ydb_proto::operations::operation_params::OperationMode;
use ydb_grpc::ydb_proto::operations::OperationParams;
use ydb_grpc::ydb_proto::query::v1::query_service_client::QueryServiceClient;
use ydb_grpc::ydb_proto::query::{ExecMode, ExecuteScriptRequest, QueryContent, Syntax};

struct RawQueryScriptClient {
    service: QueryServiceClient<InterceptedChannel>,
}

impl WithGrpcMaxMessageSize for RawQueryScriptClient {
    fn with_grpc_max_message_size(mut self, bytes: usize) -> Self {
        self.service = self
            .service
            .max_decoding_message_size(bytes)
            .max_encoding_message_size(bytes);
        self
    }
}

impl GrpcServiceForDiscovery for RawQueryScriptClient {
    fn get_grpc_discovery_service() -> Service {
        // Query service shares endpoints with table service in discovery.
        Service::Table
    }
}

/// Start a long-running ExecuteScript operation (for integration tests).
pub(crate) async fn start_execute_script_operation(
    connection_manager: &GrpcConnectionManager,
) -> RawResult<String> {
    let mut client = connection_manager
        .get_auth_service(|ch| RawQueryScriptClient {
            service: QueryServiceClient::new(ch),
        })
        .await
        .map_err(|e| RawError::custom(e.to_string()))?;

    let response = client
        .service
        .execute_script(ExecuteScriptRequest {
            operation_params: Some(OperationParams {
                operation_mode: OperationMode::Async as i32,
                operation_timeout: None,
                cancel_after: None,
                labels: Default::default(),
                report_cost_info: ydb_grpc::ydb_proto::feature_flag::Status::Unspecified.into(),
            }),
            exec_mode: ExecMode::Execute as i32,
            script_content: Some(QueryContent {
                syntax: Syntax::YqlV1 as i32,
                text: "$items = ListFromRange(1, 50000000); SELECT ListSum($items);".to_string(),
            }),
            parameters: Default::default(),
            stats_mode: 0,
            results_ttl: None,
            pool_id: String::new(),
        })
        .await?;

    let operation = response.into_inner();
    if operation.id.is_empty() {
        return Err(RawError::custom("execute script returned empty operation id"));
    }

    Ok(operation.id)
}
