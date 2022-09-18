use std::collections::HashMap;

#[derive(Debug)]
pub(crate) struct RawOperationParams {
    operation_mode: OperationMode,
    operation_timeout: Option<crate::grpc_wrapper::raw_common_types::Duration>,
    cancel_after: Option<crate::grpc_wrapper::raw_common_types::Duration>,
    labels: HashMap<String, String>,
}

impl RawOperationParams {
    pub fn new_with_timeouts(
        operation_timeout: std::time::Duration,
        cancel_after: std::time::Duration,
    ) -> Self {
        Self {
            operation_mode: OperationMode::Sync,
            operation_timeout: Some(operation_timeout.into()),
            cancel_after: Some(cancel_after.into()),
            labels: Default::default(),
        }
    }
}

impl From<RawOperationParams> for ydb_grpc::ydb_proto::operations::OperationParams {
    fn from(params: RawOperationParams) -> Self {
        Self {
            operation_mode: params.operation_mode.into(),
            operation_timeout: params.operation_timeout.map(|item| item.into()),
            cancel_after: params.cancel_after.map(|item| item.into()),
            labels: params.labels,
            report_cost_info: ydb_grpc::ydb_proto::feature_flag::Status::Unspecified.into(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum OperationMode {
    _Unspecified,
    Sync,
    _Async,
}

use ydb_grpc::ydb_proto::operations::operation_params::OperationMode as GrpcOperationMode;
impl From<OperationMode> for i32 {
    fn from(mode: OperationMode) -> Self {
        let val = match mode {
            OperationMode::_Unspecified => GrpcOperationMode::Unspecified,
            OperationMode::Sync => GrpcOperationMode::Sync,
            OperationMode::_Async => GrpcOperationMode::Async,
        };
        val as i32
    }
}
