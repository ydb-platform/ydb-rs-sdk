use crate::grpc_wrapper::channel::ChannelWithAuth;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use ydb_grpc::ydb_proto::table::v1::table_service_client::TableServiceClient;

pub(crate) struct GrpcTableClient {
    service: TableServiceClient<ChannelWithAuth>,
}

pub(crate) struct ExecuteSchemeQueryRequest {
    pub(crate) session_id: String,
    pub(crate) text: String,
    pub(crate) operation_params: RawOperationParams,
}

pub(crate) struct ExecuteSchemeQueryResult {}
