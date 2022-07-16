use crate::grpc_wrapper::channel::ChannelWithAuth;
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::raw_ydb_operation::OperationParams;
use ydb_grpc::ydb_proto::scheme::v1::scheme_service_client::SchemeServiceClient;

pub(crate) struct SchemeClient {
    service: SchemeServiceClient<ChannelWithAuth>,
}

impl SchemeClient {
    pub(crate) async fn ListDirectory(req: ListDirectoryRequest)->
}

impl GrpcServiceForDiscovery for SchemeClient {
    fn get_grpc_discovery_service() -> Service {
        return Service::Scheme;
    }
}

pub(crate) struct ListDirectoryRequest {
    pub(crate) operation_params: OperationParams,
    pub(crate) path: String,
}

pub(crate) struct ListDirectoryResult {
    pub(crate) self_item: Entry,
    pub(crate) childred: Vec<Entry>,
}

pub(crate) struct Entry {
    pub(crate) name: String,
    pub(crate) owner: String,
    pub(crate) r#type: EntryType,
    pub(crate) effective_permissions: Vec<Permissions>,
    pub(crate) permissions: Vec<Permissions>,
    pub(crate) size_bytes: u64,
}

pub(crate) enum EntryType {
    TypeUnspecified = 0,
    Directory = 1,
    Table = 2,
    PersQueueGroup = 3,
    Database = 4,
    RtmrVolume = 5,
    BlockStoreVolume = 6,
    CoordinationNode = 7,
    Sequence = 15,
    Replication = 16,
    Unknown(i32),
}

pub(crate) struct Permissions {
    pub(crate) subject: String,
    pub(crate) permission_names: Vec<String>,
}
