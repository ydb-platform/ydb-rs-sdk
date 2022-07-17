use std::sync::Arc;
use tracing::trace;

use ydb_grpc::ydb_proto::scheme::v1::scheme_service_client::SchemeServiceClient;
use ydb_grpc::ydb_proto::scheme::{Entry, MakeDirectoryRequest, RemoveDirectoryRequest};

use crate::channel_pool::{ChannelPool, ChannelPoolImpl};
use crate::client::{Middleware, TimeoutSettings};
use crate::client_common::DBCredentials;
use crate::grpc::{grpc_read_operation_result, grpc_read_void_operation_result, operation_params};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_scheme_client::list_directory_types::ListDirectoryRequest;
use crate::grpc_wrapper::raw_services::Service;
use crate::grpc_wrapper::raw_ydb_operation::OperationParams;
use crate::{grpc_wrapper, Discovery, YdbResult};

pub(crate) type DirectoryServiceClientType = SchemeServiceClient<Middleware>;
pub(crate) type DirectoryServiceChannelPool = Arc<Box<dyn ChannelPool<DirectoryServiceClientType>>>;

pub struct SchemeClient {
    timeouts: TimeoutSettings,
    channel_pool: DirectoryServiceChannelPool,
    connection_manager: GrpcConnectionManager,
}

impl SchemeClient {
    pub(crate) fn new(
        credentials: DBCredentials,
        discovery: Arc<Box<dyn Discovery>>,
        timeouts: TimeoutSettings,
        connection_manager: GrpcConnectionManager,
    ) -> Self {
        let channel_pool = ChannelPoolImpl::new::<DirectoryServiceClientType>(
            discovery,
            credentials.clone(),
            Service::Scheme,
            DirectoryServiceClientType::new,
        );

        Self {
            channel_pool: Arc::new(Box::new(channel_pool)),
            timeouts,
            connection_manager,
        }
    }

    pub async fn make_directory(&mut self, path: String) -> YdbResult<()> {
        let req = MakeDirectoryRequest {
            operation_params: operation_params(self.timeouts.operation_timeout),
            path,
        };
        trace!(
            "make directory request: {}",
            serde_json::to_string(&req).unwrap_or("bad json".into())
        );
        let resp = self
            .channel_pool
            .create_channel()
            .await?
            .make_directory(req)
            .await?;

        grpc_read_void_operation_result(resp)
    }

    pub async fn list_directory(&mut self, path: String) -> YdbResult<Vec<Entry>> {
        let req = ListDirectoryRequest {
            operation_params: OperationParams::new_with_timeout(self.timeouts.operation_timeout),
            path,
        };
        return Ok(self
            .connection_manager
            .get_auth_service(grpc_wrapper::raw_scheme_client::client::SchemeClient::new)
            .await?
            .list_directory(req)?
            .children);
    }

    pub async fn remove_directory(&mut self, path: String) -> YdbResult<()> {
        let req = RemoveDirectoryRequest {
            operation_params: operation_params(self.timeouts.operation_timeout),
            path,
        };
        trace!(
            "remove directory request: {}",
            serde_json::to_string(&req).unwrap_or("bad json".into())
        );
        let resp = self
            .channel_pool
            .create_channel()
            .await?
            .remove_directory(req)
            .await?;

        grpc_read_void_operation_result(resp)
    }
}
