use std::sync::Arc;
use tracing::trace;

use ydb_grpc::ydb_proto::scheme::v1::scheme_service_client::SchemeServiceClient;
use ydb_grpc::ydb_proto::scheme::{MakeDirectoryRequest, RemoveDirectoryRequest};

use crate::channel_pool::{ChannelPool, ChannelPoolImpl};
use crate::client::{Middleware, TimeoutSettings};
use crate::client_common::DBCredentials;
use crate::client_scheme::list_types::SchemeEntry;
use crate::grpc::{grpc_read_operation_result, grpc_read_void_operation_result, operation_params};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_scheme_client::client::{
    RawMakeDirectoryRequest, RawRemoveDirectoryRequest,
};
use crate::grpc_wrapper::raw_scheme_client::list_directory_types::RawListDirectoryRequest;
use crate::grpc_wrapper::raw_services::Service;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use crate::{grpc_wrapper, Discovery, YdbResult};

pub(crate) type DirectoryServiceClientType = SchemeServiceClient<Middleware>;
pub(crate) type DirectoryServiceChannelPool = Arc<Box<dyn ChannelPool<DirectoryServiceClientType>>>;

pub struct SchemeClient {
    timeouts: TimeoutSettings,
    connection_manager: GrpcConnectionManager,
}

impl SchemeClient {
    pub(crate) fn new(
        timeouts: TimeoutSettings,
        connection_manager: GrpcConnectionManager,
    ) -> Self {
        Self {
            timeouts,
            connection_manager,
        }
    }

    pub async fn make_directory(&mut self, path: String) -> YdbResult<()> {
        let req = RawMakeDirectoryRequest {
            operation_params: RawOperationParams::new_with_timeout(self.timeouts.operation_timeout),
            path,
        };
        let mut service = self.connection().await?;
        service.make_directory(req).await?;
        return Ok(());
    }

    pub async fn list_directory(&mut self, path: String) -> YdbResult<Vec<SchemeEntry>> {
        let req = RawListDirectoryRequest {
            operation_params: RawOperationParams::new_with_timeout(self.timeouts.operation_timeout),
            path,
        };

        let mut service = self.connection().await?;
        let res = service.list_directory(req).await?;

        return Ok(res
            .children
            .into_iter()
            .map(|item| SchemeEntry::from(item))
            .collect());
    }

    pub async fn remove_directory(&mut self, path: String) -> YdbResult<()> {
        let req = RawRemoveDirectoryRequest {
            operation_params: RawOperationParams::new_with_timeout(self.timeouts.operation_timeout),
            path,
        };
        let mut service = self.connection().await?;
        service.remove_directory(req).await?;
        return Ok(());
    }

    async fn connection(&self) -> YdbResult<grpc_wrapper::raw_scheme_client::client::SchemeClient> {
        return self
            .connection_manager
            .get_auth_service(grpc_wrapper::raw_scheme_client::client::SchemeClient::new)
            .await;
    }
}
