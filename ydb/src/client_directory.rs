use std::sync::Arc;

use ydb_grpc::ydb_proto::scheme::{Entry, ListDirectoryRequest, ListDirectoryResult, MakeDirectoryRequest, RemoveDirectoryRequest};
use ydb_grpc::ydb_proto::scheme::v1::scheme_service_client::SchemeServiceClient;

use crate::{Discovery, YdbResult};
use crate::channel_pool::{ChannelPool, ChannelPoolImpl};
use crate::client::{Middleware, TimeoutSettings};
use crate::client_common::DBCredentials;
use crate::discovery::Service;
use crate::grpc::{grpc_read_operation_result, grpc_read_void_operation_result, operation_params};

pub(crate) type DirectoryServiceClientType = SchemeServiceClient<Middleware>;
pub(crate) type DirectoryServiceChannelPool = Arc<Box<dyn ChannelPool<DirectoryServiceClientType>>>;


pub struct DirectoryClient {
    timeouts: TimeoutSettings,
    channel_pool: DirectoryServiceChannelPool,
}

impl DirectoryClient {
    pub(crate) fn new(
        credentials: DBCredentials,
        discovery: Arc<Box<dyn Discovery>>,
        timeouts: TimeoutSettings,
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
        }
    }

    pub async fn make_directory(&mut self, path: String) -> YdbResult<()> {
        let resp = self
            .channel_pool
            .create_channel()
            .await?
            .make_directory(MakeDirectoryRequest {
                operation_params: operation_params(self.timeouts.operation_timeout),
                path,
            }).await?;

        grpc_read_void_operation_result(resp)
    }

    pub async fn list_directory(&mut self, path: String) -> YdbResult<Vec<Entry>> {
        let resp = self
            .channel_pool
            .create_channel()
            .await?
            .list_directory(ListDirectoryRequest {
                operation_params: operation_params(self.timeouts.operation_timeout),
                path,
            }).await?;

        let result: ListDirectoryResult = grpc_read_operation_result(resp)?;
        Ok(result.children)
    }

    pub async fn remove_directory(&mut self, path: String) -> YdbResult<()> {
        let resp = self
            .channel_pool
            .create_channel()
            .await?
            .remove_directory(RemoveDirectoryRequest {
                operation_params: operation_params(self.timeouts.operation_timeout),
                path,
            }).await?;

        grpc_read_void_operation_result(resp)
    }
}
