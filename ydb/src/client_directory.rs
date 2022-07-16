use std::sync::Arc;
use tracing::trace;

use ydb_grpc::ydb_proto::scheme::v1::scheme_service_client::SchemeServiceClient;
use ydb_grpc::ydb_proto::scheme::{
    Entry, ListDirectoryRequest, ListDirectoryResult, MakeDirectoryRequest, RemoveDirectoryRequest,
};

use crate::channel_pool::{ChannelPool, ChannelPoolImpl};
use crate::client::{Middleware, TimeoutSettings};
use crate::client_common::DBCredentials;
use crate::grpc::{grpc_read_operation_result, grpc_read_void_operation_result, operation_params};
use crate::grpc_wrapper::grpc_services::Service;
use crate::{Discovery, YdbResult};

pub(crate) type DirectoryServiceClientType = SchemeServiceClient<Middleware>;
pub(crate) type DirectoryServiceChannelPool = Arc<Box<dyn ChannelPool<DirectoryServiceClientType>>>;

pub struct SchemeClient {
    timeouts: TimeoutSettings,
    channel_pool: DirectoryServiceChannelPool,
}

impl SchemeClient {
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
            operation_params: operation_params(self.timeouts.operation_timeout),
            path,
        };
        trace!(
            "list directory request: {}",
            serde_json::to_string(&req).unwrap_or("bad json".into())
        );
        let resp = self
            .channel_pool
            .create_channel()
            .await?
            .list_directory(req)
            .await?;

        let result: ListDirectoryResult = grpc_read_operation_result(resp)?;
        Ok(result.children)
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
