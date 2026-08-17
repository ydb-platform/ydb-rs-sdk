use crate::client::TimeoutSettings;
use crate::client_lifetime::{ClientLifetime, ShutdownGuarded};
use crate::client_scheme::list_types::SchemeEntry;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_scheme_client::client::{
    RawMakeDirectoryRequest, RawRemoveDirectoryRequest,
};
use crate::grpc_wrapper::raw_scheme_client::describe_path_types::RawDescribePathRequest;
use crate::grpc_wrapper::raw_scheme_client::list_directory_types::RawListDirectoryRequest;

use crate::{YdbResult, grpc_wrapper};
use tracing::instrument;

#[derive(Clone)]
pub struct SchemeClient {
    inner: ShutdownGuarded<SchemeClientInner>,
}

#[derive(Clone)]
struct SchemeClientInner {
    timeouts: TimeoutSettings,
    connection_manager: GrpcConnectionManager,
}

impl SchemeClient {
    pub(crate) fn new(connection_manager: GrpcConnectionManager, lifetime: ClientLifetime) -> Self {
        Self {
            inner: lifetime.guard(SchemeClientInner {
                timeouts: TimeoutSettings::default(),
                connection_manager,
            }),
        }
    }

    #[instrument(name = "ydb.SchemeClient.MakeDirectory", skip_all, fields(db.system.name = "ydb", ydb.path = %path))]
    pub async fn make_directory(&mut self, path: String) -> YdbResult<()> {
        let inner = self.inner.access()?;
        let req = RawMakeDirectoryRequest {
            operation_params: inner.timeouts.operation_params(),
            path,
        };
        let mut service = inner.connection().await?;
        service.make_directory(req).await?;
        Ok(())
    }

    #[instrument(name = "ydb.SchemeClient.DescribePath", skip_all, fields(db.system.name = "ydb", ydb.path = %path))]
    pub async fn describe_path(&mut self, path: String) -> YdbResult<SchemeEntry> {
        let inner = self.inner.access()?;
        let req = RawDescribePathRequest {
            operation_params: inner.timeouts.operation_params(),
            path,
        };

        let mut service = inner.connection().await?;
        let res = service.describe_path(req).await?;

        Ok(res.entry)
    }

    #[instrument(name = "ydb.SchemeClient.ListDirectory", skip_all, fields(db.system.name = "ydb", ydb.path = %path))]
    pub async fn list_directory(&mut self, path: String) -> YdbResult<Vec<SchemeEntry>> {
        let inner = self.inner.access()?;
        let req = RawListDirectoryRequest {
            operation_params: inner.timeouts.operation_params(),
            path,
        };

        let mut service = inner.connection().await?;
        let res = service.list_directory(req).await?;

        Ok(res.children.into_iter().collect())
    }

    #[instrument(name = "ydb.SchemeClient.RemoveDirectory", skip_all, fields(db.system.name = "ydb", ydb.path = %path))]
    pub async fn remove_directory(&mut self, path: String) -> YdbResult<()> {
        let inner = self.inner.access()?;
        let req = RawRemoveDirectoryRequest {
            operation_params: inner.timeouts.operation_params(),
            path,
        };
        let mut service = inner.connection().await?;
        service.remove_directory(req).await?;
        Ok(())
    }
}

impl SchemeClientInner {
    async fn connection(
        &self,
    ) -> YdbResult<grpc_wrapper::raw_scheme_client::client::RawSchemeClient> {
        self.connection_manager
            .get_auth_service(grpc_wrapper::raw_scheme_client::client::RawSchemeClient::new)
            .await
    }
}
