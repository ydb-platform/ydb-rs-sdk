use crate::client::TimeoutSettings;
use crate::client_scheme::list_types::SchemeEntry;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_scheme_client::client::{
    RawMakeDirectoryRequest, RawRemoveDirectoryRequest,
};
use crate::grpc_wrapper::raw_scheme_client::list_directory_types::RawListDirectoryRequest;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use crate::{grpc_wrapper, YdbResult};

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
        Ok(())
    }

    pub async fn list_directory(&mut self, path: String) -> YdbResult<Vec<SchemeEntry>> {
        let req = RawListDirectoryRequest {
            operation_params: RawOperationParams::new_with_timeout(self.timeouts.operation_timeout),
            path,
        };

        let mut service = self.connection().await?;
        let res = service.list_directory(req).await?;

        Ok(res.children.into_iter().collect())
    }

    pub async fn remove_directory(&mut self, path: String) -> YdbResult<()> {
        let req = RawRemoveDirectoryRequest {
            operation_params: RawOperationParams::new_with_timeout(self.timeouts.operation_timeout),
            path,
        };
        let mut service = self.connection().await?;
        service.remove_directory(req).await?;
        Ok(())
    }

    async fn connection(
        &self,
    ) -> YdbResult<grpc_wrapper::raw_scheme_client::client::RawSchemeClient> {
        self
            .connection_manager
            .get_auth_service(grpc_wrapper::raw_scheme_client::client::RawSchemeClient::new)
            .await
    }
}
