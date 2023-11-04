use crate::client::TimeoutSettings;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_coordination_service::alter_node::RawAlterNodeRequest;
use crate::grpc_wrapper::raw_coordination_service::common::config::RawCoordinationNodeConfig;
use crate::grpc_wrapper::raw_coordination_service::create_node::RawCreateNodeRequest;
use crate::grpc_wrapper::raw_coordination_service::describe_node::RawDescribeNodeRequest;
use crate::grpc_wrapper::raw_coordination_service::drop_node::RawDropNodeRequest;
use crate::{grpc_wrapper, YdbResult};

use super::list_types::{NodeConfig, NodeDescription};

pub struct CoordinationClient {
    timeouts: TimeoutSettings,
    connection_manager: GrpcConnectionManager,
}

impl CoordinationClient {
    pub(crate) fn new(
        timeouts: TimeoutSettings,
        connection_manager: GrpcConnectionManager,
    ) -> Self {
        Self {
            timeouts,
            connection_manager,
        }
    }

    pub async fn create_node(&mut self, path: String, config: NodeConfig) -> YdbResult<()> {
        let req = RawCreateNodeRequest {
            config: RawCoordinationNodeConfig::from(config),
            operation_params: self.timeouts.operation_params(),
            path,
        };

        let mut service = self.raw_client_connection().await?;
        service.create_node(req).await?;

        Ok(())
    }

    pub async fn alter_node(&mut self, path: String, config: NodeConfig) -> YdbResult<()> {
        let req = RawAlterNodeRequest {
            config: RawCoordinationNodeConfig::from(config),
            operation_params: self.timeouts.operation_params(),
            path,
        };

        let mut service = self.raw_client_connection().await?;
        service.alter_node(req).await?;

        Ok(())
    }

    pub async fn describe_node(&mut self, path: String) -> YdbResult<NodeDescription> {
        let req = RawDescribeNodeRequest {
            operation_params: self.timeouts.operation_params(),
            path,
        };

        let mut service = self.raw_client_connection().await?;
        let result = service.describe_node(req).await?;
        let description = NodeDescription::from(result);

        Ok(description)
    }

    pub async fn drop_node(&mut self, path: String) -> YdbResult<()> {
        let req = RawDropNodeRequest {
            operation_params: self.timeouts.operation_params(),
            path,
        };

        let mut service = self.raw_client_connection().await?;
        service.drop_node(req).await?;

        Ok(())
    }

    pub(crate) async fn raw_client_connection(
        &self,
    ) -> YdbResult<grpc_wrapper::raw_coordination_service::client::RawCoordinationClient> {
        self.connection_manager
            .get_auth_service(
                grpc_wrapper::raw_coordination_service::client::RawCoordinationClient::new,
            )
            .await
    }
}
