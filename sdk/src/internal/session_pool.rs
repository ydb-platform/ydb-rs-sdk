use crate::errors::*;
use crate::internal::client_common::DBCredentials;
use crate::internal::client_fabric::Middleware;
use crate::internal::discovery::Service;
use crate::internal::grpc::{create_grpc_client, grpc_read_result};
use crate::internal::load_balancer::{LoadBalancer, SharedLoadBalancer};
use crate::internal::session::Session;
use ydb_protobuf::generated::ydb::table::v1::table_service_client::TableServiceClient;
use ydb_protobuf::generated::ydb::table::{CreateSessionRequest, CreateSessionResult};

#[derive(Clone)]
pub(crate) struct SessionPool {
    load_balancer: SharedLoadBalancer,
    credencials: DBCredentials,
}

impl SessionPool {
    fn grpc_client(&self) -> Result<TableServiceClient<Middleware>> {
        create_grpc_client(
            self.load_balancer.endpoint(Service::Table)?,
            self.credencials.clone(),
            TableServiceClient::new,
        )
    }

    pub(crate) fn new(load_balancer: SharedLoadBalancer, credencials: DBCredentials) -> Self {
        return Self {
            credencials,
            load_balancer,
        };
    }

    pub(crate) async fn session(&mut self) -> Result<Session> {
        let mut client = self.grpc_client()?;
        let session_res: CreateSessionResult = grpc_read_result(
            client
                .create_session(CreateSessionRequest::default())
                .await?,
        )?;
        return Ok(Session::new(client, session_res.session_id));
    }
}
