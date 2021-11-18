use crate::errors::*;
use crate::internal::client_common::DBCredentials;
use crate::internal::client_fabric::Middleware;
use crate::internal::discovery::Service;
use crate::internal::grpc::{create_grpc_client, grpc_read_result};
use crate::internal::load_balancer::{LoadBalancer, SharedLoadBalancer};
use crate::internal::session::Session;
use ydb_protobuf::generated::ydb::table::v1::table_service_client::TableServiceClient;
use ydb_protobuf::generated::ydb::table::{CreateSessionRequest, CreateSessionResult};
use crate::internal::channel_pool::ChannelPool;

#[derive(Clone)]
pub(crate) struct SessionPool {
    channel_pool: ChannelPool<TableServiceClient<Middleware>>
}

impl SessionPool {
    pub(crate) fn new(channel_pool: ChannelPool<TableServiceClient<Middleware>>, credencials: DBCredentials) -> Self {
        return Self {
            channel_pool,
        };
    }

    pub(crate) async fn session(&mut self) -> Result<Session> {
        let mut client = self.channel_pool.create_channel()?;
        let session_res: CreateSessionResult = grpc_read_result(
            client
                .create_session(CreateSessionRequest::default())
                .await?,
        )?;
        return Ok(Session::new(client, session_res.session_id));
    }
}
