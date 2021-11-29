use std::sync::{Arc};
use tokio::sync::Semaphore;
use crate::errors::*;
use crate::internal::client_fabric::Middleware;

use crate::internal::grpc::{grpc_read_operation_result};

use crate::internal::session::Session;
use ydb_protobuf::generated::ydb::table::v1::table_service_client::TableServiceClient;
use ydb_protobuf::generated::ydb::table::{CreateSessionRequest, CreateSessionResult};
use crate::internal::channel_pool::ChannelPool;

const DEFAULT_SIZE: usize = 1000;

#[derive(Clone)]
pub(crate) struct SessionPool {
    channel_pool: ChannelPool<TableServiceClient<Middleware>>,
    active_sessions: Arc<Semaphore>,
}

impl SessionPool {
    pub(crate) fn new(channel_pool: ChannelPool<TableServiceClient<Middleware>>) -> Self {
        return Self {
            channel_pool,
            active_sessions: Arc::new(Semaphore::new(DEFAULT_SIZE)),
        };
    }

    pub(crate) fn with_max_active_sessions(mut self, size: usize)->Self {
        self.active_sessions = Arc::new(Semaphore::new(size));
        return self;
    }

    pub(crate) async fn session(&mut self) -> Result<Session> {
        let active_session_permit = self.active_sessions.clone().acquire_owned().await?;
        let mut client = self.channel_pool.create_channel()?;
        let session_res: CreateSessionResult = grpc_read_operation_result(
            client
                .create_session(CreateSessionRequest::default())
                .await?,
        )?;
        let mut session = Session::new(client, session_res.session_id);
        session.on_drop(Box::new( move|| {
            drop(active_session_permit);
        }));
        return Ok(session);
    }
}
