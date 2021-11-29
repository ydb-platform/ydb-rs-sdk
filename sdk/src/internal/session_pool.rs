use async_trait::async_trait;
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

#[async_trait]
pub(crate) trait CreateSession: Send + Sync {
    async fn create_session(&self)->Result<Session>;
}

#[async_trait]
impl CreateSession for ChannelPool<TableServiceClient<Middleware>> {
    async fn create_session(&self)->Result<Session>{
        let mut client = self.create_channel()?;
        let session_res: CreateSessionResult = grpc_read_operation_result(
            client
                .create_session(CreateSessionRequest::default())
                .await?,
        )?;
        let session = Session::new(session_res.session_id);
        return Ok(session);
    }
}

#[derive(Clone)]
pub(crate) struct SessionPool {
    active_sessions: Arc<Semaphore>,
    create_session: Arc<Box<dyn CreateSession>>,
}

impl SessionPool {
    pub(crate) fn new(channel_pool: Box<dyn CreateSession>)-> Self {
        return Self {
            active_sessions: Arc::new(Semaphore::new(DEFAULT_SIZE)),
            create_session: Arc::new(channel_pool),
        };

    }

    pub(crate) fn with_max_active_sessions(mut self, size: usize)->Self {
        self.active_sessions = Arc::new(Semaphore::new(size));
        return self;
    }

    pub(crate) async fn session(&mut self) -> Result<Session> {
        let active_session_permit = self.active_sessions.clone().acquire_owned().await?;
        let mut session = self.create_session.create_session().await?;
        session.on_drop(Box::new( move|| {
            drop(active_session_permit);
        }));
        return Ok(session);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    struct  CreateSessionMock {}

    #[async_trait]
    impl CreateSession for CreateSessionMock {
        async fn create_session(&self)->Result<Session> {
            return Ok((Session::new("asd".into())))
        }
    }

    #[tokio::test]
    async fn max_active_session()->Result<()>{
        let mut pool = SessionPool::new(Box::new(CreateSessionMock{})).with_max_active_sessions(1);

        return Ok(());
    }
}