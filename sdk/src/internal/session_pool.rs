use crate::errors::YdbError::Custom;
use crate::errors::*;
use crate::internal::client_table::TableServiceChannelPool;
use crate::internal::grpc::grpc_read_operation_result;
use crate::internal::session::Session;
use async_trait::async_trait;
use std::collections::vec_deque::VecDeque;
use std::ops::Sub;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::Semaphore;
use ydb_protobuf::generated::ydb::table::{
    CreateSessionRequest, CreateSessionResult, KeepAliveRequest, KeepAliveResult,
};

const DEFAULT_SIZE: usize = 1000;

#[async_trait]
pub(crate) trait SessionClient: Send + Sync {
    async fn create_session(&self) -> YdbResult<Session>;
}

#[async_trait]
impl SessionClient for TableServiceChannelPool {
    async fn create_session(&self) -> YdbResult<Session> {
        let mut channel = self.create_channel().await?;
        let session_res: CreateSessionResult = grpc_read_operation_result(
            channel
                .create_session(CreateSessionRequest::default())
                .await?,
        )?;
        let session = Session::new(session_res.session_id, self.clone());
        return Ok(session);
    }
}

type IdleSessions = Arc<Mutex<VecDeque<IdleSessionItem>>>;

#[derive(Clone)]
pub(crate) struct SessionPool {
    active_sessions: Arc<Semaphore>,
    create_session: Arc<Box<dyn SessionClient>>,
    idle_sessions: IdleSessions,
}

struct IdleSessionItem {
    idle_since: std::time::Instant,
    session: Session,
}

impl SessionPool {
    pub(crate) fn new(session_client: Box<dyn SessionClient>) -> Self {
        let pool = Self {
            active_sessions: Arc::new(Semaphore::new(DEFAULT_SIZE)),
            create_session: Arc::new(session_client),
            idle_sessions: Arc::new(Mutex::new(VecDeque::new())),
        };

        tokio::spawn(sessions_pinger(
            pool.create_session.clone(),
            Arc::downgrade(&pool.idle_sessions),
            std::time::Duration::from_secs(60),
        ));
        return pool;
    }

    pub(crate) fn with_max_active_sessions(mut self, size: usize) -> Self {
        self.active_sessions = Arc::new(Semaphore::new(size));
        return self;
    }

    pub(crate) async fn session(&self) -> YdbResult<Session> {
        let active_session_permit = self.active_sessions.clone().acquire_owned().await?;
        let idle_sessions = self.idle_sessions.clone();

        let mut session = {
            let idle_item = {
                // brackets need for drop mutex guard right after pop element: before start async await
                idle_sessions.lock()?.pop_front()
            };
            if let Some(idle_item) = idle_item {
                println!("got session from pool: {}", &idle_item.session.id);
                idle_item.session
            } else {
                let session = self.create_session.create_session().await?;
                println!("create session: {}", &session.id);
                session
            }
        };

        session.on_drop(Box::new(move |s: &mut Session| {
            println!("moved to pool: {}", s.id);
            let item = IdleSessionItem {
                idle_since: std::time::Instant::now(),
                session: s.clone_without_ondrop(),
            };
            idle_sessions.lock().unwrap().push_back(item);
            drop(active_session_permit);
        }));
        return Ok(session);
    }
}

async fn sessions_pinger(
    session_client: Arc<Box<dyn SessionClient>>,
    idle_sessions: Weak<Mutex<VecDeque<IdleSessionItem>>>,
    interval: std::time::Duration,
) {
    let mut sleep_time = interval;
    loop {
        tokio::time::sleep(sleep_time).await;
        let ping_since = std::time::Instant::now().sub(interval);
        {
            let idle_sessions = if let Some(idle_sessions) = idle_sessions.upgrade() {
                idle_sessions
            } else {
                return;
            };

            'sessions: loop {
                let mut session = {
                    let mut idle_sessions = idle_sessions.lock().unwrap();
                    if let Some(idle_item) = idle_sessions.front() {
                        if idle_item.idle_since <= ping_since {
                            idle_sessions.pop_front().unwrap().session
                        } else {
                            // wait until front session need to ping
                            sleep_time = ping_since.sub(idle_item.idle_since);
                            break 'sessions;
                        }
                    } else {
                        // empty queue
                        sleep_time = interval;
                        break 'sessions;
                    }
                };
                if session.keepalive().await.is_ok() && session.can_pooled {
                    let mut idle_sessions = idle_sessions.lock().unwrap();
                    idle_sessions.push_back(IdleSessionItem {
                        idle_since: std::time::Instant::now(),
                        session,
                    });
                } else {
                    drop(session)
                };
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::SessionClient;
    use crate::errors::{YdbError, YdbResult};
    use crate::internal::channel_pool::ChannelPool;
    use crate::internal::client_table::{TableServiceChannelPool, TableServiceClientType};
    use crate::internal::session::Session;
    use crate::internal::session_pool::SessionPool;
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::oneshot;

    struct SessionClientMock {}

    #[async_trait]
    impl SessionClient for SessionClientMock {
        async fn create_session(&self) -> YdbResult<Session> {
            return Ok(Session::new(
                "asd".into(),
                Arc::new(Box::new(TableChannelPoolMock {})),
            ));
        }
    }

    struct TableChannelPoolMock {}

    #[async_trait]
    impl ChannelPool<TableServiceClientType> for TableChannelPoolMock {
        async fn create_channel(&self) -> YdbResult<TableServiceClientType> {
            return Err(YdbError::Custom("test".into()));
        }
    }

    #[tokio::test]
    async fn max_active_session() -> YdbResult<()> {
        let mut pool = SessionPool::new(Box::new(SessionClientMock {})).with_max_active_sessions(1);
        let first_session = pool.session().await?;

        let (thread_started_sender, thread_started_receiver) = oneshot::channel();
        let (second_session_got_sender, mut second_session_got_receiver) = oneshot::channel();
        let mut cloned_pool = pool.clone();

        tokio::spawn(async move {
            thread_started_sender.send(true).unwrap();
            cloned_pool.session().await.unwrap();
            second_session_got_sender.send(true).unwrap();
        });

        thread_started_receiver.await?;

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(second_session_got_receiver.try_recv().is_err());

        drop(first_session);

        second_session_got_receiver.await?;

        return Ok(());
    }
}
