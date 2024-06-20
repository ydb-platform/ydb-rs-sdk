use crate::client::TimeoutSettings;
use crate::errors::*;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_table_service::client::RawTableClient;
use crate::session::Session;
use async_trait::async_trait;
use std::collections::vec_deque::VecDeque;
use std::ops::{Add, Sub};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::Semaphore;
use tracing::trace;

const DEFAULT_SIZE: usize = 1000;

#[async_trait]
pub(crate) trait SessionFabric: Send + Sync {
    async fn create_session(&self, timeouts: TimeoutSettings) -> YdbResult<Session>;
}

#[async_trait]
impl SessionFabric for GrpcConnectionManager {
    async fn create_session(&self, timeouts: TimeoutSettings) -> YdbResult<Session> {
        let mut table = self
            .get_auth_service(RawTableClient::new)
            .await?
            .with_timeout(timeouts);
        let session_res = table.create_session().await?;
        let session = Session::new(session_res.id, self.clone(), TimeoutSettings::default());
        return Ok(session);
    }
}

type IdleSessions = Arc<Mutex<VecDeque<IdleSessionItem>>>;

#[derive(Clone)]
pub(crate) struct SessionPool {
    active_sessions: Arc<Semaphore>,
    create_session: Arc<Box<dyn SessionFabric>>,
    idle_sessions: IdleSessions,
    timeouts: TimeoutSettings,
}

impl SessionPool {
    pub(crate) fn new(session_client: Box<dyn SessionFabric>, timeouts: TimeoutSettings) -> Self {
        let pool = Self {
            active_sessions: Arc::new(Semaphore::new(DEFAULT_SIZE)),
            create_session: Arc::new(session_client),
            idle_sessions: Arc::new(Mutex::new(VecDeque::new())),
            timeouts,
        };

        tokio::spawn(sessions_pinger(
            pool.create_session.clone(),
            Arc::downgrade(&pool.idle_sessions),
            std::time::Duration::from_secs(60),
        ));
        pool
    }

    pub(crate) fn with_max_active_sessions(mut self, size: usize) -> Self {
        self.active_sessions = Arc::new(Semaphore::new(size));
        self
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
                trace!("got session from pool: {}", &idle_item.session.id);
                idle_item.session
            } else {
                let session = self.create_session.create_session(self.timeouts).await?;
                trace!("create session: {}", &session.id);
                session
            }
        };

        session.on_drop(Box::new(move |s: &mut Session| {
            trace!("moved to pool: {}", s.id);
            let item = IdleSessionItem {
                idle_since: tokio::time::Instant::now(),
                session: s.clone_without_ondrop(),
            };
            idle_sessions.lock().unwrap().push_back(item);
            drop(active_session_permit);
        }));
        session = session.with_timeouts(TimeoutSettings::default());
        Ok(session)
    }
}

struct IdleSessionItem {
    idle_since: tokio::time::Instant,
    session: Session,
}

async fn sessions_pinger(
    _session_client: Arc<Box<dyn SessionFabric>>,
    idle_sessions: Weak<Mutex<VecDeque<IdleSessionItem>>>,
    interval: std::time::Duration,
) {
    let mut sleep_until = tokio::time::Instant::now().add(interval);
    loop {
        tokio::time::sleep_until(sleep_until).await;
        let now = tokio::time::Instant::now();
        let ping_since = now.sub(interval);
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
                            sleep_until = idle_item.idle_since.add(interval);
                            break 'sessions;
                        }
                    } else {
                        // empty queue
                        sleep_until = now.add(interval);
                        break 'sessions;
                    }
                };
                if session.keepalive().await.is_ok() && session.can_pooled {
                    let mut idle_sessions = idle_sessions.lock().unwrap();
                    idle_sessions.push_back(IdleSessionItem {
                        idle_since: tokio::time::Instant::now(),
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
    use super::SessionFabric;
    use crate::client::TimeoutSettings;

    use crate::errors::{YdbError, YdbResult};
    use crate::grpc_wrapper::raw_table_service::client::RawTableClient;
    use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;
    use crate::session::{CreateTableClient, Session};
    use crate::session_pool::SessionPool;
    use async_trait::async_trait;

    use std::time::Duration;
    use tokio::sync::oneshot;
    use ydb_grpc::ydb_proto::table::v1::table_service_client::TableServiceClient;

    struct SessionClientMock {}

    #[async_trait]
    impl SessionFabric for SessionClientMock {
        async fn create_session(&self, timeouts: TimeoutSettings) -> YdbResult<Session> {
            Ok(Session::new(
                "asd".into(),
                TableChannelPoolMock {},
                timeouts,
            ))
        }
    }

    struct TableChannelPoolMock {}

    #[async_trait]
    impl CreateTableClient for TableChannelPoolMock {
        async fn create_grpc_table_client(
            &self,
        ) -> YdbResult<TableServiceClient<InterceptedChannel>> {
            Err(YdbError::Custom("test".into()))
        }

        async fn create_table_client(
            &self,
            _timeouts: TimeoutSettings,
        ) -> YdbResult<RawTableClient> {
            Err(YdbError::Custom("test".into()))
        }

        fn clone_box(&self) -> Box<dyn CreateTableClient> {
            Box::new(TableChannelPoolMock {})
        }
    }

    #[tokio::test]
    async fn max_active_session() -> YdbResult<()> {
        let pool = SessionPool::new(Box::new(SessionClientMock {}), TimeoutSettings::default())
            .with_max_active_sessions(1);
        let first_session = pool.session().await?;

        let (thread_started_sender, thread_started_receiver) = oneshot::channel();
        let (second_session_got_sender, mut second_session_got_receiver) = oneshot::channel();
        let cloned_pool = pool.clone();

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

        Ok(())
    }
}
