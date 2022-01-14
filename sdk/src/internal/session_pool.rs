use std::collections::vec_deque::VecDeque;
use std::ops::{Sub};
use async_trait::async_trait;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::Semaphore;
use crate::errors::*;
use crate::internal::client_fabric::Middleware;
use crate::internal::grpc::{grpc_read_operation_result};
use crate::internal::session::Session;
use ydb_protobuf::generated::ydb::table::v1::table_service_client::TableServiceClient;
use ydb_protobuf::generated::ydb::table::{CreateSessionRequest, CreateSessionResult, KeepAliveRequest, KeepAliveResult};
use crate::errors::Error::Custom;
use crate::internal::channel_pool::ChannelPool;

const DEFAULT_SIZE: usize = 1000;

#[async_trait]
pub(crate) trait SessionClient: Send + Sync {
    async fn create_session(&self)->Result<Session>;
    async fn keepalive_session(&self, session: &mut Session)->Result<()>;
}

#[async_trait]
impl SessionClient for ChannelPool<TableServiceClient<Middleware>> {
    async fn create_session(&self)->Result<Session>{
        let mut channel = self.create_channel().await?;
        let session_res: CreateSessionResult = grpc_read_operation_result(
            channel
                .create_session(CreateSessionRequest::default())
                .await?,
        )?;
        let session = Session::new(session_res.session_id);
        return Ok(session);
    }

    async fn keepalive_session(&self, session: &mut Session)->Result<()>{
        use ydb_protobuf::generated::ydb::table::keep_alive_result::SessionStatus;
        let mut channel = self.create_channel().await?;
        let keepalive_res: KeepAliveResult = session.handle_error(grpc_read_operation_result(channel.keep_alive(KeepAliveRequest{
            session_id: session.id.clone(),
            ..KeepAliveRequest::default()
        }).await?))?;
        if SessionStatus::from_i32(keepalive_res.session_status) == Some(SessionStatus::Ready) {
            return Ok(())
        }
        return Err(Custom(format!("bad status while session ping: {:?}", keepalive_res)))
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
    pub(crate) fn new(channel_pool: Box<dyn SessionClient>)-> Self {
        let pool = Self {
            active_sessions: Arc::new(Semaphore::new(DEFAULT_SIZE)),
            create_session: Arc::new(channel_pool),
            idle_sessions:Arc::new(Mutex::new(VecDeque::new())),
        };

        tokio::spawn(sessions_pinger(pool.create_session.clone(), Arc::downgrade(&pool.idle_sessions), std::time::Duration::from_secs(60)));
        return pool;
    }

    pub(crate) fn with_max_active_sessions(mut self, size: usize)->Self {
        self.active_sessions = Arc::new(Semaphore::new(size));
        return self;
    }

    pub(crate) async fn session(&mut self) -> Result<Session> {
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

        session.on_drop(Box::new( move|s: &mut Session| {
            println!("moved to pool: {}", s.id);
            let item = IdleSessionItem{
                idle_since: std::time::Instant::now(),
                session: s.clone_without_ondrop(),
            };
            idle_sessions.lock().unwrap().push_back(item);
            drop(active_session_permit);
        }));
        return Ok(session);
    }
}

async fn sessions_pinger(session_client: Arc<Box<dyn SessionClient>>, idle_sessions: Weak<Mutex<VecDeque<IdleSessionItem>>>, interval: std::time::Duration) {
    let mut sleep_time = interval;
    loop {
        tokio::time::sleep(sleep_time).await;
        let ping_since = std::time::Instant::now().sub(interval);
        {
            let idle_sessions = if let Some(idle_sessions) = idle_sessions.upgrade() {
                idle_sessions
            } else {
                return
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
                if session_client.keepalive_session(&mut session).await.is_ok() && session.can_pooled {
                    let mut idle_sessions = idle_sessions.lock().unwrap();
                    idle_sessions.push_back(IdleSessionItem{
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
    use std::time::Duration;
    use async_trait::async_trait;
    use tokio::sync::oneshot;
    use crate::internal::session::Session;
    use crate::errors::Result;
    use crate::internal::session_pool::SessionPool;
    use super::SessionClient;

    struct  SessionClientMock {}

    #[async_trait]
    impl SessionClient for SessionClientMock {
        async fn create_session(&self)->Result<Session> {
            return Ok(Session::new("asd".into()))
        }

        async fn keepalive_session(&self, _session: &mut Session)->Result<()>{
            return Ok(())
        }
    }

    #[tokio::test]
    async fn max_active_session()->Result<()>{
        let mut pool = SessionPool::new(Box::new(SessionClientMock{})).with_max_active_sessions(1);
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