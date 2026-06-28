use std::sync::Arc;

use tracing::trace;

use crate::client::TimeoutSettings;
use crate::discovery::Discovery;
use crate::errors::YdbResult;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::session::{NodePinnedTableClient, Session};

use super::query_pool::{
    spawn_pool_release, QuerySessionPool, QuerySessionPoolSettings, QuerySessionPoolStats,
};

const TABLE_DEFAULT_POOL_LIMIT: usize = 1000;

/// Table service session pool backed by Query Service CreateSession + AttachSession.
///
/// Session IDs from the query service are valid for table RPCs; AttachSession keeps sessions alive
/// without periodic Table KeepAlive calls.
#[derive(Clone)]
pub(crate) struct SessionPool {
    pool: QuerySessionPool,
    connection_manager: GrpcConnectionManager,
    discovery: Arc<Box<dyn Discovery>>,
    timeouts: TimeoutSettings,
}

impl SessionPool {
    pub(crate) fn new_default(
        connection_manager: GrpcConnectionManager,
        discovery: Arc<Box<dyn Discovery>>,
        timeouts: TimeoutSettings,
    ) -> Self {
        Self {
            pool: QuerySessionPool::new_explicit_sync(
                connection_manager.clone(),
                timeouts,
                discovery.clone(),
                QuerySessionPoolSettings::default().with_limit(TABLE_DEFAULT_POOL_LIMIT),
            ),
            connection_manager,
            discovery,
            timeouts,
        }
    }

    pub(crate) fn from_shared(
        pool: QuerySessionPool,
        connection_manager: GrpcConnectionManager,
        discovery: Arc<Box<dyn Discovery>>,
        timeouts: TimeoutSettings,
    ) -> Self {
        Self {
            pool,
            connection_manager,
            discovery,
            timeouts,
        }
    }

    pub(crate) async fn with_settings(
        connection_manager: GrpcConnectionManager,
        discovery: Arc<Box<dyn Discovery>>,
        timeouts: TimeoutSettings,
        settings: QuerySessionPoolSettings,
    ) -> YdbResult<Self> {
        let pool = QuerySessionPool::new_explicit(
            connection_manager.clone(),
            timeouts,
            discovery.clone(),
            settings,
        )
        .await?;
        Ok(Self {
            pool,
            connection_manager,
            discovery,
            timeouts,
        })
    }

    pub(crate) fn stats(&self) -> QuerySessionPoolStats {
        self.pool.stats()
    }

    pub(crate) fn connection_manager(&self) -> GrpcConnectionManager {
        self.connection_manager.clone()
    }

    pub(crate) fn discovery(&self) -> Arc<Box<dyn Discovery>> {
        self.discovery.clone()
    }

    pub(crate) fn with_max_active_sessions(self, size: usize) -> Self {
        Self {
            pool: QuerySessionPool::new_explicit_sync(
                self.connection_manager.clone(),
                self.timeouts,
                self.discovery.clone(),
                QuerySessionPoolSettings::default().with_limit(size),
            ),
            ..self
        }
    }

    pub(crate) async fn session(&self) -> YdbResult<Session> {
        let mut lease = self.pool.acquire_explicit().await?;
        let session_id = lease.session_id().to_string();
        let node_uri = lease.node_uri().clone();

        let mut session = Session::new(
            session_id,
            NodePinnedTableClient::new(self.connection_manager.clone(), node_uri),
            self.timeouts,
        );

        session.on_drop(Box::new(move |s: &mut Session| {
            if !s.can_pooled {
                lease.invalidate_session();
            }
            spawn_pool_release(async move {
                lease.return_to_pool().await;
            });
        }));

        trace!("leased table session: {}", session.id);
        Ok(session.with_timeouts(TimeoutSettings::default()))
    }
}

#[cfg(test)]
mod test {
    use super::SessionPool;
    use crate::client::TimeoutSettings;
    use crate::discovery::StaticDiscovery;
    use crate::errors::YdbResult;
    use crate::grpc_connection_manager::GrpcConnectionManager;
    use crate::grpc_wrapper::grpc_limits::DEFAULT_GRPC_MESSAGE_SIZE_LIMIT_BYTES;
    use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
    use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};
    use crate::session_pool::{QuerySessionPool, QuerySessionPoolSettings};
    use http::Uri;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::oneshot;

    fn bench_pool() -> QuerySessionPool {
        QuerySessionPool::new_explicit_bench(
            QuerySessionPoolSettings::new().with_limit(1).with_warm_up(1),
        )
    }

    fn bench_connection_manager() -> GrpcConnectionManager {
        GrpcConnectionManager::new(
            SharedLoadBalancer::new_with_balancer(Box::new(StaticLoadBalancer::new(
                Uri::from_static("http://127.0.0.1/bench"),
            ))),
            "bench".to_string(),
            MultiInterceptor::new(),
            None,
            DEFAULT_GRPC_MESSAGE_SIZE_LIMIT_BYTES,
        )
    }

    #[tokio::test]
    async fn max_active_session() -> YdbResult<()> {
        let pool = SessionPool::from_shared(
            bench_pool(),
            bench_connection_manager(),
            Arc::new(Box::new(
                StaticDiscovery::new_from_str("http://127.0.0.1/bench").unwrap(),
            )),
            TimeoutSettings::default(),
        );
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
