use tracing::trace;

use crate::client::TimeoutSettings;
use crate::errors::YdbResult;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::session::{NodePinnedTableClient, Session};

use super::query_pool::{spawn_pool_release, QuerySessionPool};

/// Table-side adapter over the driver session pool.
#[derive(Clone)]
pub(crate) struct SessionPool {
    pool: QuerySessionPool,
    connection_manager: GrpcConnectionManager,
    timeouts: TimeoutSettings,
}

impl SessionPool {
    pub(crate) fn from_shared(
        pool: QuerySessionPool,
        connection_manager: GrpcConnectionManager,
        timeouts: TimeoutSettings,
    ) -> Self {
        Self {
            pool,
            connection_manager,
            timeouts,
        }
    }

    pub(crate) async fn session(&self) -> YdbResult<Session> {
        let mut lease = self.pool.acquire_explicit().await?;
        lease.ensure_alive()?;
        lease.begin_use();
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
            // Drop the in-use guard synchronously so AttachSession close can drain in-flight
            // table RPCs before async return_to_pool (return_to_pool also calls end_use).
            lease.end_use();
            spawn_pool_release(async move {
                lease.return_to_pool().await;
            });
        }));

        trace!("leased table session: {}", session.id);
        Ok(session)
    }
}

#[cfg(test)]
mod test {
    use super::SessionPool;
    use crate::client::TimeoutSettings;
    use crate::errors::YdbResult;
    use crate::grpc_connection_manager::GrpcConnectionManager;
    use crate::grpc_wrapper::grpc_limits::DEFAULT_GRPC_MESSAGE_SIZE_LIMIT_BYTES;
    use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
    use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};
    use crate::session_pool::{QuerySessionPool, SessionPoolSettings};
    use http::Uri;
    use std::time::Duration;
    use tokio::sync::oneshot;

    fn bench_pool() -> QuerySessionPool {
        QuerySessionPool::new_explicit_bench(
            SessionPoolSettings::new().with_limit(1).with_warm_up(1),
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
