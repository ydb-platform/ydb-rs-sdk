use tracing::trace;

use crate::client::TimeoutSettings;
use crate::errors::YdbResult;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::retry_settings::RetrySettings;
use crate::session::TableSession;

use super::pool::SessionPool;

/// Table-side adapter over the driver session pool.
#[derive(Clone)]
pub(crate) struct TableSessionPool {
    pool: SessionPool,
    connection_manager: GrpcConnectionManager,
    retry_settings: RetrySettings,
}

impl TableSessionPool {
    pub(crate) fn from_shared(
        pool: SessionPool,
        connection_manager: GrpcConnectionManager,
        retry_settings: RetrySettings,
    ) -> Self {
        Self {
            pool,
            connection_manager,
            retry_settings,
        }
    }

    pub(crate) fn connection_manager(&self) -> &GrpcConnectionManager {
        &self.connection_manager
    }

    pub(crate) fn retry_settings(&self) -> &RetrySettings {
        &self.retry_settings
    }

    pub(crate) async fn session(&self) -> YdbResult<TableSession> {
        let lease = self.pool.acquire_explicit().await?;
        let session = TableSession::new(
            lease,
            self.connection_manager.clone(),
            TimeoutSettings::default(),
        );

        trace!("leased table session: {}", session.session_id());
        Ok(session)
    }
}

#[cfg(test)]
mod test {
    use super::TableSessionPool;
    use crate::GrpcOptions;
    use crate::errors::YdbResult;
    use crate::grpc_connection_manager::GrpcConnectionManager;
    use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
    use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};
    use crate::retry_settings::RetrySettings;
    use crate::session_pool::{SessionPool, SessionPoolSettings};
    use http::Uri;
    use std::time::Duration;
    use tokio::sync::oneshot;

    fn bench_pool() -> SessionPool {
        SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1).with_warm_up(1))
    }

    fn bench_connection_manager() -> GrpcConnectionManager {
        GrpcConnectionManager::new(
            SharedLoadBalancer::new_with_balancer(Box::new(StaticLoadBalancer::new(
                Uri::from_static("http://127.0.0.1/bench"),
            ))),
            "bench".to_string(),
            MultiInterceptor::new(),
            GrpcOptions::default(),
        )
    }

    #[tokio::test]
    async fn max_active_session() -> YdbResult<()> {
        let pool = TableSessionPool::from_shared(
            bench_pool(),
            bench_connection_manager(),
            RetrySettings::with_default_backoff(),
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

    #[tokio::test]
    async fn explicitly_returned_table_session_is_reused() -> YdbResult<()> {
        let pool = TableSessionPool::from_shared(
            bench_pool(),
            bench_connection_manager(),
            RetrySettings::with_default_backoff(),
        );
        let first = pool.session().await?;
        let first_id = first.session_id().to_string();

        first.return_to_pool();

        let second = pool.session().await?;
        assert_eq!(second.session_id(), first_id);
        Ok(())
    }
}
