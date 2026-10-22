use std::sync::Arc;

use crate::connection_pool::{Connection, ConnectionPool, RacyRoundRobin, Simple};
use crate::grpc_wrapper::grpc_limits::WithGrpcMaxMessageSize;
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::runtime_interceptors::{InterceptedChannel, MultiInterceptor};
use crate::load_balancer::{LoadBalancer, SharedLoadBalancer};
use crate::{GrpcOptions, YdbResult};
use http::Uri;
use std::fmt::{Debug, Formatter};
use tracing::instrument;

pub(crate) type GrpcConnectionManager = GrpcConnectionManagerGeneric<SharedLoadBalancer, Simple>;
pub(crate) type DiscoveryConnectionManager =
    GrpcConnectionManagerGeneric<NoBalancer, RacyRoundRobin>;

#[derive(Debug, Clone, Copy)]
pub(crate) struct NoBalancer;

pub(crate) struct GrpcConnectionManagerGeneric<BalancerT, ConnectionT: Connection> {
    balancer: BalancerT,
    connections_pool: Arc<ConnectionPool<ConnectionT>>,
    interceptor: MultiInterceptor,
    database: String,
    opts: GrpcOptions,
}

// `ConnectionT` is behind an `Arc`, so cloning does not require it to be
// `Clone` - only the balancer does.
impl<BalancerT: Clone, ConnectionT: Connection> Clone
    for GrpcConnectionManagerGeneric<BalancerT, ConnectionT>
{
    fn clone(&self) -> Self {
        Self {
            balancer: self.balancer.clone(),
            connections_pool: self.connections_pool.clone(),
            interceptor: self.interceptor.clone(),
            database: self.database.clone(),
            opts: self.opts.clone(),
        }
    }
}

impl<BalancerT: Debug, ConnectionT: Connection + Debug> Debug
    for GrpcConnectionManagerGeneric<BalancerT, ConnectionT>
{
    // `interceptor` is skipped: `MultiInterceptor` is not `Debug`.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcConnectionManagerGeneric")
            .field("balancer", &self.balancer)
            .field("connections_pool", &self.connections_pool)
            .field("database", &self.database)
            .field("opts", &self.opts)
            .finish()
    }
}

impl<BalancerT, ConnectionT: Connection> GrpcConnectionManagerGeneric<BalancerT, ConnectionT> {
    pub(crate) fn new(
        balancer: BalancerT,
        database: String,
        interceptor: MultiInterceptor,
        opts: GrpcOptions,
    ) -> Self {
        let cp = ConnectionPool::new(opts.clone());

        Self {
            balancer,
            connections_pool: cp.into(),
            interceptor,
            database,
            opts,
        }
    }

    #[instrument(name = "ydb.ConnectionManager.GetAuthService", skip_all, fields(db.system.name = "ydb", ydb.service.name = ?T::get_grpc_discovery_service()))]
    pub(crate) async fn get_auth_service<
        T: GrpcServiceForDiscovery + WithGrpcMaxMessageSize,
        F: FnOnce(InterceptedChannel) -> T,
    >(
        &self,
        new: F,
    ) -> YdbResult<T>
    where
        BalancerT: LoadBalancer,
    {
        let uri = self.balancer.endpoint(T::get_grpc_discovery_service())?;
        self.get_auth_service_to_node(new, &uri).await
    }

    #[instrument(name = "ydb.ConnectionManager.GetAuthServiceToNode", skip_all)]
    pub(crate) async fn get_auth_service_to_node<
        T: GrpcServiceForDiscovery + WithGrpcMaxMessageSize,
        F: FnOnce(InterceptedChannel) -> T,
    >(
        &self,
        new: F,
        uri: &Uri,
    ) -> YdbResult<T> {
        let channel = Box::pin(self.connections_pool.connection(uri)).await?;

        let intercepted_channel = InterceptedChannel::new(channel, self.interceptor.clone());
        Ok(new(intercepted_channel).with_grpc_max_message_size(self.opts.max_message_size))
    }

    #[instrument(name = "ydb.ConnectionManager.GetEndpoint", skip_all, fields(ydb.service.name = ?service))]
    pub(crate) fn endpoint(&self, service: Service) -> YdbResult<Uri>
    where
        BalancerT: LoadBalancer,
    {
        self.balancer.endpoint(service)
    }

    pub(crate) fn database(&self) -> &String {
        &self.database
    }

    pub(crate) fn max_message_size(&self) -> usize {
        self.opts.max_message_size
    }
}

#[cfg(test)]
mod manager_derive_tests {
    use super::*;
    use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};

    fn discovery_manager() -> DiscoveryConnectionManager {
        GrpcConnectionManagerGeneric::new(
            NoBalancer,
            "test-database".to_string(),
            MultiInterceptor::new(),
            GrpcOptions::default(),
        )
    }

    /// `Debug` is hand-written because `MultiInterceptor` is not `Debug`. Pin
    /// the fields it reports, and that the interceptor stays out of the output.
    #[test]
    fn debug_reports_configuration_without_interceptor() {
        let rendered = format!("{:?}", discovery_manager());

        assert!(
            rendered.starts_with("GrpcConnectionManagerGeneric {"),
            "unexpected shape: {rendered}"
        );
        assert!(
            rendered.contains("test-database"),
            "database missing: {rendered}"
        );
        assert!(
            rendered.contains("balancer"),
            "balancer missing: {rendered}"
        );
        assert!(
            rendered.contains("connections_pool"),
            "connections_pool missing: {rendered}"
        );

        assert!(
            !rendered.contains("interceptor"),
            "interceptor must stay out of Debug: {rendered}"
        );
    }

    /// `Clone` is bound on `BalancerT` only - the connection pool sits behind an
    /// `Arc`, so a non-`Clone` `ConnectionT` must not block cloning, and clones
    /// must keep sharing the same pool.
    #[test]
    fn clone_shares_the_connection_pool() {
        let manager = discovery_manager();
        let clone = manager.clone();

        assert!(Arc::ptr_eq(
            &manager.connections_pool,
            &clone.connections_pool
        ));
        assert_eq!(manager.database, clone.database);
    }

    /// The same `Clone` bound has to hold for the balancer used in production.
    #[test]
    fn clone_works_for_the_shared_balancer_manager() {
        let manager = GrpcConnectionManager::new(
            SharedLoadBalancer::new_with_balancer(Box::new(StaticLoadBalancer::new(
                Uri::from_static("http://127.0.0.1:2136/local"),
            ))),
            "local".to_string(),
            MultiInterceptor::new(),
            GrpcOptions::default(),
        );

        let clone = manager.clone();

        assert!(Arc::ptr_eq(
            &manager.connections_pool,
            &clone.connections_pool
        ));
    }
}
