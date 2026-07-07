use std::sync::Arc;

use crate::connection_pool::{Connection, ConnectionPool, RacyRoundRobin, Simple};
use crate::grpc_wrapper::grpc_limits::WithGrpcMaxMessageSize;
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::runtime_interceptors::{InterceptedChannel, MultiInterceptor};
use crate::load_balancer::{LoadBalancer, SharedLoadBalancer};
use crate::{GrpcOptions, YdbResult};
use derivative::Derivative;
use http::Uri;

pub(crate) type GrpcConnectionManager = GrpcConnectionManagerGeneric<SharedLoadBalancer, Simple>;
pub(crate) type DiscoveryConnectionManager =
    GrpcConnectionManagerGeneric<NoBalancer, RacyRoundRobin>;

#[derive(Debug, Clone, Copy)]
pub(crate) struct NoBalancer;

#[derive(Derivative)]
#[derivative(Clone(bound = "B: Clone"), Debug)]
pub(crate) struct GrpcConnectionManagerGeneric<B, C: Connection> {
    balancer: B,
    connections_pool: Arc<ConnectionPool<C>>,
    #[derivative(Debug = "ignore")]
    interceptor: MultiInterceptor,
    database: String,
    opts: GrpcOptions,
}

impl<B, C: Connection> GrpcConnectionManagerGeneric<B, C> {
    pub(crate) fn new(
        balancer: B,
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

    pub(crate) async fn get_auth_service<
        T: GrpcServiceForDiscovery + WithGrpcMaxMessageSize,
        F: FnOnce(InterceptedChannel) -> T,
    >(
        &self,
        new: F,
    ) -> YdbResult<T>
    where
        B: LoadBalancer,
    {
        let uri = self.balancer.endpoint(T::get_grpc_discovery_service())?;
        self.get_auth_service_to_node(new, &uri).await
    }

    pub(crate) async fn get_auth_service_to_node<
        T: GrpcServiceForDiscovery + WithGrpcMaxMessageSize,
        F: FnOnce(InterceptedChannel) -> T,
    >(
        &self,
        new: F,
        uri: &Uri,
    ) -> YdbResult<T> {
        let channel = self.connections_pool.connection(uri).await?;

        let intercepted_channel = InterceptedChannel::new(channel, self.interceptor.clone());
        Ok(new(intercepted_channel).with_grpc_max_message_size(self.opts.max_message_size))
    }

    pub(crate) fn endpoint(&self, service: Service) -> YdbResult<Uri>
    where
        B: LoadBalancer,
    {
        self.balancer.endpoint(service)
    }

    pub(crate) fn database(&self) -> &String {
        &self.database
    }
}
