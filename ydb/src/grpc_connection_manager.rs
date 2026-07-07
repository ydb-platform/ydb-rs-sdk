use crate::YdbResult;
use crate::connection_pool::ConnectionPool;
use crate::grpc_wrapper::grpc_limits::WithGrpcMaxMessageSize;
use crate::grpc_wrapper::raw_services::{GrpcServiceForDiscovery, Service};
use crate::grpc_wrapper::runtime_interceptors::{InterceptedChannel, MultiInterceptor};
use crate::load_balancer::{LoadBalancer, SharedLoadBalancer};
use http::Uri;

pub(crate) type GrpcConnectionManager = GrpcConnectionManagerGeneric<SharedLoadBalancer>;

#[derive(Clone)]
pub(crate) struct GrpcConnectionManagerGeneric<TBalancer: LoadBalancer> {
    state: State<TBalancer>,
}

impl<TBalancer: LoadBalancer> GrpcConnectionManagerGeneric<TBalancer> {
    pub(crate) fn new(
        balancer: TBalancer,
        database: String,
        interceptor: MultiInterceptor,
        cert_path: Option<String>,
        grpc_max_message_size: usize,
    ) -> Self {
        GrpcConnectionManagerGeneric {
            state: State::new(
                balancer,
                database,
                interceptor,
                cert_path,
                grpc_max_message_size,
            ),
        }
    }

    pub(crate) async fn get_auth_service<
        T: GrpcServiceForDiscovery + WithGrpcMaxMessageSize,
        F: FnOnce(InterceptedChannel) -> T,
    >(
        &self,
        new: F,
    ) -> YdbResult<T> {
        let uri = self
            .state
            .balancer
            .endpoint(T::get_grpc_discovery_service())?;
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
        let channel = self.state.connections_pool.connection(uri).await?;

        let intercepted_channel = InterceptedChannel::new(channel, self.state.interceptor.clone());
        Ok(new(intercepted_channel).with_grpc_max_message_size(self.state.grpc_max_message_size))
    }

    pub(crate) fn endpoint(&self, service: Service) -> YdbResult<Uri> {
        self.state.balancer.endpoint(service)
    }

    pub(crate) fn database(&self) -> &String {
        &self.state.database
    }
}

#[derive(Clone)]
struct State<TBalancer: LoadBalancer> {
    balancer: TBalancer,
    connections_pool: ConnectionPool,
    interceptor: MultiInterceptor,
    database: String,
    grpc_max_message_size: usize,
}

impl<TBalancer: LoadBalancer> State<TBalancer> {
    fn new(
        balancer: TBalancer,
        database: String,
        interceptor: MultiInterceptor,
        cert_path: Option<String>,
        grpc_max_message_size: usize,
    ) -> Self {
        let mut cp = ConnectionPool::new();
        if let Some(cert_path) = cert_path {
            cp = cp.load_certificate(cert_path);
        }

        State {
            balancer,
            connections_pool: cp,
            interceptor,
            database,
            grpc_max_message_size,
        }
    }
}
