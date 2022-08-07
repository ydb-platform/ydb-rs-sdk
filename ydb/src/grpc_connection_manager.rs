use crate::client_common::DBCredentials;
use crate::connection_pool::ConnectionPool;
use crate::grpc_wrapper::auth::create_service_with_auth;
use crate::grpc_wrapper::channel::ChannelWithAuth;
use crate::grpc_wrapper::raw_services::GrpcServiceForDiscovery;
use crate::load_balancer::{LoadBalancer, SharedLoadBalancer};
use crate::YdbResult;
use http::Uri;

pub(crate) type GrpcConnectionManager = GrpcConnectionManagerGeneric<SharedLoadBalancer>;

#[derive(Clone)]
pub(crate) struct GrpcConnectionManagerGeneric<TBalancer: LoadBalancer> {
    state: State<TBalancer>,
}

impl<TBalancer: LoadBalancer> GrpcConnectionManagerGeneric<TBalancer> {
    pub(crate) fn new(balancer: TBalancer, cred: DBCredentials) -> Self {
        GrpcConnectionManagerGeneric {
            state: State::new(balancer, cred),
        }
    }

    pub(crate) async fn get_auth_service<
        T: GrpcServiceForDiscovery,
        F: FnOnce(ChannelWithAuth) -> T,
    >(
        &self,
        new: F,
    ) -> YdbResult<T> {
        let uri = self
            .state
            .balancer
            .endpoint(T::get_grpc_discovery_service())?;
        return self.get_auth_service_to_node(new, &uri).await;
    }

    pub(crate) async fn get_auth_service_to_node<
        T: GrpcServiceForDiscovery,
        F: FnOnce(ChannelWithAuth) -> T,
    >(
        &self,
        new: F,
        uri: &Uri,
    ) -> YdbResult<T> {
        let channel = self.state.connections_pool.connection(uri).await?;
        let auth_channel = create_service_with_auth(channel, self.state.cred.clone());
        Ok(new(auth_channel))
    }

    pub(crate) fn database(&self) -> &String {
        &self.state.cred.database
    }
}

#[derive(Clone)]
struct State<TBalancer: LoadBalancer> {
    balancer: TBalancer,
    connections_pool: ConnectionPool,
    cred: DBCredentials,
}

impl<TBalancer: LoadBalancer> State<TBalancer> {
    fn new(balancer: TBalancer, cred: DBCredentials) -> Self {
        State {
            balancer,
            connections_pool: ConnectionPool::new(),
            cred,
        }
    }
}
