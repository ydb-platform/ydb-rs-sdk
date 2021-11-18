use crate::internal::load_balancer::{LoadBalancer, SharedLoadBalancer};
use crate::errors::Result;
use crate::internal::client_common::DBCredentials;
use crate::internal::discovery::Service;
use crate::internal::grpc::create_grpc_client;
use crate::internal::middlewares::AuthService;

#[derive(Clone)]
pub(crate) struct ChannelPool<T> {
    create_new_channel_fn: fn (AuthService) -> T,
    credentials: DBCredentials,
    load_balancer: SharedLoadBalancer,
    service: Service,
}

impl<T> ChannelPool<T> {
    pub(crate) fn new<CB>(load_balancer: SharedLoadBalancer, credentials: DBCredentials, service: Service, create_new_channel_fn: fn (AuthService) -> T) ->Self
    {
        return Self{
            create_new_channel_fn,
            credentials,
            load_balancer,
            service,
        }
    }

    pub(crate) fn create_channel(&self)->Result<T>{
        let endpoint = self.load_balancer.endpoint(self.service)?;
        return create_grpc_client(endpoint, self.credentials.clone(), self.create_new_channel_fn);
    }
}