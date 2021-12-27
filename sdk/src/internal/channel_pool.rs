use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use http::{Request, Uri};
use tonic::body::BoxBody;
use tonic::client::GrpcService;
use tonic::codegen::{Body, StdError};
use tonic::transport::Channel;
use tonic::transport::channel::ResponseFuture;
use crate::internal::load_balancer::{LoadBalancer, SharedLoadBalancer};
use crate::errors::Result;
use crate::internal::client_common::DBCredentials;
use crate::internal::discovery::Service;
use crate::internal::grpc::create_grpc_client;
use crate::internal::middlewares::AuthService;

#[derive(Clone)]
pub(crate) struct ChannelPool<T> where T:Clone{
    create_new_channel_fn: fn (AuthService) -> T,
    credentials: DBCredentials,
    load_balancer: SharedLoadBalancer,
    service: Service,
    shared_state: Arc<Mutex<SharedState<T>>>
}

struct SharedState<T> {
    channels: HashMap<Uri,T>,
}

impl<T> Default for SharedState<T> {
    fn default() -> Self {
        return Self{
            channels: HashMap::new(),
        }
    }
}

impl<T> ChannelPool<T> where T:Clone {
    pub(crate) fn new<CB>(load_balancer: SharedLoadBalancer, credentials: DBCredentials, service: Service, create_new_channel_fn: fn (AuthService) -> T) ->Self
    {
        return Self{
            create_new_channel_fn,
            credentials,
            load_balancer,
            service,
            shared_state: Arc::new(Mutex::new(SharedState::default())),
        }
    }

    pub(crate) fn create_channel(&self)->Result<T>{
        let endpoint = self.load_balancer.endpoint(self.service)?;
        return if let Some(ch) = self.get_channel_from_pool(&endpoint){
            Ok(ch)
        } else {
            match create_grpc_client(endpoint.clone(), self.credentials.clone(), self.create_new_channel_fn) {
                Ok(ch) => {
                    self.shared_state.lock()?.channels.insert(endpoint, ch.clone());
                    Ok(ch)
                }
                Err(err)=> Err(err)
            }
        };
    }

    fn get_channel_from_pool(&self, endpoint: &Uri)->Option<T>{
        return if let Some(ch) = self.shared_state.lock().ok()?.channels.get(endpoint) {
            println!("got channel from pool for {}", endpoint);
            Some(ch.clone())
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ChannelProxy {
    ch: Channel,
}

impl ChannelProxy {
    pub fn new(ch: Channel)->Self{
        return ChannelProxy{ch}
    }
}

impl tower::Service<http::Request<BoxBody>> for ChannelProxy {
    type Response = http::Response<tonic::transport::Body>;
    type Error = tonic::transport::Error;
    type Future = tonic::transport::channel::ResponseFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        return tower::Service::poll_ready(&mut self.ch, cx)
    }

    fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
        return tower::Service::call(&mut self.ch, req)
    }
}