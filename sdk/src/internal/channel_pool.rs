use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use http::{Request, Uri};
use tokio::sync::mpsc;
use tonic::body::BoxBody;
use tonic::transport::Channel;
use crate::internal::load_balancer::{LoadBalancer, SharedLoadBalancer};
use crate::errors::Result;
use crate::internal::client_common::DBCredentials;
use crate::internal::discovery::Service;
use crate::internal::grpc::{create_grpc_client, create_grpc_client_with_error_sender};
use crate::internal::middlewares::AuthService;

type ChannelErrorInfo=();

#[derive(Clone)]
pub(crate) struct ChannelPool<T> where T:Clone{
    create_new_channel_fn: fn (AuthService) -> T,
    credentials: DBCredentials,
    load_balancer: SharedLoadBalancer,
    service: Service,
    shared_state: Arc<Mutex<SharedState<T>>>,
    channel_error_sender: mpsc::Sender<ChannelErrorInfo>,
}

struct SharedState<T> {
    channels: HashMap<Uri,T>,
    channel_error_receiver: mpsc::Receiver<ChannelErrorInfo>,
}

impl<T> SharedState<T> {
    fn new(error_receiver: mpsc::Receiver<ChannelErrorInfo>)->Self{
        return Self{
            channels: HashMap::new(),
            channel_error_receiver: error_receiver,
        }
    }
}

impl<T> ChannelPool<T> where T:Clone {
    pub(crate) fn new<CB>(load_balancer: SharedLoadBalancer, credentials: DBCredentials, service: Service, create_new_channel_fn: fn (AuthService) -> T) ->Self
    {
        let (sender, receiver) = mpsc::channel(1);
        return Self{
            create_new_channel_fn,
            credentials,
            load_balancer,
            service,
            shared_state: Arc::new(Mutex::new(SharedState::new(receiver))),
            channel_error_sender: sender,
        }
    }

    pub(crate) fn create_channel(&self)->Result<T>{
        let endpoint = self.load_balancer.endpoint(self.service)?;
        return if let Some(ch) = self.get_channel_from_pool(&endpoint){
            Ok(ch)
        } else {
            match create_grpc_client_with_error_sender(endpoint.clone(), self.credentials.clone(),  Some(self.channel_error_sender.clone()), self.create_new_channel_fn) {
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

pub struct ChannelProxyFuture {
    inner: <Channel as tower::Service<http::Request<BoxBody>> >::Future,
    error_event: Option<tokio::sync::mpsc::Sender<()>>,
}

impl Future for ChannelProxyFuture {
    type Output = std::result::Result<ChannelResponse, ChannelError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res =  Future::poll(Pin::new(& mut self.inner), cx);
        if let (Poll::Ready(Err(_)), Some(sender)) = (&res, self.error_event.clone()) {
            tokio::spawn(async move {
                // TODO: tokio spawn - is workaround.
                // ideal way - async send message to sender and wait it here
                sender.send(()).await.ok();
            });
        }
        return res;
    }
}

pub(crate) type ChannelProxyErrorSender=Option<tokio::sync::mpsc::Sender<ChannelErrorInfo>>;

#[derive(Clone, Debug)]
pub(crate) struct ChannelProxy {
    ch: Channel,
    error_sender: ChannelProxyErrorSender
}

type ChannelResponse = <Channel as tower::Service<http::Request<BoxBody>> >::Response;
type ChannelError = <Channel as tower::Service<http::Request<BoxBody>> >::Error;

impl ChannelProxy {
    pub fn new(ch: Channel, error_sender: ChannelProxyErrorSender) ->Self{
        return ChannelProxy{
            ch, error_sender
        }
    }
}

impl tower::Service<http::Request<BoxBody>> for ChannelProxy {
    type Response = ChannelResponse;
    type Error = ChannelError;
    type Future = ChannelProxyFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        return tower::Service::poll_ready(&mut self.ch, cx)
    }

    fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
        return ChannelProxyFuture{
            inner: tower::Service::call(&mut self.ch, req),
            error_event: self.error_sender.clone(),
        }
    }
}
