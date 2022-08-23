use crate::client_common::DBCredentials;
use crate::discovery::Discovery;
use crate::errors::YdbResult;
use crate::grpc::create_grpc_client_with_error_sender;
use crate::grpc_wrapper::raw_services::Service;
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;
use crate::load_balancer::{LoadBalancer, SharedLoadBalancer};
use async_trait::async_trait;
use http::{Uri};
use std::collections::HashMap;


use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use tokio::sync::mpsc::UnboundedReceiver;


use tracing::trace;

#[async_trait]
pub(crate) trait ChannelPool<T>: Send + Sync
where
    T: Send,
{
    async fn create_channel(&self) -> YdbResult<T>;
}

pub(crate) struct ChannelErrorInfo {
    pub(crate) endpoint: Uri,
}

// TODO: implement Channel for Channel pool for drop-in replacements in grpc-clients
#[derive(Clone)]
pub(crate) struct ChannelPoolImpl<T>
where
    T: Clone,
{
    create_new_channel_fn: fn(InterceptedChannel) -> T,
    credentials: DBCredentials,
    load_balancer: SharedLoadBalancer,
    service: Service,
    shared_state: Arc<Mutex<SharedState<T>>>,
    channel_error_sender: mpsc::UnboundedSender<ChannelErrorInfo>,
}

struct SharedState<T> {
    channels: HashMap<Uri, T>,
}

impl<T> SharedState<T> {
    fn new() -> Self {
        Self {
            channels: HashMap::new(),
        }
    }
}

impl<T> ChannelPoolImpl<T>
where
    T: Clone,
{
    pub(crate) fn new<CB>(
        discovery: Arc<Box<dyn Discovery>>,
        credentials: DBCredentials,
        service: Service,
        create_new_channel_fn: fn(InterceptedChannel) -> T,
    ) -> Self {
        let load_balancer = SharedLoadBalancer::new(discovery.as_ref().as_ref());
        let (channel_error_sender, channel_error_receiver) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            Self::node_pessimization_loop(discovery, channel_error_receiver).await;
        });
        Self {
            create_new_channel_fn,
            credentials,
            load_balancer,
            service,
            shared_state: Arc::new(Mutex::new(SharedState::new())),
            channel_error_sender,
        }
    }

    fn get_channel_from_pool(&self, endpoint: &Uri) -> Option<T> {
        return if let Some(ch) = self.shared_state.lock().ok()?.channels.get(endpoint) {
            trace!("got channel from pool for {}", endpoint);
            Some(ch.clone())
        } else {
            None
        };
    }

    async fn node_pessimization_loop(
        discovery: Arc<Box<dyn Discovery>>,
        mut errors: UnboundedReceiver<ChannelErrorInfo>,
    ) {
        loop {
            if let Some(err) = errors.recv().await {
                discovery.pessimization(&err.endpoint)
            } else {
                return;
            };
        }
    }
}

#[async_trait]
impl<T> ChannelPool<T> for ChannelPoolImpl<T>
where
    T: Clone + Send,
{
    async fn create_channel(&self) -> YdbResult<T> {
        let endpoint = self.load_balancer.endpoint(self.service)?;
        return if let Some(ch) = self.get_channel_from_pool(&endpoint) {
            Ok(ch)
        } else {
            match create_grpc_client_with_error_sender(
                endpoint.clone(),
                self.credentials.clone(),
                Some(self.channel_error_sender.clone()),
                self.create_new_channel_fn,
            )
            .await
            {
                Ok(ch) => {
                    self.shared_state
                        .lock()?
                        .channels
                        .insert(endpoint, ch.clone());
                    Ok(ch)
                }
                Err(err) => Err(err),
            }
        };
    }
}

pub(crate) type ChannelErrorSender = Option<tokio::sync::mpsc::UnboundedSender<ChannelErrorInfo>>;
