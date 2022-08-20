use crate::channel_pool::ChannelErrorInfo;
use crate::grpc_wrapper::runtime_interceptors::{
    ChannelResponse, GrpcInterceptor, InterceptorError, InterceptorRequest, InterceptorResult,
    RequestMetadata,
};
use crate::Discovery;
use http::uri::PathAndQuery;
use http::Uri;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::trace;

pub(crate) struct DiscoveryPessimizationInterceptor {
    sender: UnboundedSender<ChannelErrorInfo>,
}

impl DiscoveryPessimizationInterceptor {
    pub fn new(discovery: Arc<Box<dyn Discovery>>) -> Self {
        let (channel_error_sender, channel_error_receiver) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            Self::node_pessimization_loop(discovery, channel_error_receiver).await;
        });
        Self {
            sender: channel_error_sender,
        }
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

impl GrpcInterceptor for DiscoveryPessimizationInterceptor {
    fn on_call(
        &self,
        metadata: &mut RequestMetadata,
        req: InterceptorRequest,
    ) -> InterceptorResult<InterceptorRequest> {
        *metadata = Some(Box::new(req.uri().clone()));
        Ok(req)
    }

    fn on_feature_poll_ready(
        &self,
        metadata: &mut RequestMetadata,
        res: Result<ChannelResponse, InterceptorError>,
    ) -> Result<ChannelResponse, InterceptorError> {
        if res.is_err() {
            let uri = metadata
                .as_mut()
                .unwrap()
                .downcast_mut::<Uri>()
                .unwrap()
                .clone();

            let mut parts = uri.into_parts();
            parts.path_and_query = Some(PathAndQuery::from_static(""));
            let uri = Uri::from_parts(parts).map_err(|err| {
                InterceptorError::custom(format!(
                    "failed to trim uri path for send node pessimize err: '{:?}'",
                    err
                ))
            })?;

            fn result_to_str(res: Result<(), SendError<ChannelErrorInfo>>) -> &'static str {
                if res.is_ok() {
                    "OK"
                } else {
                    "receiver closed"
                }
            }

            let send_result = self.sender.send(ChannelErrorInfo {
                endpoint: uri.clone(),
            });
            trace!(
                "GrpcInterceptor sent error for uri: '{}' with result: {:?}",
                &uri,
                result_to_str(send_result)
            );
        };
        res
    }
}
