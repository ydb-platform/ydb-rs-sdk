use itertools::enumerate;
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::transport::Channel;

pub(crate) type InterceptorResult<T> = std::result::Result<T, InterceptorError>;
pub(crate) type InterceptorRequest = http::Request<tonic::body::BoxBody>;

#[derive(Clone)]
pub(crate) struct InterceptedChannel {
    inner: Channel,
    interceptor: MultiInterceptor,
}

impl InterceptedChannel {
    pub fn new(channel: Channel, interceptor: MultiInterceptor) -> Self {
        Self {
            inner: channel,
            interceptor,
        }
    }
}

impl tower::Service<InterceptorRequest> for InterceptedChannel {
    type Response = ChannelResponse;
    type Error = InterceptorError;
    type Future = ChannelFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner
            .poll_ready(cx)
            .map_err(InterceptorError::Transport)
    }

    fn call(&mut self, mut req: InterceptorRequest) -> Self::Future {
        let mut metadata: RequestMetadata = None;
        req = match self.interceptor.on_call(&mut metadata, req) {
            Ok(res) => res,
            Err(err) => return ChannelFuture::Error(Some(err)),
        };

        ChannelFuture::Future(ChannelFutureState {
            channel_future: self.inner.call(req),
            interceptor: self.interceptor.clone(),
            metadata,
        })
    }
}

impl Debug for InterceptedChannel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "channel: {:?}, incerceptors count: {}",
            self.inner,
            self.interceptor.interceptors.len()
        )
    }
}

pub(crate) type ChannelResponse = <Channel as tower::Service<InterceptorRequest>>::Response;

pub(crate) enum ChannelFuture {
    Error(Option<InterceptorError>),
    Future(ChannelFutureState),
}

pub(crate) struct ChannelFutureState {
    channel_future: <Channel as tower::Service<InterceptorRequest>>::Future,
    interceptor: MultiInterceptor,
    metadata: RequestMetadata,
}

impl Future for ChannelFuture {
    type Output = std::result::Result<ChannelResponse, InterceptorError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res: Poll<Self::Output> = match self.get_mut() {
            ChannelFuture::Error(None) => Poll::Ready(Err(InterceptorError::internal(
                "interceptor error is empty",
            ))),
            ChannelFuture::Error(err) => {
                let err_content = err.take().unwrap();
                *err = Some(InterceptorError::Internal(format!(
                    "interceptor err consumed already, prev err: '{}'",
                    err_content
                )));
                Poll::Ready(Err(err_content))
            }
            ChannelFuture::Future(state) => {
                let poll_res = Future::poll(Pin::new(&mut state.channel_future), cx);

                match poll_res {
                    Poll::Ready(res) => {
                        let mut res = res.map_err(InterceptorError::Transport);
                        res = state
                            .interceptor
                            .on_feature_poll_ready(&mut state.metadata, res);
                        res.into()
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        };
        res
    }
}

pub(crate) trait GrpcInterceptor: Send + Sync {
    fn on_call(
        &self,
        _metadata: &mut RequestMetadata,
        req: InterceptorRequest,
    ) -> InterceptorResult<InterceptorRequest> {
        Ok(req)
    }

    fn on_feature_poll_ready(
        &self,
        _metadata: &mut RequestMetadata,
        res: Result<ChannelResponse, InterceptorError>,
    ) -> Result<ChannelResponse, InterceptorError> {
        res
    }
}

#[derive(Clone)]
pub(crate) struct MultiInterceptor {
    interceptors: Vec<Arc<Box<dyn GrpcInterceptor>>>,
}

impl MultiInterceptor {
    pub fn new() -> Self {
        Self {
            interceptors: Vec::new(),
        }
    }

    pub fn with_interceptor<T: GrpcInterceptor + 'static>(mut self, interceptor: T) -> Self {
        let boxed_interceptor: Box<dyn GrpcInterceptor> = Box::new(interceptor);
        let arc_boxed_interceptor = Arc::new(boxed_interceptor);
        self.interceptors.push(arc_boxed_interceptor);
        self
    }
}

impl GrpcInterceptor for MultiInterceptor {
    fn on_call(
        &self,
        metadata: &mut RequestMetadata,
        mut req: InterceptorRequest,
    ) -> InterceptorResult<InterceptorRequest> {
        let mut metadatas: Vec<RequestMetadata> = Vec::new();
        metadatas.resize_with(self.interceptors.len(), || None);
        for (index, interceptor) in enumerate(self.interceptors.iter()) {
            req = interceptor.on_call(&mut metadatas[index], req)?;
        }
        *metadata = Some(Box::new(metadatas));
        Ok(req)
    }

    fn on_feature_poll_ready(
        &self,
        metadata: &mut RequestMetadata,
        mut res: Result<ChannelResponse, InterceptorError>,
    ) -> Result<ChannelResponse, InterceptorError> {
        let metadata = metadata
            .as_mut()
            .unwrap()
            .downcast_mut::<Vec<RequestMetadata>>()
            .unwrap();

        for (index, interceptor) in enumerate(self.interceptors.iter()) {
            let item_meta = &mut metadata[index];
            res = interceptor.on_feature_poll_ready(item_meta, res)
        }
        res
    }
}

pub(crate) type RequestMetadata = Option<Box<dyn Any + Send>>;

pub(crate) enum InterceptorError {
    Custom(String),
    Internal(String),
    Transport(tonic::transport::Error),
}

impl InterceptorError {
    pub fn custom<S: Into<String>>(text: S) -> Self {
        InterceptorError::Custom(text.into())
    }

    pub fn internal<S: Into<String>>(text: S) -> Self {
        InterceptorError::Internal(text.into())
    }
}

impl Debug for InterceptorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use InterceptorError::*;

        match self {
            Custom(text) => write!(f, "interceptor custom error: '{}'", text),
            Internal(text) => write!(f, "interceptor internal error: '{}'", text),
            Transport(err) => write!(f, "interceptor transport error: {:?}", err),
        }
    }
}

impl Display for InterceptorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        <Self as Debug>::fmt(self, f)
    }
}

impl std::error::Error for InterceptorError {}
