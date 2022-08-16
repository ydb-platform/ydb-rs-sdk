use std::fmt::{write, Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tonic::transport::Channel;

pub(crate) type InterceptorResult<T> = std::result::Result<T, InterceptorError>;
pub(crate) type InterceptorRequest = http::Request<tonic::body::BoxBody>;

struct ServiceWithMultiInterceptor {
    inner: Channel,
    interceptors: Vec<Interceptor>,
}

impl ServiceWithMultiInterceptor {}

impl tower::Service<InterceptorRequest> for ServiceWithMultiInterceptor {
    type Response = ChannelResponse;
    type Error = InterceptorError;
    type Future = ChannelFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner
            .poll_ready(cx)
            .map_err(|err| InterceptorError::Transport(err))
    }

    fn call(&mut self, mut req: InterceptorRequest) -> Self::Future {
        for interceptor in self.interceptors.iter() {
            if let Some(interceptor) = &interceptor.on_call {
                req = match interceptor(req) {
                    Ok(res) => res,
                    Err(err) => return ChannelFuture::Error(Some(err)),
                }
            }
        }

        ChannelFuture::Future(self.inner.call(req))
    }
}

type ChannelResponse = <Channel as tower::Service<InterceptorRequest>>::Response;

enum ChannelFuture {
    Error(Option<InterceptorError>),
    Future(<Channel as tower::Service<InterceptorRequest>>::Future),
}

impl Future for ChannelFuture {
    type Output = std::result::Result<ChannelResponse, InterceptorError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
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
            ChannelFuture::Future(future) => {
                let res = Future::poll(Pin::new(future), cx);
                res.map_err(|err| InterceptorError::Transport(err))
            }
        };
        res
    }
}

struct Interceptor {
    on_call: Option<Box<OnCallInterceptor>>,
}

type OnCallInterceptor =
    dyn Fn(InterceptorRequest) -> InterceptorResult<InterceptorRequest> + Send + Sync;

pub(crate) struct MultiInterceptor {
    interceptors: Vec<Interceptor>,
}

impl MultiInterceptor {
    pub fn new() -> Self {
        Self {
            interceptors: Vec::new(),
        }
    }

    pub fn with_interceptor(mut self, interceptor: Interceptor) -> Self {
        self.interceptors.push(interceptor);
        self
    }

    fn on_call(&self, mut req: InterceptorRequest) -> InterceptorResult<InterceptorRequest> {
        for interceptor in self.interceptors.iter() {
            if let Some(interceptor) = &interceptor.on_call {
                req = interceptor(req)?
            }
        }

        Ok(req)
    }
}

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
