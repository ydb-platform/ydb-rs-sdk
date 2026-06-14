use std::collections::HashMap;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

use crate::types::Value;

use super::exec::{client_begin_stream, CallOptions, ClientExecContext};
use super::QueryStream;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub enum Streamed {}

pub type QueryStreamBuilder<'a> = CallBuilder<'a, Streamed>;

pub struct CallBuilder<'a, K> {
    ctx: &'a mut ClientExecContext,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
    _kind: PhantomData<fn() -> K>,
}

impl<'a, K> CallBuilder<'a, K> {
    pub(crate) fn new(ctx: &'a mut ClientExecContext, text: String) -> Self {
        Self {
            ctx,
            text,
            params: HashMap::new(),
            opts: CallOptions::default(),
            _kind: PhantomData,
        }
    }

    pub fn param(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.params.insert(name.into(), value.into());
        self
    }

    pub fn params(mut self, params: HashMap<String, Value>) -> Self {
        self.params.extend(params);
        self
    }

    /// Per-call operation timeout (opening the gRPC stream only).
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.opts.timeout = Some(timeout);
        self
    }

    pub fn idempotent(mut self, idempotent: bool) -> Self {
        self.opts.idempotent = Some(idempotent);
        self
    }

    pub fn collect_stats(mut self) -> Self {
        self.opts.collect_stats = true;
        self
    }
}

impl<'a> IntoFuture for CallBuilder<'a, Streamed> {
    type Output = crate::errors::YdbResult<QueryStream>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let stream = client_begin_stream(self.ctx, self.text, self.params, self.opts).await?;
            Ok(QueryStream { stream })
        })
    }
}

pub trait QueryExecutor {
    fn query(&mut self, text: impl Into<String>) -> QueryStreamBuilder<'_>;
}
