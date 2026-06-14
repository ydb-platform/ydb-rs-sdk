use std::collections::HashMap;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

use crate::types::Value;

use super::exec::CallOptions;
use super::internal::{ExecCoreRef, HasCore};
use super::stream_facade::QueryStream;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub enum Streamed {}

pub type QueryStreamBuilder<'a> = CallBuilder<'a, Streamed>;

pub struct CallBuilder<'a, K> {
    core: ExecCoreRef<'a>,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
    _kind: PhantomData<fn() -> K>,
}

impl<'a, K> CallBuilder<'a, K> {
    pub(crate) fn new(core: ExecCoreRef<'a>, text: String) -> Self {
        Self {
            core,
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

    /// Per-call operation timeout.
    ///
    /// For [`QueryStream`](Self) the timeout applies only while opening the gRPC
    /// stream; iterating result sets is not bounded by this value.
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
    type Output = crate::errors::YdbResult<QueryStream<'a>>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            let stream = self
                .core
                .begin_stream(self.text, self.params, self.opts)
                .await?;
            Ok(QueryStream {
                core: self.core,
                stream,
            })
        })
    }
}

#[allow(private_bounds)]
pub trait QueryExecutor: HasCore {
    fn query(&mut self, text: impl Into<String>) -> QueryStreamBuilder<'_> {
        CallBuilder::new(self.core_mut(), text.into())
    }
}

macro_rules! impl_query_methods {
    () => {
        pub fn query(&mut self, text: impl Into<String>) -> QueryStreamBuilder<'_> {
            QueryExecutor::query(self, text)
        }
    };
}

pub(crate) use impl_query_methods;
