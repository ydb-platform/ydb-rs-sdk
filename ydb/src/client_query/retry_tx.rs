use std::future::IntoFuture;
use std::marker::PhantomData;
use std::time::Duration;

use futures_util::future::BoxFuture;

use crate::Transaction;
use crate::TransactionOptions;
use crate::TxMode;
use crate::async_closure::AsyncFnMut;
use crate::async_closure::DynAsyncFnMut;
use crate::async_closure::with_lifetime::Mut;
use crate::errors::YdbResultWithCustomerErr;

use super::QueryClient;

/// Builder for [`QueryClient::retry_tx`].
pub struct RetryTxBuilder<'a, F, T> {
    client: &'a QueryClient,
    callback: F,
    options: TransactionOptions,
    timeout: Option<Duration>,
    idempotent: bool,
    _phantom: PhantomData<fn() -> T>,
}

/// Auxiliary trait for closures that can be passed into [`QueryClient::retry_tx`].
pub trait RetryTxAttempt<T>: Send {
    fn attempt<'a>(
        &'a mut self,
        tx: &'a mut Transaction,
    ) -> BoxFuture<'a, YdbResultWithCustomerErr<T>>;
}

impl<'c, T> RetryTxAttempt<T> for DynAsyncFnMut<'c, Mut<Transaction>, YdbResultWithCustomerErr<T>> {
    fn attempt<'a>(
        &'a mut self,
        tx: &'a mut Transaction,
    ) -> BoxFuture<'a, YdbResultWithCustomerErr<T>> {
        self.call(tx)
    }
}

impl<'a, F, T> RetryTxBuilder<'a, F, T> {
    pub(crate) fn new(client: &'a QueryClient, callback: F) -> Self {
        Self {
            client,
            callback,
            options: TransactionOptions::default(),
            timeout: None,
            idempotent: false,
            _phantom: PhantomData,
        }
    }

    /// Transaction isolation mode (default: [`TxMode::SerializableReadWrite`]).
    pub fn with_mode(mut self, mode: TxMode) -> Self {
        self.options = self.options.with_mode(mode);
        self
    }

    /// Alias for [`Self::with_mode`].
    pub fn isolation(self, mode: TxMode) -> Self {
        self.with_mode(mode)
    }

    /// Call `BeginTransaction` RPC before the first `ExecuteQuery`.
    pub fn with_begin(mut self) -> Self {
        self.options = self.options.with_begin();
        self
    }

    /// Also retry errors that require idempotency (see [`crate::CallBuilder::idempotent`]).
    pub fn idempotent(mut self, idempotent: bool) -> Self {
        self.idempotent = idempotent;
        self
    }

    /// Wall-clock limit for the whole `retry_tx` call (all attempts, backoff, and in-callback RPCs).
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl<'a, F, T> IntoFuture for RetryTxBuilder<'a, F, T>
where
    F: RetryTxAttempt<T>,
    F: 'a,
    T: Send + 'a,
{
    type Output = YdbResultWithCustomerErr<T>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.client.run_retry_tx(
            self.callback,
            self.options,
            self.timeout,
            self.idempotent,
        ))
    }
}
