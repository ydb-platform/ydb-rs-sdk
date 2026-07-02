use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

use crate::errors::YdbResultWithCustomerErr;
use crate::TransactionOptions;
use crate::TxMode;

use super::QueryClient;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Builder for [`QueryClient::retry_transaction`].
pub struct RetryTransactionBuilder<'a, F, T> {
    client: &'a QueryClient,
    callback: F,
    options: TransactionOptions,
    wall_timeout: Option<Duration>,
    _phantom: PhantomData<fn() -> T>,
}

impl<'a, F, T> RetryTransactionBuilder<'a, F, T> {
    pub(crate) fn new(client: &'a QueryClient, callback: F) -> Self {
        Self {
            client,
            callback,
            options: TransactionOptions::default(),
            wall_timeout: None,
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

    /// Total wall-clock budget for automatic retries on transient errors.
    pub fn retry_budget(mut self, budget: Duration) -> Self {
        self.options = self.options.with_retry_budget(budget);
        self
    }

    /// Disable automatic retries.
    pub fn no_retry(mut self) -> Self {
        self.options = self.options.with_no_retry();
        self
    }

    /// Max wall-clock time for the whole `retry_transaction` call (all attempts).
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.wall_timeout = Some(timeout);
        self
    }
}

impl<'a, F, T> IntoFuture for RetryTransactionBuilder<'a, F, T>
where
    F: AsyncFnMut(&mut super::Transaction) -> YdbResultWithCustomerErr<T>,
    F: 'a,
    T: 'a,
{
    type Output = YdbResultWithCustomerErr<T>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.client.run_retry_transaction(
            self.callback,
            self.options,
            self.wall_timeout,
        ))
    }
}
