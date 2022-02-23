use crate::errors::*;
use crate::internal::channel_pool::{ChannelPool, ChannelPoolImpl};
use crate::internal::client::Middleware;
use crate::internal::client_common::DBCredentials;
use crate::internal::discovery::{Discovery, Service};
use crate::internal::session::Session;
use crate::internal::session_pool::SessionPool;
use crate::internal::transaction::{AutoCommit, Mode, SerializableReadWriteTx, Transaction};

use num::pow;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use ydb_protobuf::ydb_proto::table::v1::table_service_client::TableServiceClient;

const DEFAULT_RETRY_TIMEOUT: Duration = Duration::from_secs(5);
const INITIAL_RETRY_BACKOFF_MILLISECONDS: u64 = 1;

pub(crate) type TableServiceClientType = TableServiceClient<Middleware>;
pub(crate) type TableServiceChannelPool = Arc<Box<dyn ChannelPool<TableServiceClientType>>>;

type TransactionArgType = Box<dyn Transaction>; // real type may be changed

#[derive(Clone)]
pub struct TransactionOptions {
    mode: Mode,
    autocommit: bool, // Commit transaction after every query. From DB side it visible as many small transactions
}

impl TransactionOptions {
    pub fn new() -> Self {
        return Self {
            mode: Mode::SerializableReadWrite,
            autocommit: false,
        };
    }

    #[allow(dead_code)]
    pub(crate) fn with_mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        return self;
    }

    #[allow(dead_code)]
    pub(crate) fn with_autocommit(mut self, autocommit: bool) -> Self {
        self.autocommit = autocommit;
        return self;
    }
}

pub struct RetryOptions {
    idempotent_operation: bool,
    retrier: Option<Arc<Box<dyn Retry>>>,
}

impl RetryOptions {
    pub fn new() -> Self {
        return Self {
            idempotent_operation: false,
            retrier: None,
        };
    }

    #[allow(dead_code)]
    pub(crate) fn with_idempotent(mut self, idempotent: bool) -> Self {
        self.idempotent_operation = idempotent;
        return self;
    }

    #[allow(dead_code)]
    pub(crate) fn with_timeout(mut self, timeout: Duration) -> Self {
        self.retrier = Some(Arc::new(Box::new(TimeoutRetrier { timeout })));
        return self;
    }
}

#[derive(Clone)]
pub struct TableClient {
    error_on_truncate: bool,
    session_pool: SessionPool,
    retrier: Arc<Box<dyn Retry>>,
    transaction_options: TransactionOptions,
    idempotent_operation: bool,
}

impl TableClient {
    pub(crate) fn new(credencials: DBCredentials, discovery: Arc<Box<dyn Discovery>>) -> Self {
        let channel_pool = ChannelPoolImpl::new::<TableServiceClientType>(
            discovery,
            credencials.clone(),
            Service::Table,
            TableServiceClient::new,
        );
        let channel_pool: TableServiceChannelPool = Arc::new(Box::new(channel_pool));

        return Self {
            error_on_truncate: false,
            session_pool: SessionPool::new(Box::new(channel_pool)),
            retrier: Arc::new(Box::new(TimeoutRetrier::default())),
            transaction_options: TransactionOptions::new(),
            idempotent_operation: false,
        };
    }

    #[allow(dead_code)]
    pub(crate) fn with_max_active_sessions(mut self, size: usize) -> Self {
        self.session_pool = self.session_pool.with_max_active_sessions(size);
        return self;
    }

    #[allow(dead_code)]
    pub fn clone_with_retry_timeout(&self, timeout: Duration) -> Self {
        return Self {
            retrier: Arc::new(Box::new(TimeoutRetrier { timeout })),
            ..self.clone()
        };
    }

    #[allow(dead_code)]
    pub fn clone_with_no_retry(&self) -> Self {
        return Self {
            retrier: Arc::new(Box::new(NoRetrier {})),
            ..self.clone()
        };
    }

    #[allow(dead_code)]
    pub fn clone_with_idempotent_operations(&self, idempotent: bool) -> Self {
        return Self {
            idempotent_operation: idempotent,
            ..self.clone()
        };
    }

    pub(crate) fn create_autocommit_transaction(&self, mode: Mode) -> impl Transaction {
        AutoCommit::new(self.session_pool.clone(), mode)
            .with_error_on_truncate(self.error_on_truncate)
    }

    pub(crate) fn create_interactive_transaction(&self) -> impl Transaction {
        SerializableReadWriteTx::new(self.session_pool.clone())
            .with_error_on_truncate(self.error_on_truncate)
    }

    #[allow(dead_code)]
    pub(crate) async fn create_session(&mut self) -> YdbResult<Session> {
        return self.session_pool.session().await;
    }

    async fn retry<CallbackFuture, CallbackResult>(
        &self,
        callback: impl Fn() -> CallbackFuture,
    ) -> YdbResult<CallbackResult>
    where
        CallbackFuture: Future<Output = YdbResult<CallbackResult>>,
    {
        let mut attempt: usize = 0;
        let start = Instant::now();
        loop {
            attempt += 1;
            let last_err = match callback().await {
                Ok(res) => return Ok(res),
                Err(err) => match (err.need_retry(), self.idempotent_operation) {
                    (NeedRetry::True, _) => err,
                    (NeedRetry::IdempotentOnly, true) => err,
                    _ => return Err(err),
                },
            };

            let now = std::time::Instant::now();
            let retry_decision = self.retrier.wait_duration(RetryParams {
                attempt,
                time_from_start: now.duration_since(start),
            });
            if !retry_decision.allow_retry {
                return Err(last_err);
            }
            tokio::time::sleep(retry_decision.wait_timeout).await;
        }
    }

    pub async fn retry_execute_scheme_query<T: Into<String>>(&self, query: T) -> YdbResult<()> {
        let query = Arc::new(query.into());
        self.retry(|| async {
            let mut session = self.session_pool.session().await?;
            return session.execute_schema_query(query.to_string()).await;
        })
        .await
    }

    pub async fn retry_transaction<CallbackFuture, CallbackResult>(
        &self,
        callback: impl Fn(TransactionArgType) -> CallbackFuture,
    ) -> YdbResultWithCustomerErr<CallbackResult>
    where
        CallbackFuture: Future<Output = YdbResultWithCustomerErr<CallbackResult>>,
    {
        let mut attempts: usize = 0;
        let start = Instant::now();
        loop {
            let transaction: Box<dyn Transaction> = if self.transaction_options.autocommit {
                Box::new(self.create_autocommit_transaction(self.transaction_options.mode))
            } else {
                if self.transaction_options.mode != Mode::SerializableReadWrite {
                    return Err(YdbOrCustomerError::YDB(YdbError::Custom(
                        "only serializable rw transactions allow to interactive mode".into(),
                    )));
                }
                Box::new(self.create_interactive_transaction())
            };

            let res = callback(transaction).await;

            let err = if let Err(err) = res {
                err
            } else {
                return res;
            };

            if !Self::check_retry_error(self.idempotent_operation, &err) {
                return Err(err);
            }

            let now = Instant::now();
            attempts += 1;
            let loop_decision = self.retrier.wait_duration(RetryParams {
                attempt: attempts,
                time_from_start: now.duration_since(start),
            });
            if loop_decision.allow_retry {
                sleep(loop_decision.wait_timeout).await;
            } else {
                return Err(err);
            };
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn retry_with_session<CallbackFuture, CallbackResult>(
        &self,
        opts: RetryOptions,
        callback: impl Fn(Session) -> CallbackFuture,
    ) -> YdbResultWithCustomerErr<CallbackResult>
    where
        CallbackFuture: Future<Output = YdbResultWithCustomerErr<CallbackResult>>,
    {
        let retrier = opts.retrier.unwrap_or_else(|| self.retrier.clone());
        let mut attempts: usize = 0;
        let start = Instant::now();
        loop {
            let session = self.session_pool.session().await?;
            let res = callback(session).await;

            let err = if let Err(err) = res {
                err
            } else {
                return res;
            };

            if !Self::check_retry_error(opts.idempotent_operation, &err) {
                return Err(err);
            }

            let now = Instant::now();
            attempts += 1;
            let loop_decision = retrier.wait_duration(RetryParams {
                attempt: attempts,
                time_from_start: now.duration_since(start),
            });
            if loop_decision.allow_retry {
                sleep(loop_decision.wait_timeout).await;
            } else {
                return Err(err);
            };
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_error_on_truncate(mut self, error_on_truncate: bool) -> Self {
        self.error_on_truncate = error_on_truncate;
        return self;
    }

    fn check_retry_error(is_idempotent_operation: bool, err: &YdbOrCustomerError) -> bool {
        let ydb_err = match &err {
            YdbOrCustomerError::Customer(_) => return false,
            YdbOrCustomerError::NoneInOption => return false,
            YdbOrCustomerError::YDB(err) => err,
        };

        return match ydb_err.need_retry() {
            NeedRetry::True => true,
            NeedRetry::IdempotentOnly => is_idempotent_operation,
            NeedRetry::False => false,
        };
    }
}

struct RetryParams {
    pub(crate) attempt: usize,
    pub(crate) time_from_start: Duration,
}

// May be extend in feature
#[derive(Default)]
struct RetryDecision {
    pub(crate) allow_retry: bool,
    pub(crate) wait_timeout: Duration,
}

trait Retry {
    fn wait_duration(&self, params: RetryParams) -> RetryDecision;
}

struct TimeoutRetrier {
    timeout: Duration,
}

impl Default for TimeoutRetrier {
    fn default() -> Self {
        return Self {
            timeout: DEFAULT_RETRY_TIMEOUT,
        };
    }
}

impl Retry for TimeoutRetrier {
    fn wait_duration(&self, params: RetryParams) -> RetryDecision {
        let mut res = RetryDecision::default();
        if params.time_from_start < self.timeout {
            if params.attempt > 0 {
                res.wait_timeout =
                    Duration::from_millis(pow(INITIAL_RETRY_BACKOFF_MILLISECONDS, params.attempt));
            }
            res.allow_retry = (params.time_from_start + res.wait_timeout) < self.timeout;
        };

        return res;
    }
}

struct NoRetrier {}

impl Retry for NoRetrier {
    fn wait_duration(&self, _: RetryParams) -> RetryDecision {
        return RetryDecision {
            allow_retry: false,
            wait_timeout: Duration::default(),
        };
    }
}
