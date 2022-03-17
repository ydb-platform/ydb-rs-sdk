use crate::channel_pool::{ChannelPool, ChannelPoolImpl};
use crate::client::Middleware;
use crate::client_common::DBCredentials;
use crate::discovery::{Discovery, Service};
use crate::errors::*;
use crate::session::Session;
use crate::session_pool::SessionPool;
use crate::transaction::{AutoCommit, Mode, SerializableReadWriteTx, Transaction};

use num::pow;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use ydb_grpc::ydb_proto::table::v1::table_service_client::TableServiceClient;

const DEFAULT_RETRY_TIMEOUT: Duration = Duration::from_secs(5);
const INITIAL_RETRY_BACKOFF_MILLISECONDS: u64 = 1;

pub(crate) type TableServiceClientType = TableServiceClient<Middleware>;
pub(crate) type TableServiceChannelPool = Arc<Box<dyn ChannelPool<TableServiceClientType>>>;

type TransactionArgType = Box<dyn Transaction>; // real type may be changed

/// Options for create transaction
#[derive(Clone)]
pub struct TransactionOptions {
    mode: Mode,
    autocommit: bool, // Commit transaction after every query. From DB side it visible as many small transactions
}

impl TransactionOptions {
    /// Create default transaction
    ///
    /// With Mode::SerializableReadWrite and no autocommit.
    pub fn new() -> Self {
        return Self {
            mode: Mode::SerializableReadWrite,
            autocommit: false,
        };
    }

    /// Set transaction [Mode]
    pub fn with_mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        return self;
    }

    /// Set autocommit mode
    pub fn with_autocommit(mut self, autocommit: bool) -> Self {
        self.autocommit = autocommit;
        return self;
    }
}

/// Retry options
pub struct RetryOptions {
    /// Operations under the option is idempotent. Repeat completed operation - safe.
    idempotent_operation: bool,

    /// Algorithm for retry decision
    retrier: Option<Arc<Box<dyn Retry>>>,
}

impl RetryOptions {
    /// Default option for no retries
    pub fn new() -> Self {
        return Self {
            idempotent_operation: false,
            retrier: None,
        };
    }

    /// Operations under the options is safe for complete few times instead of one.
    #[allow(dead_code)]
    pub(crate) fn with_idempotent(mut self, idempotent: bool) -> Self {
        self.idempotent_operation = idempotent;
        return self;
    }

    /// Set retry timeout
    #[allow(dead_code)]
    pub(crate) fn with_timeout(mut self, timeout: Duration) -> Self {
        self.retrier = Some(Arc::new(Box::new(TimeoutRetrier { timeout })));
        return self;
    }
}

/// Client for YDB table service (SQL queries)
///
/// Table service used for work with data abd DB struct
/// with SQL queries.
///
/// TableClient contains options for make queries.
/// See [TableClient::retry_transaction] for examples.
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

    /// Clone the table client and set new retry timeouts
    #[allow(dead_code)]
    pub fn clone_with_retry_timeout(&self, timeout: Duration) -> Self {
        return Self {
            retrier: Arc::new(Box::new(TimeoutRetrier { timeout })),
            ..self.clone()
        };
    }

    /// Clone the table client and deny retries
    #[allow(dead_code)]
    pub fn clone_with_no_retry(&self) -> Self {
        return Self {
            retrier: Arc::new(Box::new(NoRetrier {})),
            ..self.clone()
        };
    }

    /// Clone the table client and set feature operations as idempotent (can retry in more cases)
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

    /// Execute scheme query with retry policy
    pub async fn retry_execute_scheme_query<T: Into<String>>(&self, query: T) -> YdbResult<()> {
        let query = Arc::new(query.into());
        self.retry(|| async {
            let mut session = self.session_pool.session().await?;
            return session.execute_schema_query(query.to_string()).await;
        })
        .await
    }

    /// Retry callback in transaction
    ///
    /// retries callback as retry policy.
    /// every call of callback will within new transaction
    /// retry will call callback next time if:
    /// 1. allow by retry policy
    /// 2. callback return retriable error
    ///
    /// Example with move lambda args:
    /// ```no_run
    /// # use ydb::YdbResult;
    /// #
    /// # #[tokio::main]
    /// # async fn main()->YdbResult<()>{
    /// #   use ydb::{Query, Value};
    /// #   let table_client = ydb::ClientBuilder::from_str("")?.client()?.table_client();
    ///     let res: Option<i32> = table_client.retry_transaction(|mut t| async move {
    ///         let value: Value = t.query(Query::new("SELECT 1 + 1 as sum")).await?
    ///             .into_only_row()?
    ///             .remove_field_by_name("sum")?;
    ///         let res: Option<i32> = value.try_into()?;
    ///         return Ok(res);
    ///     }).await?;
    ///     assert_eq!(Some(2), res);
    /// #     return Ok(());
    /// # }
    /// ```
    ///
    /// Example without move lambda args - it allow to borrow external items:
    /// ```no_run
    /// # use ydb::YdbResult;
    /// #
    /// # #[tokio::main]
    /// # async fn main()->YdbResult<()>{
    /// #   use std::sync::atomic::{AtomicUsize, Ordering};
    /// #   use ydb::{Query, Value};
    /// #   let table_client = ydb::ClientBuilder::from_str("")?.client()?.table_client();
    ///     let mut attempts: AtomicUsize = AtomicUsize::new(0);
    ///     let res: Option<i32> = table_client.retry_transaction(|mut t| async {
    ///         let mut t = t; // explicit move lambda argument inside async code block for borrow checker
    ///         attempts.fetch_add(1, Ordering::Relaxed); // can borrow outer values istead of move
    ///         let value: Value = t.query(Query::new("SELECT 1 + 1 as sum")).await?
    ///             .into_only_row()?
    ///             .remove_field_by_name("sum")?;
    ///         let res: Option<i32> = value.try_into()?;
    ///         return Ok(res);
    ///     }).await?;
    ///     assert_eq!(Some(2), res);
    ///     assert_eq!(1, attempts.load(Ordering::Relaxed));
    /// #   return Ok(());
    /// # }
    /// ```
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

trait Retry: Send + Sync {
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
