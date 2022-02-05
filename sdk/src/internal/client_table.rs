use crate::errors::*;
use crate::internal::channel_pool::{ChannelPool, ChannelPoolImpl};
use crate::internal::client_common::DBCredentials;
use crate::internal::client_fabric::Middleware;
use crate::internal::discovery::{Discovery, Service};
use crate::internal::session::Session;
use crate::internal::session_pool::SessionPool;
use crate::internal::transaction::{AutoCommit, Mode, SerializableReadWriteTx, Transaction};
use num::pow;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use ydb_protobuf::generated::ydb::table::v1::table_service_client::TableServiceClient;

const DEFAULT_RETRY_TIMEOUT: Duration = Duration::from_secs(5);
const INITIAL_RETRY_BACKOFF_MILLISECONDS: u64 = 1;

pub(crate) type TableServiceClientType = TableServiceClient<Middleware>;
pub(crate) type TableServiceChannelPool = Arc<Box<dyn ChannelPool<TableServiceClientType>>>;

type TransactionArgType = Box<dyn Transaction>; // real type may be changed

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

pub struct TableClient {
    error_on_truncate: bool,
    session_pool: SessionPool,
    retrier: Arc<Box<dyn Retry>>,
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
        };
    }

    #[allow(dead_code)]
    pub(crate) fn with_max_active_session(mut self, size: usize) -> Self {
        self.session_pool = self.session_pool.with_max_active_sessions(size);
        return self;
    }

    #[allow(dead_code)]
    pub(crate) fn with_retry_timeout(mut self, timeout: Duration) -> Self {
        self.retrier = Arc::new(Box::new(TimeoutRetrier { timeout }));
        return self;
    }

    pub(crate) fn create_autocommit_transaction(&self, mode: Mode) -> impl Transaction {
        AutoCommit::new(self.session_pool.clone(), mode)
            .with_error_on_truncate(self.error_on_truncate)
    }

    pub(crate) fn create_interactive_transaction(&self) -> impl Transaction {
        SerializableReadWriteTx::new(self.session_pool.clone())
            .with_error_on_truncate(self.error_on_truncate)
    }

    pub(crate) async fn create_session(&mut self) -> YdbResult<Session> {
        return self.session_pool.session().await;
    }

    pub async fn retry_transaction<CallbackFuture, CallbackResult>(
        &self,
        transaction_options: TransactionOptions,
        retry_options: RetryOptions,
        callback: impl Fn(TransactionArgType) -> CallbackFuture,
    ) -> YdbResultWithCustomerErr<CallbackResult>
    where
        CallbackFuture: Future<Output = YdbResultWithCustomerErr<CallbackResult>>,
    {
        let retrier = retry_options
            .retrier
            .unwrap_or_else(|| self.retrier.clone());
        let mut attempts: usize = 0;
        let start = Instant::now();
        loop {
            let transaction: Box<dyn Transaction> = if transaction_options.autocommit {
                Box::new(self.create_autocommit_transaction(transaction_options.mode))
            } else {
                if transaction_options.mode != Mode::SerializableReadWrite {
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

            if !Self::check_retry_error(retry_options.idempotent_operation, &err) {
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
