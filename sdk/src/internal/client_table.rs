use std::any::Any;
use std::sync::Arc;
use std::time::{Duration, Instant};
use num::pow;
use tokio::time::sleep;
use crate::errors::*;
use crate::internal::client_common::DBCredentials;
use crate::internal::client_fabric::Middleware;
use crate::internal::discovery::{Discovery, Service};

use crate::internal::load_balancer::{SharedLoadBalancer};
use crate::internal::session::Session;
use crate::internal::session_pool::{SessionClient, SessionPool};
use crate::internal::transaction::{AutoCommit, Mode, SerializableReadWriteTx, Transaction};
use ydb_protobuf::generated::ydb::table::v1::table_service_client::TableServiceClient;
use crate::internal::channel_pool::ChannelPool;

const DEFAULT_RETRY_TIMEOUT: Duration = Duration::from_secs(5);
const INITIAL_RETRY_BACKOFF_MILLISECONDS: u64 = 1;

pub struct TransactionRetryOptions {
    mode: Mode,
    autocommit: bool, // Commit transaction after every query. From DB side it visible as many small transactions
    idempotent_operation: bool,
    retrier: Option<Arc<Box<dyn Retry>>>,
}

impl TransactionRetryOptions {
    pub fn new()->Self{
        return Self{
            mode: Mode::SerializableReadWrite,
            autocommit: false,
            idempotent_operation: false,
            retrier: None,
        }
    }

    pub fn with_mode(mut self, mode: Mode)->Self {
        self.mode = mode;
        return self
    }

    pub fn with_autocommit(mut self, autocommit: bool)->Self {
        self.autocommit = autocommit;
        return self;
    }

    pub fn with_idempotent(mut self, idempotent: bool)->Self {
        self.idempotent_operation = idempotent;
        return self;
    }

    pub fn with_timeout(mut self, timeout: Option<Duration>)->Self {
        self.retrier = match timeout {
            Some(timeout) => Some(Arc::new(Box::new(TimeoutRetrier{timeout}))),
            None=>None,
        };
        return self;
    }
}

pub(crate) struct TableClient {
    error_on_truncate: bool,
    session_pool: SessionPool,
    channel_pool: ChannelPool<TableServiceClient<Middleware>>,
    retrier: Arc<Box<dyn Retry>>,
}

impl TableClient {
    pub(crate) fn new(credencials: DBCredentials, discovery: Arc<Box<dyn Discovery>>,) -> Self {
        let channel_pool =ChannelPool::new::<TableServiceClient<Middleware>>(discovery, credencials.clone(), Service::Table, TableServiceClient::new);

        return Self {
            error_on_truncate: false,
            session_pool: SessionPool::new(Box::new(channel_pool.clone())),
            channel_pool,
            retrier: Arc::new(Box::new(TimeoutRetrier::default())),
        };
    }

    #[allow(dead_code)]
    pub(crate) fn with_max_active_session(mut self, size: usize)->Self {
        self.session_pool = self.session_pool.with_max_active_sessions(size);
        return self;
    }

    pub fn with_retry_timeout(mut self, timeout: Duration)->Self {
        self.retrier = Arc::new(Box::new(TimeoutRetrier{timeout}));
        return self;
    }

    pub fn create_autocommit_transaction(&self, mode: Mode) -> impl Transaction {
        AutoCommit::new(self.channel_pool.clone(), self.session_pool.clone(), mode).with_error_on_truncate(self.error_on_truncate)
    }

    pub fn create_interactive_transaction(&self) -> impl Transaction {
        SerializableReadWriteTx::new(self.channel_pool.clone(), self.session_pool.clone()).with_error_on_truncate(self.error_on_truncate)
    }

    pub(crate) async fn create_session(&mut self) -> Result<Session> {
        return self.session_pool.session().await;
    }

    pub async fn retry_transaction<Op,OpRes,OpErr>(&self, opts: TransactionRetryOptions, op: Op)->ResultWithCustomerErr<OpRes, OpErr>
    where
        OpErr: std::error::Error,
        Op: Fn(Box<dyn Transaction>)->ResultWithCustomerErr<OpRes, OpErr>
    {
        let retrier = opts.retrier.unwrap_or_else(|| self.retrier.clone());
        let mut attempts : usize = 0;
        let start = Instant::now();
        loop {
            let transaction: Box<dyn Transaction> = if opts.autocommit {
                Box::new(self.create_autocommit_transaction(opts.mode))
            } else {
                if opts.mode != Mode::SerializableReadWrite {
                    return Err(YdbOrCustomerError::<OpErr>::YDB(Error::Custom("only serializable rw transactions allow to interactive mode".into())))
                }
                Box::new(self.create_interactive_transaction())
            };

            let res = op(transaction);

            let err = if let Err(err) = res {
                err
            } else {
                return res;
            };

            let now = Instant::now();
            attempts+= 1;
            let loop_decision = retrier.wait_duration(RetryParams{ attempt: attempts, time_from_start: now.duration_since(start) });
            if loop_decision.allow_retry {
                sleep(loop_decision.wait_timeout).await;
            } else {
                return Err(err)
            };

            Self::check_retry_error(opts.idempotent_operation, err)?;
        };
    }

    #[allow(dead_code)]
    pub fn with_error_on_truncate(mut self, error_on_truncate: bool) -> Self {
        self.error_on_truncate = error_on_truncate;
        return self;
    }

    fn check_retry_error<Err: std::error::Error>(is_idempotent_operation: bool, err: YdbOrCustomerError<Err>)->std::result::Result<(),YdbOrCustomerError<Err>>{
        let ydb_err = match &err {
            YdbOrCustomerError::Customer(_) => return Err(err),
            YdbOrCustomerError::YDB(err)=>err,
        };

        return match (ydb_err.need_retry(), is_idempotent_operation) {
            (NeedRetry::True, _) =>Ok(()),
            (NeedRetry::IdempotentOnly, true) => Ok(()),
            _ => Err(err)
        }
    }
}

struct RetryParams {
    pub attempt: usize,
    pub time_from_start: Duration,
}

// May be extend in feature
#[derive(Default)]
struct RetryDecision {
    pub allow_retry: bool,
    pub wait_timeout: Duration,
}

trait Retry {
    fn wait_duration(&self, params: RetryParams)->RetryDecision;
}

struct TimeoutRetrier {
    timeout: Duration
}

impl Default for TimeoutRetrier {
    fn default() -> Self {
        return Self {
            timeout: DEFAULT_RETRY_TIMEOUT,
        }
    }
}

impl Retry for TimeoutRetrier {
    fn wait_duration(&self, params: RetryParams) -> RetryDecision {
        let mut res = RetryDecision::default();
        if params.time_from_start < self.timeout {
            if params.attempt > 0 {
                res.wait_timeout = Duration::from_millis(pow(INITIAL_RETRY_BACKOFF_MILLISECONDS, params.attempt));
            }
            res.allow_retry = (params.time_from_start + res.wait_timeout) < self.timeout;
        };

        return res;
    }
}