use crate::client::TimeoutSettings;

use crate::errors::*;
use crate::session::Session;
use crate::session_pool::{SessionPool, TableSessionPool};
use crate::transaction::{AutoCommit, Mode, SerializableReadWriteTx, Transaction};
use crate::types::Value;

use crate::grpc_connection_manager::GrpcConnectionManager;

use crate::grpc_wrapper::grpc_limits::WithGrpcMaxMessageSize;
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;
use crate::retry::{NoRetrier, Retry, RetryParams, TimeoutRetrier};
use crate::table_service_types::{CopyTableItem, TableDescription};
use crate::table_requests::{
    AlterTableRequest, CreateTableRequest, DropTableRequest, PreparedDataQuery, ReadRowsRequest,
    ReadTableOptions, TableOptionsDescription,
};
use crate::grpc_wrapper::raw_table_service::bulk_upsert::RawBulkUpsertRequest;
use crate::grpc_wrapper::raw_table_service::client::RawTableClient;
use crate::grpc_wrapper::raw_table_service::read_rows::RawReadRowsRequest;
use crate::session::CreateTableClient;
use crate::types_converters::try_vec_to_list_of_structs;
use crate::{Query, QueryResult, StreamReadTableResult, StreamResult};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{instrument, trace};
use ydb_grpc::ydb_proto::table::v1::table_service_client::TableServiceClient;

pub(crate) type TableServiceClientType = TableServiceClient<InterceptedChannel>;

impl WithGrpcMaxMessageSize for TableServiceClientType {
    fn with_grpc_max_message_size(self, bytes: usize) -> Self {
        self.max_decoding_message_size(bytes)
            .max_encoding_message_size(bytes)
    }
}

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
        Self {
            mode: Mode::SerializableReadWrite,
            autocommit: false,
        }
    }

    /// Set transaction [Mode]
    pub fn with_mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    /// Set autocommit mode
    pub fn with_autocommit(mut self, autocommit: bool) -> Self {
        self.autocommit = autocommit;
        self
    }
}

impl Default for TransactionOptions {
    fn default() -> Self {
        Self::new()
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
        Self {
            idempotent_operation: false,
            retrier: None,
        }
    }

    /// Operations under the options is safe for complete few times instead of one.
    #[allow(dead_code)]
    pub(crate) fn with_idempotent(mut self, idempotent: bool) -> Self {
        self.idempotent_operation = idempotent;
        self
    }

    /// Set retry timeout
    #[allow(dead_code)]
    pub(crate) fn with_timeout(mut self, timeout: Duration) -> Self {
        self.retrier = Some(Arc::new(Box::new(TimeoutRetrier { timeout })));
        self
    }
}

impl Default for RetryOptions {
    fn default() -> Self {
        Self::new()
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
    ignore_truncated: bool,
    session_pool: TableSessionPool,
    retrier: Arc<Box<dyn Retry>>,
    transaction_options: TransactionOptions,
    idempotent_operation: bool,
    timeouts: TimeoutSettings,
}

impl TableClient {
    pub(crate) fn new(
        connection_manager: GrpcConnectionManager,
        timeouts: TimeoutSettings,
        session_pool: SessionPool,
    ) -> Self {
        Self {
            ignore_truncated: false,
            session_pool: TableSessionPool::from_shared(session_pool, connection_manager, timeouts),
            retrier: Arc::new(Box::<TimeoutRetrier>::default()),
            transaction_options: TransactionOptions::new(),
            idempotent_operation: false,
            timeouts,
        }
    }

    // Clone the table client and set new timeouts settings
    pub fn clone_with_timeouts(&self, timeouts: TimeoutSettings) -> Self {
        Self {
            timeouts,
            ..self.clone()
        }
    }

    /// Clone the table client and set new retry timeouts
    #[allow(dead_code)]
    pub fn clone_with_retry_timeout(&self, timeout: Duration) -> Self {
        Self {
            retrier: Arc::new(Box::new(TimeoutRetrier { timeout })),
            ..self.clone()
        }
    }

    /// Clone the table client and deny retries
    #[allow(dead_code)]
    pub fn clone_with_no_retry(&self) -> Self {
        Self {
            retrier: Arc::new(Box::new(NoRetrier {})),
            ..self.clone()
        }
    }

    /// Clone the table client and set feature operations as idempotent (can retry in more cases)
    #[allow(dead_code)]
    pub fn clone_with_idempotent_operations(&self, idempotent: bool) -> Self {
        Self {
            idempotent_operation: idempotent,
            ..self.clone()
        }
    }

    pub fn clone_with_transaction_options(&self, opts: TransactionOptions) -> Self {
        Self {
            transaction_options: opts,
            ..self.clone()
        }
    }

    pub(crate) fn create_autocommit_transaction(&self, mode: Mode) -> impl Transaction {
        AutoCommit::new(self.session_pool.clone(), mode, self.timeouts)
            .with_ignore_truncated(self.ignore_truncated)
    }

    pub(crate) fn create_interactive_transaction(&self) -> impl Transaction {
        SerializableReadWriteTx::new(self.session_pool.clone(), self.timeouts)
            .with_ignore_truncated(self.ignore_truncated)
    }

    #[allow(dead_code)]
    pub(crate) async fn create_session(&self) -> YdbResult<Session> {
        Ok(self
            .session_pool
            .session()
            .await?
            .with_timeouts(self.timeouts))
    }

    async fn sessionless_table_client(&self) -> YdbResult<RawTableClient> {
        CreateTableClient::create_table_client(
            self.session_pool.connection_manager(),
            self.timeouts,
        )
        .await
    }

    async fn bulk_upsert_once(&self, table_path: String, rows: Value) -> YdbResult<()> {
        let raw_rows: crate::grpc_wrapper::raw_table_service::value::RawTypedValue =
            rows.try_into().map_err(YdbError::from)?;
        let mut client = self.sessionless_table_client().await?;
        client
            .bulk_upsert(RawBulkUpsertRequest {
                table: table_path,
                rows: raw_rows.into(),
                operation_params: self.timeouts.operation_params(),
            })
            .await
            .map_err(YdbError::from)?;
        Ok(())
    }

    async fn read_rows_once(
        &self,
        request: RawReadRowsRequest,
        ignore_truncated: bool,
    ) -> YdbResult<crate::ResultSet> {
        let mut client = self.sessionless_table_client().await?;
        let raw_response = client.read_rows(request).await.map_err(YdbError::from)?;
        let result_set: crate::ResultSet = raw_response.result_set.try_into()?;
        if !ignore_truncated && result_set.is_truncated() {
            return Err(YdbError::TruncatedResult {
                result_set_index: 0,
            });
        }
        Ok(result_set)
    }

    async fn retry_idempotent<CallbackFuture, CallbackResult>(
        &self,
        callback: impl Fn() -> CallbackFuture,
    ) -> YdbResult<CallbackResult>
    where
        CallbackFuture: Future<Output = YdbResult<CallbackResult>>,
    {
        self.clone_with_idempotent_operations(true)
            .retry(callback)
            .await
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

    /// Execute scan query. The method will auto-retry errors while start query execution,
    /// but no retries after server start streaming result.
    pub async fn retry_execute_scan_query(&self, query: Query) -> YdbResult<StreamResult> {
        self.retry(|| async {
            let mut session = self.create_session().await?;
            session.execute_scan_query(query.clone()).await
        })
        .await
    }

    /// Execute scheme query with retry policy
    pub async fn retry_execute_scheme_query<T: Into<String>>(&self, query: T) -> YdbResult<()> {
        let query = query.into();
        self.retry(|| async {
            let mut session = self.create_session().await?;
            session.execute_schema_query(query.clone()).await
        })
        .await
    }

    /// Read rows by primary key without opening a session (go-sdk: `table.Client.ReadRows`).
    ///
    /// `request.keys` must be a list of [`Value::Struct`] primary-key values.
    /// Returns an empty result set when `keys` is empty.
    pub async fn retry_read_rows_request(
        &self,
        request: ReadRowsRequest,
    ) -> YdbResult<crate::ResultSet> {
        if request.keys.is_empty() {
            return Ok(crate::ResultSet::default());
        }

        let raw = request.clone().into_raw(String::new())?;
        let ignore_truncated = self.ignore_truncated;
        self.retry_idempotent(|| async {
            self.read_rows_once(raw.clone(), ignore_truncated).await
        })
        .await
    }

    /// From table with given path `table_path` request rows by primary keys `keys`, which must be
    /// vector of [`Value::Struct`]. If any key does not meet requirement, error will be returned.
    ///
    /// If `columns` is `None`, all columns of requested rows will be returned. Otherwise, only
    /// `columns` will be returned.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ydb::{TableClient, YdbResult, ydb_struct};
    /// # async fn example(table_client: TableClient) -> YdbResult<()> {
    /// let keys = vec![
    ///     ydb_struct!("id" => 1_i64),
    ///     ydb_struct!("id" => 2_i64),
    /// ];
    ///
    /// let columns = Some(vec!["date".to_string(), "count".to_string()]);
    ///
    /// let result_set = table_client
    ///     .retry_read_rows("/local/my_table".to_string(), keys, columns)
    ///     .await?;
    ///
    /// for row in result_set.rows() {
    ///     // Your code here.
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn retry_read_rows(
        &self,
        table_path: impl Into<String>,
        keys: Vec<Value>,
        columns: Option<Vec<String>>,
    ) -> YdbResult<crate::ResultSet> {
        let mut request = ReadRowsRequest::new(table_path).with_keys(keys);
        if let Some(columns) = columns {
            request.columns = columns;
        }
        self.retry_read_rows_request(request).await
    }

    /// Execute explain data query with retry policy
    ///
    /// # Type Parameters
    /// - `T`: Any type that can be converted to String (e.g., &str, String)
    ///
    /// # Arguments
    /// - `query`: The YQL query to explain
    /// - `collect_full_diagnostics`: Boolean flag to enable full diagnostics collection
    ///
    /// # Returns
    /// - `YdbResult<ExplainResult>`: The explain result containing query AST, plan, and diagnostics
    ///
    /// # Example
    /// ```no_run
    /// # use ydb::YdbResult;
    /// # #[tokio::main]
    /// # async fn main() -> YdbResult<()> {
    /// #   let client = ydb::ClientBuilder::new_from_connection_string("")?.client()?;
    /// #   client.wait().await?;
    /// #   let table_client = client.table_client();
    ///     let result = table_client.retry_explain_data_query("SELECT * FROM my_table", false).await?;
    ///     println!("Query AST: {}", result.query_ast);
    ///     println!("Query Plan: {}", result.query_plan);
    /// #   Ok(())
    /// # }
    /// ```
    pub async fn retry_explain_data_query<T: Into<String>>(
        &self,
        query: T,
        collect_full_diagnostics: bool,
    ) -> YdbResult<crate::result::ExplainResult> {
        let query = query.into();
        self.retry(|| async {
            let mut session = self.create_session().await?;
            session
                .explain_data_query(query.clone(), collect_full_diagnostics)
                .await
        })
        .await
    }

    /// Bulk upsert rows without opening a session (go-sdk: `table.Client.BulkUpsert`).
    pub async fn retry_bulk_upsert(
        &self,
        table_path: impl Into<String>,
        rows: Vec<Value>,
    ) -> YdbResult<()> {
        let Some(value) = try_vec_to_list_of_structs(rows)? else {
            return Ok(());
        };
        let table_path = table_path.into();
        self.retry_idempotent(|| async {
            self.bulk_upsert_once(table_path.clone(), value.clone())
                .await
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
    /// #   let table_client = ydb::ClientBuilder::new_from_connection_string("")?.client()?.table_client();
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
    /// #   let table_client = ydb::ClientBuilder::new_from_connection_string("")?.client()?.table_client();
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
    #[instrument(level = "trace", skip_all, err)]
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
            attempts += 1;
            trace!("attempt: {}", attempts);
            let transaction: Box<dyn Transaction> = if self.transaction_options.autocommit {
                Box::new(self.create_autocommit_transaction(self.transaction_options.mode))
            } else {
                if self.transaction_options.mode != Mode::SerializableReadWrite {
                    return Err(YdbOrCustomerError::YDB(YdbError::Custom(
                        "interactive retry_transaction requires Mode::SerializableReadWrite; \
                         other modes (e.g. SnapshotReadOnly) are supported with autocommit: true"
                            .into(),
                    )));
                }
                Box::new(self.create_interactive_transaction())
            };

            let res = callback(transaction).await;

            let err = if let Err(err) = res {
                err
            } else {
                match &res {
                    Ok(_) => trace!("return successfully after '{}' attempts", attempts),
                    Err(err) => trace!(
                        "return with customer error after '{}' attempts: {:?}",
                        attempts,
                        err
                    ),
                };
                return res;
            };

            if !Self::check_retry_error(self.idempotent_operation, &err) {
                return Err(err);
            }

            let now = Instant::now();
            let loop_decision = self.retrier.wait_duration(RetryParams {
                attempt: attempts,
                time_from_start: now.duration_since(start),
            });
            if loop_decision.allow_retry {
                sleep(loop_decision.wait_timeout).await;
            } else {
                trace!(
                    "return with ydb error after '{}' attempts by retry decision: {}",
                    attempts,
                    err
                );
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
            let session = self.create_session().await?;
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

    /// Do not return [`YdbError::TruncatedResult`] when a result set is truncated (go-sdk: `WithIgnoreTruncated`).
    ///
    /// By default truncated result sets produce an error.
    pub fn with_ignore_truncated(mut self, ignore_truncated: bool) -> Self {
        self.ignore_truncated = ignore_truncated;
        self
    }

    #[instrument(level = "trace", ret)]
    fn check_retry_error(is_idempotent_operation: bool, err: &YdbOrCustomerError) -> bool {
        let ydb_err = match &err {
            YdbOrCustomerError::Customer(_) => return false,
            YdbOrCustomerError::YDB(err) => err,
        };

        match ydb_err.need_retry() {
            NeedRetry::True => true,
            NeedRetry::IdempotentOnly => is_idempotent_operation,
            NeedRetry::False => false,
        }
    }

    pub async fn copy_table(&self, source_path: String, destination_path: String) -> YdbResult<()> {
        self.retry_with_session(RetryOptions::new(), |session| async {
            let mut session = session; // force borrow for lifetime of t inside closure
            session
                .copy_table(source_path.clone(), destination_path.clone())
                .await?;

            Ok(())
        })
        .await
        .map_err(YdbOrCustomerError::to_ydb_error)
    }

    pub async fn copy_tables(&self, tables: Vec<CopyTableItem>) -> YdbResult<()> {
        self.retry_with_session(RetryOptions::new(), |session| async {
            let mut session = session; // force borrow for lifetime of t inside closure
            session.copy_tables(tables.to_vec()).await?;

            Ok(())
        })
        .await
        .map_err(YdbOrCustomerError::to_ydb_error)
    }

    pub async fn describe_table(&self, path: String) -> YdbResult<TableDescription> {
        self.retry_with_session(RetryOptions::new(), |session| async {
            let mut session = session;
            let result = session.describe_table(path.clone()).await?;
            Ok(result)
        })
        .await
        .map_err(YdbOrCustomerError::to_ydb_error)
    }

    /// Create a table via `CreateTable` RPC (go-sdk: `Session.CreateTable`).
    pub async fn retry_create_table(&self, request: CreateTableRequest) -> YdbResult<()> {
        self.retry(|| async {
            let mut session = self.create_session().await?;
            session.create_table(request.clone()).await
        })
        .await
    }

    /// Drop a table via `DropTable` RPC.
    pub async fn retry_drop_table(&self, request: DropTableRequest) -> YdbResult<()> {
        self.retry(|| async {
            let mut session = self.create_session().await?;
            session.drop_table(request.clone()).await
        })
        .await
    }

    /// Alter a table via `AlterTable` RPC (columns, attributes, etc.).
    pub async fn retry_alter_table(&self, request: AlterTableRequest) -> YdbResult<()> {
        self.retry(|| async {
            let mut session = self.create_session().await?;
            session.alter_table(request.clone()).await
        })
        .await
    }

    /// Prepare a data query for repeated execution (go-sdk: `Session.Prepare`).
    pub async fn retry_prepare_data_query(
        &self,
        yql_text: impl Into<String>,
    ) -> YdbResult<PreparedDataQuery> {
        let yql_text = yql_text.into();
        self.retry(|| async {
            let mut session = self.create_session().await?;
            session.prepare_data_query(yql_text.clone()).await
        })
        .await
    }

    /// Describe cluster-wide table option presets (go-sdk: `Session.DescribeTableOptions`).
    pub async fn retry_describe_table_options(&self) -> YdbResult<TableOptionsDescription> {
        self.retry(|| async {
            let mut session = self.create_session().await?;
            session.describe_table_options().await
        })
        .await
    }

    /// Stream-read a table without SQL (go-sdk: `Session.StreamReadTable`).
    pub async fn retry_stream_read_table(
        &self,
        path: impl Into<String>,
        options: ReadTableOptions,
    ) -> YdbResult<StreamReadTableResult> {
        let path = path.into();
        self.retry(|| async {
            let mut session = self.create_session().await?;
            session
                .stream_read_table(path.clone(), options.clone())
                .await
        })
        .await
    }

    /// Prepare and execute a data query on the same session (go-sdk: `Prepare` + `Statement.Execute`).
    pub async fn retry_execute_prepared_query(
        &self,
        yql_text: impl Into<String>,
        query: Query,
        mode: Mode,
    ) -> YdbResult<QueryResult> {
        let yql_text = yql_text.into();
        self.retry(|| async {
            let mut session = self.create_session().await?;
            let prepared = session.prepare_data_query(yql_text.clone()).await?;
            session
                .execute_prepared_query(&prepared, query.clone(), mode)
                .await
        })
        .await
    }
}
