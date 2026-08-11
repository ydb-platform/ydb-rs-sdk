use std::collections::HashMap;
use std::future::Future;
use std::time::{Duration, Instant};

use tokio::time::timeout;

use crate::errors::{Idempotency, YdbError, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;
use crate::grpc_wrapper::raw_query_service::execute_query::RawExecuteQueryRequest;
use crate::grpc_wrapper::raw_query_service::stream::ExecuteQueryStream;
use crate::grpc_wrapper::raw_query_service::transaction_control::{
    RawTxMode, TransactionId, begin_tx_control, tx_id_control,
};
use crate::retry_settings::RetrySettings;
use crate::traces::helpers::ensure_len_string;

use crate::types::Value;
use crate::{TransactionOptions, TxMode, closure};
use tracing::instrument;

use crate::session_pool::{SessionPool, SessionPoolLease, spawn_pool_release};

use super::hooks::{QueryTxCommitStatus, QueryTxHook};

#[derive(Clone, Debug)]
pub(crate) struct CallOptions {
    pub timeout: Option<Duration>,
    pub idempotency: Idempotency,
    pub collect_stats: bool,
    pub commit_tx: bool,
    /// Explicit per-call transaction mode. `None` uses the surrounding context default.
    pub tx_mode_override: Option<TxMode>,
    /// One-shot [`QueryClient`] only: send `ExecuteQuery` with an empty `session_id`.
    pub implicit_session: bool,
}

impl CallOptions {
    pub(super) fn for_transaction() -> Self {
        Self {
            commit_tx: false,
            ..Self::default()
        }
    }
}

impl Default for CallOptions {
    fn default() -> Self {
        Self {
            timeout: None,
            idempotency: Idempotency::NonIdempotent,
            collect_stats: false,
            commit_tx: true,
            tx_mode_override: None,
            implicit_session: false,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ClientExecContext {
    pub connection_manager: GrpcConnectionManager,
    pub session_pool: SessionPool,
    pub retry_settings: RetrySettings,
}

/// Opened client stream and its explicit session ownership mode.
pub(crate) struct OpenedClientQueryStream {
    pub(crate) stream: ExecuteQueryStream,
    pub(crate) session: ClientQuerySession,
}

pub(crate) enum ClientQuerySession {
    ServerImplicit,
    Pooled(SessionPoolLease),
}

impl ClientQuerySession {
    pub(crate) fn complete(self) {
        if let Self::Pooled(lease) = self {
            lease.return_to_pool();
        }
    }
}

/// Local transaction lifetime. Only `Live` owns server resources and pending hooks; every
/// terminal state is ownership-free.
pub(crate) enum TxState {
    /// The only state that owns a session lease and pending hooks.
    Live(LiveTransaction),
    /// Real, confirmed commit: either `CommitTransaction` succeeded or `commit_tx` completed.
    Committed,
    /// Rollback path was chosen and the SDK must not report a commit.
    RolledBack,
    /// The server ended the transaction after a definitive status on a query.
    Invalidated(YdbError),
    /// A commit or rollback RPC returned an error, so the local end attempt was not confirmed.
    Ambiguous(YdbError),
}

impl TxState {
    pub(crate) fn is_live(&self) -> bool {
        matches!(self, Self::Live(_))
    }

    pub(crate) fn operation_is_in_flight(&self) -> bool {
        matches!(self, Self::Live(live) if live.server.is_in_flight())
    }
}

pub(crate) struct LiveTransaction {
    lease: SessionPoolLease,
    server: ServerTransaction,
    hooks: Vec<Box<dyn QueryTxHook>>,
}

/// Server-side progress within a live transaction.
///
/// In-flight states retain the lease in the transaction so cancellation is conservative: dropping
/// the transaction discards the session instead of issuing a second finalization RPC.
enum ServerTransaction {
    NotStarted,
    Ready(TransactionId),
    InFlight(InFlightOperation),
}

enum InFlightOperation {
    Begin,
    FirstQuery,
    Query(TransactionId),
    Commit(TransactionId),
    Rollback(TransactionId),
}

enum FinalizationDispatch {
    Local,
    Rpc,
}

impl ServerTransaction {
    // Moving a transaction id between enum variants requires replacing the complete value. Every
    // rejected transition restores the original state before returning its error.
    fn operation_in_progress_error(&self) -> YdbError {
        YdbError::InternalError("query transaction operation is already in progress".to_string())
    }

    fn query_not_in_progress_error() -> YdbError {
        YdbError::InternalError("query transaction is not executing a query".to_string())
    }

    fn mark_query_dispatched(&mut self) -> YdbResult<()> {
        let previous = std::mem::replace(self, Self::NotStarted);
        match previous {
            Self::NotStarted => {
                *self = Self::InFlight(InFlightOperation::FirstQuery);
                Ok(())
            }
            Self::Ready(transaction_id) => {
                *self = Self::InFlight(InFlightOperation::Query(transaction_id));
                Ok(())
            }
            in_flight => {
                let error = in_flight.operation_in_progress_error();
                *self = in_flight;
                Err(error)
            }
        }
    }

    fn finish_query(&mut self) -> YdbResult<()> {
        let previous = std::mem::replace(self, Self::NotStarted);
        match previous {
            Self::InFlight(InFlightOperation::Query(transaction_id)) => {
                *self = Self::Ready(transaction_id);
                Ok(())
            }
            Self::InFlight(InFlightOperation::FirstQuery) => {
                *self = Self::InFlight(InFlightOperation::FirstQuery);
                Err(YdbError::InternalError(
                    "ExecuteQuery response missing transaction id".to_string(),
                ))
            }
            state => {
                let error = Self::query_not_in_progress_error();
                *self = state;
                Err(error)
            }
        }
    }

    fn restore_after_query_error(&mut self) -> YdbResult<()> {
        let previous = std::mem::replace(self, Self::NotStarted);
        match previous {
            Self::InFlight(InFlightOperation::FirstQuery) => Ok(()),
            Self::InFlight(InFlightOperation::Query(transaction_id)) => {
                *self = Self::Ready(transaction_id);
                Ok(())
            }
            state => {
                let error = Self::query_not_in_progress_error();
                *self = state;
                Err(error)
            }
        }
    }

    fn is_in_flight(&self) -> bool {
        matches!(self, Self::InFlight(_))
    }

    fn is_query_in_flight(&self) -> bool {
        matches!(
            self,
            Self::InFlight(InFlightOperation::FirstQuery | InFlightOperation::Query(_))
        )
    }

    fn capture_query_transaction_id(&mut self, incoming: TransactionId) -> YdbResult<()> {
        match self {
            Self::InFlight(InFlightOperation::FirstQuery) => {
                *self = Self::InFlight(InFlightOperation::Query(incoming));
                Ok(())
            }
            Self::InFlight(InFlightOperation::Query(existing)) if existing == &incoming => Ok(()),
            Self::InFlight(InFlightOperation::Query(existing)) => {
                Err(YdbError::InternalError(format!(
                    "query transaction id changed from {} to {}",
                    existing.as_str(),
                    incoming.as_str()
                )))
            }
            _ => Err(YdbError::InternalError(
                "query response contained a transaction id while no query was in progress"
                    .to_string(),
            )),
        }
    }

    fn mark_commit_dispatched(&mut self) -> YdbResult<FinalizationDispatch> {
        self.mark_finalization_dispatched(InFlightOperation::Commit)
    }

    fn mark_rollback_dispatched(&mut self) -> YdbResult<FinalizationDispatch> {
        self.mark_finalization_dispatched(InFlightOperation::Rollback)
    }

    fn mark_finalization_dispatched(
        &mut self,
        operation: fn(TransactionId) -> InFlightOperation,
    ) -> YdbResult<FinalizationDispatch> {
        let previous = std::mem::replace(self, Self::NotStarted);
        match previous {
            Self::NotStarted => Ok(FinalizationDispatch::Local),
            Self::Ready(transaction_id) => {
                *self = Self::InFlight(operation(transaction_id));
                Ok(FinalizationDispatch::Rpc)
            }
            state => {
                let error = state.operation_in_progress_error();
                *self = state;
                Err(error)
            }
        }
    }
}

pub(crate) struct TransactionExecContext {
    pub connection_manager: GrpcConnectionManager,
    pub tx_mode: TxMode,
    /// When set, the first operation calls `BeginTransaction` RPC instead of lazy `BeginTx` in `ExecuteQuery`.
    pub begin: bool,
    pub state: TxState,
    /// Absolute deadline from [`QueryClient::retry_tx`] `.timeout()`, propagated to every RPC in the callback.
    pub retry_deadline: Option<Instant>,
}

impl LiveTransaction {
    fn notify_hooks(&mut self, status: QueryTxCommitStatus) {
        for hook in &mut self.hooks {
            hook.after_commit(status);
        }
    }

    fn finish(mut self, status: QueryTxCommitStatus) -> SessionPoolLease {
        self.notify_hooks(status);
        self.lease
    }
}

impl TransactionExecContext {
    fn live(&self) -> YdbResult<&LiveTransaction> {
        match &self.state {
            TxState::Live(live) => Ok(live),
            _ => Err(transaction_finished_error()),
        }
    }

    fn live_mut(&mut self) -> YdbResult<&mut LiveTransaction> {
        match &mut self.state {
            TxState::Live(live) => Ok(live),
            _ => Err(transaction_finished_error()),
        }
    }

    pub(super) fn session_lease(&self) -> YdbResult<&SessionPoolLease> {
        Ok(&self.live()?.lease)
    }

    pub(super) fn transaction_id(&self) -> Option<&TransactionId> {
        match &self.state {
            TxState::Live(LiveTransaction {
                server: ServerTransaction::Ready(id),
                ..
            }) => Some(id),
            _ => None,
        }
    }

    pub(super) fn register_hook(&mut self, hook: Box<dyn QueryTxHook>) -> YdbResult<()> {
        self.live_mut()?.hooks.push(hook);
        Ok(())
    }

    fn take_live(&mut self, replacement: TxState) -> YdbResult<LiveTransaction> {
        let previous = std::mem::replace(&mut self.state, replacement);
        match previous {
            TxState::Live(live) => Ok(live),
            state => {
                self.state = state;
                Err(transaction_finished_error())
            }
        }
    }

    fn finish_live(
        &mut self,
        replacement: TxState,
        status: QueryTxCommitStatus,
    ) -> YdbResult<SessionPoolLease> {
        Ok(self.take_live(replacement)?.finish(status))
    }

    pub(super) fn finish_query(&mut self, commit_at_end: bool) -> YdbResult<()> {
        if commit_at_end {
            if !self.live()?.server.is_query_in_flight() {
                let error = YdbError::InternalError(
                    "transaction query completed while no query was in progress".to_string(),
                );
                self.abort_unconfirmed(error.clone())?;
                return Err(error);
            }
            self.finish_live(TxState::Committed, QueryTxCommitStatus::Committed)?
                .return_to_pool();
            return Ok(());
        }

        if let Err(error) = self.live_mut()?.server.finish_query() {
            self.abort_unconfirmed(error.clone())?;
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn abort_unconfirmed(&mut self, error: YdbError) -> YdbResult<()> {
        self.finish_live(TxState::Ambiguous(error), QueryTxCommitStatus::Aborted)?
            .discard();
        Ok(())
    }

    pub(super) fn handle_query_error(&mut self, error: &YdbError) -> YdbResult<()> {
        if error.invalidates_server_transaction() {
            let lease = self.finish_live(
                TxState::Invalidated(error.clone()),
                QueryTxCommitStatus::Aborted,
            )?;
            if !error.requires_session_discard() {
                lease.return_to_pool();
            } else {
                lease.discard();
            }
            return Ok(());
        }

        if error.requires_session_discard() {
            return self.abort_unconfirmed(error.clone());
        }

        if self.live()?.server.is_query_in_flight() {
            self.live_mut()?.server.restore_after_query_error()?;
        }
        Ok(())
    }

    pub(super) fn cancel_query(&mut self) -> YdbResult<()> {
        self.abort_unconfirmed(YdbError::InternalError(
            "query response stream was cancelled before completion".into(),
        ))
    }

    pub(super) fn apply_stream_transaction_id(
        &mut self,
        transaction_id: Option<TransactionId>,
    ) -> YdbResult<()> {
        let Some(transaction_id) = transaction_id else {
            return Ok(());
        };
        if let Err(error) = self
            .live_mut()?
            .server
            .capture_query_transaction_id(transaction_id)
        {
            self.abort_unconfirmed(error.clone())?;
            return Err(error);
        }
        Ok(())
    }
}

fn transaction_finished_error() -> YdbError {
    YdbError::Custom("transaction already finished (committed or rolled back)".to_string())
}

/// Per-call timeout capped by the parent [`retry_tx`](crate::QueryClient::retry_tx) deadline when set.
pub(crate) fn resolve_effective_timeout(
    deadline: Option<Instant>,
    call_timeout: Option<Duration>,
) -> Option<Duration> {
    let remaining = deadline.and_then(|d| d.checked_duration_since(Instant::now()));
    match (call_timeout, remaining) {
        (None, None) => None,
        (Some(c), None) => Some(c),
        (None, Some(r)) => Some(r),
        (Some(c), Some(r)) => Some(c.min(r)),
    }
}

pub(crate) async fn maybe_with_operation_timeout<T, F>(
    timeout: Option<Duration>,
    operation: F,
) -> YdbResult<T>
where
    F: Future<Output = YdbResult<T>>,
{
    match timeout {
        Some(duration) => with_operation_timeout(duration, operation).await,
        None => operation.await,
    }
}

pub(crate) async fn with_operation_timeout<T, F>(
    timeout_duration: Duration,
    operation: F,
) -> YdbResult<T>
where
    F: Future<Output = YdbResult<T>>,
{
    match timeout(timeout_duration, operation).await {
        Ok(result) => result,
        Err(_) => Err(YdbError::Transport(format!(
            "operation timed out after {timeout_duration:?}"
        ))),
    }
}

fn tx_mode_to_raw(mode: TxMode) -> YdbResult<RawTxMode> {
    match mode {
        TxMode::Implicit => Err(YdbError::Custom(
            "TxMode::Implicit cannot be converted to a raw tx mode; \
             use server-side inference (no tx_control) instead"
                .to_string(),
        )),
        TxMode::SerializableReadWrite => Ok(RawTxMode::SerializableReadWrite),
        TxMode::SnapshotReadOnly => Ok(RawTxMode::SnapshotReadOnly),
        TxMode::SnapshotReadWrite => Ok(RawTxMode::SnapshotReadWrite),
        TxMode::StaleReadOnly => Ok(RawTxMode::StaleReadOnly),
        TxMode::OnlineReadOnly => Ok(RawTxMode::OnlineReadOnly),
        TxMode::OnlineReadOnlyInconsistent => Ok(RawTxMode::OnlineReadOnlyInconsistent),
    }
}

fn ensure_interactive_tx_mode(mode: TxMode) -> YdbResult<()> {
    if mode == TxMode::Implicit {
        return Err(YdbError::Custom(
            "TxMode::Implicit is not available inside Transaction; \
             DDL and other non-transactional statements must run on QueryClient, not inside tx"
                .to_string(),
        ));
    }
    if !mode.supported_in_interactive() {
        return Err(YdbError::Custom(format!(
            "transaction mode {mode:?} is not supported in interactive transactions \
             (use SerializableReadWrite, SnapshotReadOnly, or SnapshotReadWrite)"
        )));
    }
    Ok(())
}

fn reject_per_call_tx_mode_override(
    tx: &TransactionExecContext,
    opts: &CallOptions,
) -> YdbResult<()> {
    if let Some(override_mode) = opts.tx_mode_override
        && override_mode != tx.tx_mode
    {
        return Err(YdbError::Custom(format!(
            "per-call tx_mode {:?} does not match transaction mode {:?}",
            override_mode, tx.tx_mode
        )));
    }
    Ok(())
}

/// Build `tx_control` for an interactive transaction.
///
/// **Lazy start (default):** while `tx_id` is unknown, the first `ExecuteQuery` sends
/// `BeginTx` with `commit_tx: false` — no upfront `BeginTransaction` RPC. The server
/// returns `tx_id` in the response stream; later queries use `TxId`.
///
/// **Explicit begin:** when [`TransactionExecContext::begin`] is set or
/// [`transaction_ensure_begin`] was called, `tx_id` is already known and this
/// function always emits `TxId`.
fn tx_control_for_transaction(
    tx: &TransactionExecContext,
    opts: &CallOptions,
) -> YdbResult<Option<ydb_grpc::ydb_proto::query::TransactionControl>> {
    Ok(Some(match &tx.live()?.server {
        ServerTransaction::Ready(id) => {
            reject_per_call_tx_mode_override(tx, opts)?;
            ensure_interactive_tx_mode(tx.tx_mode)?;
            tx_id_control(id, opts.commit_tx)
        }
        ServerTransaction::NotStarted => {
            reject_per_call_tx_mode_override(tx, opts)?;
            ensure_interactive_tx_mode(tx.tx_mode)?;
            begin_tx_control(tx_mode_to_raw(tx.tx_mode)?, opts.commit_tx)
        }
        ServerTransaction::InFlight(_) => {
            return Err(tx.live()?.server.operation_in_progress_error());
        }
    }))
}

/// Build `tx_control` for one-shot [`QueryClient`] calls.
///
/// Default [`TxMode::Implicit`] omits `tx_control` (server-side inference).
fn tx_control_for_client(
    opts: &CallOptions,
) -> YdbResult<Option<ydb_grpc::ydb_proto::query::TransactionControl>> {
    let tx_mode = opts.tx_mode_override.unwrap_or(TxMode::Implicit);
    if tx_mode == TxMode::Implicit {
        return Ok(None);
    }
    Ok(Some(begin_tx_control(
        tx_mode_to_raw(tx_mode)?,
        opts.commit_tx,
    )))
}

async fn client_implicit_session_request(
    ctx: &ClientExecContext,
    text: &str,
    params: &HashMap<String, Value>,
    opts: &CallOptions,
    concurrent_result_sets: bool,
) -> YdbResult<(RawQueryClient, RawExecuteQueryRequest)> {
    let client = ctx
        .connection_manager
        .get_auth_service(RawQueryClient::new)
        .await?;
    let mut req = RawExecuteQueryRequest::new(
        "",
        text,
        params.clone(),
        tx_control_for_client(opts)?,
        opts.collect_stats,
    );
    req.concurrent_result_sets = concurrent_result_sets;
    Ok((client, req))
}

#[instrument(name = "ydb.Query.BeginStreamOnce", skip_all, fields(db.system.name = "ydb"), err)]
pub(super) async fn client_begin_stream_once(
    ctx: &ClientExecContext,
    text: &str,
    params: &HashMap<String, Value>,
    opts: &CallOptions,
    concurrent_result_sets: bool,
) -> YdbResult<OpenedClientQueryStream> {
    if opts.implicit_session {
        let (mut client, req) =
            client_implicit_session_request(ctx, text, params, opts, concurrent_result_sets)
                .await?;
        let stream = client.execute_query(req).await.map_err(YdbError::from)?;
        return Ok(OpenedClientQueryStream {
            stream: ExecuteQueryStream::new(stream),
            session: ClientQuerySession::ServerImplicit,
        });
    }

    open_pooled_query_stream(ctx, text, params, opts, concurrent_result_sets).await
}

async fn open_pooled_query_stream(
    ctx: &ClientExecContext,
    text: &str,
    params: &HashMap<String, Value>,
    opts: &CallOptions,
    concurrent_result_sets: bool,
) -> YdbResult<OpenedClientQueryStream> {
    let tx_control = tx_control_for_client(opts)?;
    let lease = ctx.session_pool.acquire_explicit().await?;
    let result = async {
        lease.ensure_healthy()?;
        let mut client = ctx
            .connection_manager
            .get_auth_service_to_node(RawQueryClient::new, lease.node_uri())
            .await?;
        let mut req = RawExecuteQueryRequest::new(
            lease.session_id(),
            text,
            params.clone(),
            tx_control,
            opts.collect_stats,
        );
        req.concurrent_result_sets = concurrent_result_sets;
        let stream = client.execute_query(req).await.map_err(YdbError::from)?;
        Ok(ExecuteQueryStream::new(stream))
    }
    .await;

    match result {
        Ok(stream) => Ok(OpenedClientQueryStream {
            stream,
            session: ClientQuerySession::Pooled(lease),
        }),
        Err(err) => lease.finish(Err(err)),
    }
}

#[instrument(name = "ydb.Query.BeginStream", skip_all, fields(db.system.name = "ydb", ydb.Query.text = %ensure_len_string(&text), ydb.Query.params = ?params, ydb.Query.opts = ?opts), err)]
pub(crate) async fn client_begin_stream(
    ctx: &ClientExecContext,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
    concurrent_result_sets: bool,
) -> YdbResult<OpenedClientQueryStream> {
    ctx.retry_settings
        .clone()
        .with_deadline(opts.timeout)
        .retry_on_retriable_errors(
            opts.idempotency,
            closure!([&ctx, &text, &params, &opts], |_| client_begin_stream_once(
                ctx,
                text,
                params,
                opts,
                concurrent_result_sets
            )),
        )
        .await
}

/// Session and transaction ids for cross-service RPCs (e.g. topic `UpdateOffsetsInTransaction`).
pub(crate) async fn transaction_identity(
    tx: &mut TransactionExecContext,
) -> YdbResult<(String, String)> {
    transaction_ensure_begin(tx).await?;
    let session_id = tx.session_lease()?.session_id().to_string();
    let transaction_id = tx
        .transaction_id()
        .ok_or_else(|| YdbError::Custom("query transaction id is not available".to_string()))?
        .as_str()
        .to_string();
    Ok((session_id, transaction_id))
}

#[instrument(name = "ydb.ExecuteQuery", skip_all, fields(db.system.name = "ydb", ydb.Query.text = %ensure_len_string(&yql_text), ydb.Query.params = ?parameters, ydb.Query.opts = ?opts))]
async fn transaction_execute_request(
    tx: &TransactionExecContext,
    yql_text: String,
    parameters: HashMap<String, Value>,
    opts: &CallOptions,
    concurrent_result_sets: bool,
) -> YdbResult<(RawQueryClient, RawExecuteQueryRequest)> {
    let client = tx
        .connection_manager
        .get_auth_service_to_node(RawQueryClient::new, tx.session_lease()?.node_uri())
        .await?;
    let mut req = RawExecuteQueryRequest::new(
        tx.session_lease()?.session_id(),
        yql_text,
        parameters,
        tx_control_for_transaction(tx, opts)?,
        opts.collect_stats,
    );
    req.concurrent_result_sets = concurrent_result_sets;
    Ok((client, req))
}

/// Open the transaction via `BeginTransaction` RPC (explicit begin).
#[instrument(name = "ydb.Query.TransactionEnsureBegin", skip_all, fields(db.system.name = "ydb", ydb.tx.mode = ?tx.tx_mode, ydb.session.id = tracing::field::Empty), err)]
pub(crate) async fn transaction_ensure_begin(tx: &mut TransactionExecContext) -> YdbResult<()> {
    match &tx.live()?.server {
        ServerTransaction::Ready(_) => return Ok(()),
        ServerTransaction::NotStarted => {}
        ServerTransaction::InFlight(_) => {
            return Err(tx.live()?.server.operation_in_progress_error());
        }
    }
    ensure_interactive_tx_mode(tx.tx_mode)?;
    tx.session_lease()?.ensure_healthy()?;
    let client = tx
        .connection_manager
        .get_auth_service_to_node(RawQueryClient::new, tx.session_lease()?.node_uri())
        .await;
    let mut client = match client {
        Ok(client) => client,
        Err(error) => {
            if error.requires_session_discard() {
                tx.live_mut()?.lease.invalidate();
            }
            return Err(error);
        }
    };
    tx.live_mut()?.server = ServerTransaction::InFlight(InFlightOperation::Begin);

    let result = async {
        let session_id = tx.session_lease()?.session_id();
        tracing::Span::current().record("ydb.session.id", session_id);
        maybe_with_operation_timeout(resolve_effective_timeout(tx.retry_deadline, None), async {
            client
                .begin_transaction(session_id, tx_mode_to_raw(tx.tx_mode)?)
                .await
                .map_err(Into::into)
        })
        .await
    }
    .await;

    match result {
        Ok(tx_id) => {
            tx.live_mut()?.server = ServerTransaction::Ready(tx_id);
            Ok(())
        }
        Err(err) => {
            let live = tx.live_mut()?;
            live.server = ServerTransaction::NotStarted;
            if err.requires_session_discard() {
                live.lease.invalidate();
            }
            Err(err)
        }
    }
}

async fn transaction_before_commit(tx: &mut TransactionExecContext) -> YdbResult<()> {
    for hook in &mut tx.live_mut()?.hooks {
        hook.before_commit().await?;
    }
    Ok(())
}

#[instrument(name = "ydb.Query.TransactionBeginStream", skip_all, fields(db.system.name = "ydb", ydb.tx.mode = ?tx.tx_mode, ydb.session.id = tracing::field::Empty), err)]
pub(crate) async fn transaction_begin_stream(
    tx: &mut TransactionExecContext,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
    concurrent_result_sets: bool,
) -> YdbResult<ExecuteQueryStream> {
    debug_assert!(
        !opts.implicit_session,
        "implicit_session is only available on QueryClient one-shot builders"
    );
    tx.live()?;
    let effective_timeout = resolve_effective_timeout(tx.retry_deadline, opts.timeout);
    let result: YdbResult<ExecuteQueryStream> =
        maybe_with_operation_timeout(effective_timeout, async {
            tx.session_lease()?.ensure_healthy()?;
            tracing::Span::current().record("ydb.session.id", tx.session_lease()?.session_id());
            if tx.begin {
                transaction_ensure_begin(tx).await?;
            }
            if opts.commit_tx {
                transaction_before_commit(tx).await?;
            }
            let (mut client, req) =
                transaction_execute_request(tx, text, params, &opts, concurrent_result_sets)
                    .await?;
            tx.live_mut()?.server.mark_query_dispatched()?;
            let stream = client.execute_query(req).await.map_err(YdbError::from)?;
            let mut stream = ExecuteQueryStream::new(stream);
            let first_part = stream.prime_first_part().await.map_err(YdbError::from);
            tx.apply_stream_transaction_id(stream.take_captured_tx_id())?;
            first_part?;
            if !stream.in_progress() {
                let error = YdbError::InternalError(
                    "ExecuteQuery response stream closed before the first part".to_string(),
                );
                tx.abort_unconfirmed(error.clone())?;
                return Err(error);
            }
            Ok(stream)
        })
        .await;
    match result {
        Ok(stream) => Ok(stream),
        Err(error) => {
            if tx.state.is_live() {
                tx.handle_query_error(&error)?;
            }
            Err(error)
        }
    }
}

#[instrument(name = "ydb.Commit", skip_all, fields(db.system.name = "ydb", ydb.tx.id = tracing::field::Empty, ydb.session.id = tracing::field::Empty), err)]
pub(crate) async fn transaction_commit(tx: &mut TransactionExecContext) -> YdbResult<()> {
    if !tx.state.is_live() {
        return Ok(());
    }
    if let Err(hook_error) = transaction_before_commit(tx).await {
        return match transaction_rollback(tx).await {
            Err(invariant) if tx.state.is_live() => Err(invariant),
            // A dispatched rollback already moved the transaction to a terminal state. Preserve
            // the hook failure that caused it as the operation's primary error.
            Ok(()) | Err(_) => Err(hook_error),
        };
    }
    if matches!(tx.live()?.server, ServerTransaction::Ready(_))
        && let Err(error) = tx.live()?.lease.ensure_healthy()
    {
        let lease = tx.finish_live(
            TxState::Ambiguous(error.clone()),
            QueryTxCommitStatus::Aborted,
        )?;
        return lease.finish(Err(error));
    }
    match tx.live_mut()?.server.mark_commit_dispatched()? {
        FinalizationDispatch::Local => {
            tx.finish_live(TxState::Committed, QueryTxCommitStatus::Committed)?
                .return_to_pool();
            return Ok(());
        }
        FinalizationDispatch::Rpc => {}
    }
    let result = async {
        let live = tx.live()?;
        let ServerTransaction::InFlight(InFlightOperation::Commit(tx_id)) = &live.server else {
            return Err(YdbError::InternalError(
                "query transaction is not committing".to_string(),
            ));
        };
        let session_id = live.lease.session_id();
        tracing::Span::current()
            .record("ydb.session.id", session_id)
            .record("ydb.tx.id", tx_id.as_str());
        let mut client = tx
            .connection_manager
            .get_auth_service_to_node(RawQueryClient::new, live.lease.node_uri())
            .await?;
        maybe_with_operation_timeout(resolve_effective_timeout(tx.retry_deadline, None), async {
            client
                .commit_transaction(session_id, tx_id.as_str())
                .await
                .map_err(Into::into)
        })
        .await
    }
    .await;

    let (terminal, hook_status) = match &result {
        Ok(()) => (TxState::Committed, QueryTxCommitStatus::Committed),
        Err(err) => (
            TxState::Ambiguous(err.clone()),
            QueryTxCommitStatus::Aborted,
        ),
    };
    let lease = tx.finish_live(terminal, hook_status)?;
    // Do not retry commit: a transport timeout may mean the commit succeeded server-side.
    lease.finish(result)
}

#[instrument(name = "ydb.Rollback", skip_all, fields(db.system.name = "ydb", ydb.tx.id = tracing::field::Empty, ydb.session.id = tracing::field::Empty), err)]
pub(crate) async fn transaction_rollback(tx: &mut TransactionExecContext) -> YdbResult<()> {
    if !tx.state.is_live() {
        return Ok(());
    }
    match tx.live_mut()?.server.mark_rollback_dispatched()? {
        FinalizationDispatch::Local => {
            tx.finish_live(TxState::RolledBack, QueryTxCommitStatus::Aborted)?
                .return_to_pool();
            return Ok(());
        }
        FinalizationDispatch::Rpc => {}
    }

    let result = async {
        let live = tx.live()?;
        let ServerTransaction::InFlight(InFlightOperation::Rollback(tx_id)) = &live.server else {
            return Err(YdbError::InternalError(
                "query transaction is not rolling back".to_string(),
            ));
        };
        let session_id = live.lease.session_id();
        tracing::Span::current()
            .record("ydb.session.id", session_id)
            .record("ydb.tx.id", tx_id.as_str());
        let mut client = tx
            .connection_manager
            .get_auth_service_to_node(RawQueryClient::new, live.lease.node_uri())
            .await?;
        maybe_with_operation_timeout(resolve_effective_timeout(tx.retry_deadline, None), async {
            client
                .rollback_transaction(session_id, tx_id.as_str())
                .await
                .map_err(Into::into)
        })
        .await
    }
    .await;

    let terminal = match &result {
        Ok(()) => TxState::RolledBack,
        Err(err) => TxState::Ambiguous(err.clone()),
    };
    let lease = tx.finish_live(terminal, QueryTxCommitStatus::Aborted)?;
    lease.finish(result)
}

/// Best-effort rollback when [`super::Transaction`] is dropped without `commit`/`rollback`.
pub(crate) fn finish_query_tx_on_drop(
    connection_manager: GrpcConnectionManager,
    mut live: LiveTransaction,
) {
    live.notify_hooks(QueryTxCommitStatus::Aborted);
    let LiveTransaction {
        lease,
        server,
        hooks: _,
    } = live;
    let tx_id = match server {
        ServerTransaction::NotStarted => {
            lease.return_to_pool();
            return;
        }
        ServerTransaction::Ready(tx_id) => tx_id,
        ServerTransaction::InFlight(_) => {
            lease.discard();
            return;
        }
    };

    spawn_pool_release(async move {
        let client_result = connection_manager
            .get_auth_service_to_node(RawQueryClient::new, lease.node_uri())
            .await;
        let rollback_ok = match client_result {
            Ok(mut client) => client
                .rollback_transaction(lease.session_id(), tx_id.as_str())
                .await
                .is_ok(),
            Err(_) => false,
        };
        if rollback_ok {
            lease.return_to_pool();
        } else {
            lease.discard();
        }
    });
}

pub(crate) fn transaction_exec_context(
    connection_manager: GrpcConnectionManager,
    lease: SessionPoolLease,
    options: TransactionOptions,
    retry_deadline: Option<Instant>,
) -> TransactionExecContext {
    TransactionExecContext {
        connection_manager,
        tx_mode: options.mode(),
        begin: options.begin(),
        state: TxState::Live(LiveTransaction {
            lease,
            server: ServerTransaction::NotStarted,
            hooks: Vec::new(),
        }),
        retry_deadline,
    }
}

#[cfg(test)]
pub(super) fn mark_begin_in_flight_for_test(tx: &mut TransactionExecContext) {
    tx.live_mut().expect("live test transaction").server =
        ServerTransaction::InFlight(InFlightOperation::Begin);
}

#[cfg(test)]
pub(super) fn build_client_execute_request_for_test(
    opts: &CallOptions,
    concurrent_result_sets: bool,
) -> RawExecuteQueryRequest {
    let mut req = RawExecuteQueryRequest::new(
        String::new(),
        "SELECT 1".to_string(),
        HashMap::new(),
        tx_control_for_client(opts).expect("valid test tx_control"),
        opts.collect_stats,
    );
    req.concurrent_result_sets = concurrent_result_sets;
    req
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::GrpcOptions;
    use crate::client_query::TransactionOptions;
    use crate::errors::{Idempotency, YdbError, YdbOrCustomerError};
    use crate::grpc_connection_manager::GrpcConnectionManager;
    use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
    use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};
    use crate::session_pool::{SessionPool, SessionPoolSettings};
    use http::Uri;
    use ydb_grpc::ydb_proto::status_ids::StatusCode;

    fn test_connection_manager() -> GrpcConnectionManager {
        GrpcConnectionManager::new(
            SharedLoadBalancer::new_with_balancer(Box::new(StaticLoadBalancer::new(
                Uri::from_static("http://127.0.0.1/bench"),
            ))),
            "bench".to_string(),
            MultiInterceptor::new(),
            GrpcOptions::default(),
        )
    }

    #[test]
    fn retry_helpers_and_wait() {
        let transport = YdbOrCustomerError::YDB(YdbError::Transport("timeout".into()));
        assert!(transport.is_retriable(Idempotency::Idempotent));
        assert!(YdbError::Transport("timeout".into()).is_retriable(Idempotency::Idempotent));
        assert!(!YdbError::Transport("timeout".into()).is_retriable(Idempotency::NonIdempotent));
        assert!(!YdbOrCustomerError::from_mess("customer").is_retriable(Idempotency::Idempotent));
    }

    #[tokio::test]
    async fn transaction_rollback_is_nop_when_finished() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let mut ctx = transaction_exec_context(
            test_connection_manager(),
            lease,
            TransactionOptions::default(),
            None,
        );
        ctx.live_mut().expect("live transaction").server = ServerTransaction::Ready(
            TransactionId::from_server("tx-1".into()).expect("non-empty transaction id"),
        );
        ctx.handle_query_error(&YdbError::YdbStatusError(crate::errors::YdbStatusError {
            message: "bad".into(),
            operation_status: StatusCode::GenericError as i32,
            issues: vec![],
        }))
        .expect("apply transaction error");
        assert!(!ctx.state.is_live());
        assert!(ctx.transaction_id().is_none());
        transaction_rollback(&mut ctx).await.expect("rollback nop");
    }

    #[tokio::test]
    async fn in_flight_transaction_cleanup_discards_its_session() {
        let manager = test_connection_manager();
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let session_id = lease.session_id().to_string();
        let mut ctx =
            transaction_exec_context(manager.clone(), lease, TransactionOptions::default(), None);
        ctx.live_mut().expect("live transaction").server =
            ServerTransaction::InFlight(InFlightOperation::Begin);
        let live = ctx
            .take_live(TxState::RolledBack)
            .expect("take live transaction");

        finish_query_tx_on_drop(manager, live);

        let replacement = pool
            .acquire_explicit()
            .await
            .expect("acquire replacement session");
        assert_ne!(replacement.session_id(), session_id);
        replacement.return_to_pool();
    }

    #[tokio::test]
    async fn transaction_commit_rejects_an_unhealthy_session() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let mut lease = pool.acquire_explicit().await.expect("acquire test session");
        lease.invalidate();
        let mut ctx = transaction_exec_context(
            test_connection_manager(),
            lease,
            TransactionOptions::default(),
            None,
        );
        ctx.live_mut().expect("live transaction").server = ServerTransaction::Ready(
            TransactionId::from_server("tx-1".into()).expect("non-empty transaction id"),
        );

        let err = transaction_commit(&mut ctx)
            .await
            .expect_err("an unhealthy session must not be committed");
        let YdbError::YdbStatusError(status) = err else {
            panic!("expected BadSession, got {err:?}");
        };
        assert_eq!(status.operation_status, StatusCode::BadSession as i32);
        assert!(matches!(ctx.state, TxState::Ambiguous(_)));
    }

    #[tokio::test]
    async fn conflicting_stream_transaction_id_aborts_the_transaction() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let mut ctx = transaction_exec_context(
            test_connection_manager(),
            lease,
            TransactionOptions::default(),
            None,
        );
        ctx.live_mut()
            .expect("live transaction")
            .server
            .mark_query_dispatched()
            .expect("dispatch first query");
        ctx.apply_stream_transaction_id(Some(
            TransactionId::from_server("tx-1".into()).expect("non-empty transaction id"),
        ))
        .expect("capture first transaction id");

        let error = ctx
            .apply_stream_transaction_id(Some(
                TransactionId::from_server("tx-2".into()).expect("non-empty transaction id"),
            ))
            .expect_err("conflicting transaction id must fail");

        assert!(matches!(error, YdbError::InternalError(_)));
        assert!(matches!(ctx.state, TxState::Ambiguous(_)));
    }
}
