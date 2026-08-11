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
    RawTxMode, begin_tx_control, tx_id_control,
};
use crate::retry_settings::RetrySettings;
use crate::traces::helpers::ensure_len_string;

use crate::types::Value;
use crate::{TransactionOptions, TxMode, closure};
use tracing::instrument;

use crate::session_pool::{SessionPool, SessionPoolLease, spawn_pool_release};

use super::hooks::QueryTxHook;

#[derive(Clone, Debug, Default)]
pub(crate) struct CallOptions {
    pub timeout: Option<Duration>,
    pub idempotent: Option<bool>,
    pub collect_stats: bool,
    /// Override Query Service `commit_tx`. `None` uses context default.
    pub commit_tx: Option<bool>,
    /// Per-call isolation override. `None` uses the surrounding context default.
    pub tx_mode: Option<TxMode>,
    /// One-shot [`QueryClient`] only: send `ExecuteQuery` with an empty `session_id`.
    pub implicit_session: bool,
}

impl CallOptions {
    pub(super) fn idempotency(&self) -> Idempotency {
        self.idempotent.unwrap_or(false).into()
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

pub(crate) enum TxState {
    /// An unfinished transaction always owns exactly one exclusive session lease.
    Active(ActiveTransaction),
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
    pub(crate) fn is_active(&self) -> bool {
        matches!(self, Self::Active(_))
    }
}

pub(crate) struct ActiveTransaction {
    lease: SessionPoolLease,
    server: ServerTransaction,
}

/// Server-side progress within an active transaction.
///
/// In-flight states retain the lease in the transaction so cancellation is conservative: dropping
/// the transaction discards the session instead of issuing a second finalization RPC.
enum ServerTransaction {
    NotStarted,
    BeginInFlight,
    Started(String),
    CommitInFlight(String),
    RollbackInFlight(String),
}

pub(crate) struct TransactionExecContext {
    pub connection_manager: GrpcConnectionManager,
    pub tx_mode: TxMode,
    /// When set, the first operation calls `BeginTransaction` RPC instead of lazy `BeginTx` in `ExecuteQuery`.
    pub begin: bool,
    pub state: TxState,
    pub hooks: Vec<Box<dyn QueryTxHook>>,
    /// Absolute deadline from [`QueryClient::retry_tx`] `.timeout()`, propagated to every RPC in the callback.
    pub retry_deadline: Option<Instant>,
}

impl TransactionExecContext {
    fn active(&self) -> YdbResult<&ActiveTransaction> {
        match &self.state {
            TxState::Active(active) => Ok(active),
            _ => Err(transaction_finished_error()),
        }
    }

    fn active_mut(&mut self) -> YdbResult<&mut ActiveTransaction> {
        match &mut self.state {
            TxState::Active(active) => Ok(active),
            _ => Err(transaction_finished_error()),
        }
    }

    pub(super) fn session_lease(&self) -> YdbResult<&SessionPoolLease> {
        Ok(&self.active()?.lease)
    }

    pub(super) fn transaction_id(&self) -> Option<&str> {
        match &self.state {
            TxState::Active(ActiveTransaction {
                server: ServerTransaction::Started(id),
                ..
            }) => Some(id),
            _ => None,
        }
    }

    fn take_active(&mut self, replacement: TxState) -> YdbResult<ActiveTransaction> {
        let previous = std::mem::replace(&mut self.state, replacement);
        match previous {
            TxState::Active(active) => Ok(active),
            state => {
                self.state = state;
                Err(transaction_finished_error())
            }
        }
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

async fn query_client_from_tx(tx: &TransactionExecContext) -> YdbResult<RawQueryClient> {
    tx.connection_manager
        .get_auth_service_to_node(RawQueryClient::new, tx.session_lease()?.node_uri())
        .await
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
    if let Some(override_mode) = opts.tx_mode
        && override_mode != tx.tx_mode
    {
        return Err(YdbError::Custom(format!(
            "per-call tx_mode {:?} does not match transaction mode {:?}",
            override_mode, tx.tx_mode
        )));
    }
    Ok(())
}

fn interactive_tx_mode(tx: &TransactionExecContext, opts: &CallOptions) -> YdbResult<TxMode> {
    reject_per_call_tx_mode_override(tx, opts)?;
    ensure_interactive_tx_mode(opts.tx_mode.unwrap_or(tx.tx_mode))?;
    Ok(tx.tx_mode)
}

fn client_tx_mode(opts: &CallOptions) -> TxMode {
    opts.tx_mode.unwrap_or(TxMode::Implicit)
}

fn default_commit_tx_client(_mode: TxMode) -> bool {
    true
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
    let commit_tx = opts.commit_tx.unwrap_or(false);
    Ok(Some(match &tx.active()?.server {
        ServerTransaction::Started(id) => {
            interactive_tx_mode(tx, opts)?;
            tx_id_control(id, commit_tx)
        }
        ServerTransaction::NotStarted => {
            reject_per_call_tx_mode_override(tx, opts)?;
            ensure_interactive_tx_mode(tx.tx_mode)?;
            begin_tx_control(tx_mode_to_raw(tx.tx_mode)?, commit_tx)
        }
        ServerTransaction::BeginInFlight
        | ServerTransaction::CommitInFlight(_)
        | ServerTransaction::RollbackInFlight(_) => {
            return Err(YdbError::InternalError(
                "query transaction operation is already in progress".to_string(),
            ));
        }
    }))
}

pub(crate) fn resolve_commit_tx(core: &super::internal::ExecCoreRef, opts: &CallOptions) -> bool {
    if let Some(commit_tx) = opts.commit_tx {
        return commit_tx;
    }
    match core {
        super::internal::ExecCoreRef::Client(_) => default_commit_tx_client(client_tx_mode(opts)),
        super::internal::ExecCoreRef::Transaction(_) => false,
    }
}

/// Build `tx_control` for one-shot [`QueryClient`] calls.
///
/// Default [`TxMode::Implicit`] omits `tx_control` (server-side inference).
fn tx_control_for_client(
    opts: &CallOptions,
) -> YdbResult<Option<ydb_grpc::ydb_proto::query::TransactionControl>> {
    let tx_mode = client_tx_mode(opts);
    if tx_mode == TxMode::Implicit {
        return Ok(None);
    }
    let commit_tx = opts
        .commit_tx
        .unwrap_or_else(|| default_commit_tx_client(tx_mode));
    Ok(Some(begin_tx_control(tx_mode_to_raw(tx_mode)?, commit_tx)))
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
            opts.idempotency(),
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
    let client = query_client_from_tx(tx).await?;
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
    match &tx.active()?.server {
        ServerTransaction::Started(_) => return Ok(()),
        ServerTransaction::NotStarted => {}
        ServerTransaction::BeginInFlight
        | ServerTransaction::CommitInFlight(_)
        | ServerTransaction::RollbackInFlight(_) => {
            return Err(YdbError::InternalError(
                "query transaction operation is already in progress".to_string(),
            ));
        }
    }
    ensure_interactive_tx_mode(tx.tx_mode)?;
    tx.session_lease()?.ensure_healthy()?;
    tx.active_mut()?.server = ServerTransaction::BeginInFlight;

    let result = async {
        let active = tx.active()?;
        let session_id = active.lease.session_id();
        tracing::Span::current().record("ydb.session.id", session_id);
        let mut client = tx
            .connection_manager
            .get_auth_service_to_node(RawQueryClient::new, active.lease.node_uri())
            .await?;
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
            tx.active_mut()?.server = ServerTransaction::Started(tx_id);
            Ok(())
        }
        Err(err) => {
            let active = tx.active_mut()?;
            active.server = ServerTransaction::NotStarted;
            if err.requires_session_discard() {
                active.lease.invalidate();
            }
            Err(err)
        }
    }
}

/// Finish a successful transaction query after its response stream reaches EOF.
pub(crate) fn transaction_finish_query(
    tx: &mut TransactionExecContext,
    commit_at_end: bool,
) -> YdbResult<()> {
    if commit_at_end {
        tx.take_active(TxState::Committed)?.lease.return_to_pool();
        return Ok(());
    }

    let message = match &tx.active()?.server {
        ServerTransaction::Started(_) => return Ok(()),
        ServerTransaction::BeginInFlight => "ExecuteQuery response missing transaction id",
        ServerTransaction::NotStarted
        | ServerTransaction::CommitInFlight(_)
        | ServerTransaction::RollbackInFlight(_) => {
            "query transaction reached an invalid state after ExecuteQuery"
        }
    };
    let error = YdbError::InternalError(message.to_string());
    let mut active = tx.take_active(TxState::Ambiguous(error.clone()))?;
    active.lease.invalidate();
    Err(error)
}

async fn transaction_before_commit(tx: &mut TransactionExecContext) -> YdbResult<()> {
    for hook in &mut tx.hooks {
        hook.before_commit().await?;
    }
    Ok(())
}

/// Apply a query error to the retained session and transaction state.
pub(crate) fn transaction_handle_query_error(tx: &mut TransactionExecContext, err: &YdbError) {
    if err.invalidates_server_transaction() && tx.state.is_active() {
        let previous = std::mem::replace(&mut tx.state, TxState::Invalidated(err.clone()));
        let TxState::Active(mut active) = previous else {
            tx.state = previous;
            return;
        };
        if err.requires_session_discard() {
            active.lease.invalidate();
        }
        active.lease.return_to_pool();
    } else if let TxState::Active(active) = &mut tx.state
        && err.requires_session_discard()
    {
        active.lease.invalidate();
    }
}

pub(crate) fn transaction_invalidate_session(tx: &mut TransactionExecContext) {
    if let TxState::Active(active) = &mut tx.state {
        active.lease.invalidate();
    }
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
    tx.active()?;
    let effective_timeout = resolve_effective_timeout(tx.retry_deadline, opts.timeout);
    let result: YdbResult<ExecuteQueryStream> =
        maybe_with_operation_timeout(effective_timeout, async {
            tx.session_lease()?.ensure_healthy()?;
            tracing::Span::current().record("ydb.session.id", tx.session_lease()?.session_id());
            if tx.begin {
                transaction_ensure_begin(tx).await?;
            }
            if opts.commit_tx.unwrap_or(false) {
                transaction_before_commit(tx).await?;
            }
            let (mut client, req) =
                transaction_execute_request(tx, text, params, &opts, concurrent_result_sets)
                    .await?;
            let starts_transaction = matches!(tx.active()?.server, ServerTransaction::NotStarted);
            if starts_transaction {
                tx.active_mut()?.server = ServerTransaction::BeginInFlight;
            }
            let stream = client.execute_query(req).await.map_err(YdbError::from)?;
            let mut stream = ExecuteQueryStream::new(stream);
            stream.prime_first_part().await?;
            if !stream.in_progress() {
                let error = YdbError::InternalError(
                    "ExecuteQuery response stream closed before the first part".to_string(),
                );
                let mut active = tx.take_active(TxState::Ambiguous(error.clone()))?;
                active.lease.invalidate();
                return Err(error);
            }
            let tx_id = stream.take_captured_tx_id();
            apply_stream_tx_id(tx, tx_id);
            Ok(stream)
        })
        .await;
    if let Err(err) = &result {
        if matches!(
            tx.state,
            TxState::Active(ActiveTransaction {
                server: ServerTransaction::BeginInFlight,
                ..
            })
        ) {
            let mut active = tx.take_active(TxState::Ambiguous(err.clone()))?;
            active.lease.invalidate();
            active.lease.return_to_pool();
        } else {
            transaction_handle_query_error(tx, err);
        }
    }
    result
}

#[instrument(name = "ydb.Commit", skip_all, fields(db.system.name = "ydb", ydb.tx.id = tracing::field::Empty, ydb.session.id = tracing::field::Empty), err)]
pub(crate) async fn transaction_commit(tx: &mut TransactionExecContext) -> YdbResult<()> {
    if !tx.state.is_active() {
        return Ok(());
    }
    if let Err(err) = transaction_before_commit(tx).await {
        let _ = transaction_rollback(tx).await;
        return Err(err);
    }
    let transaction_id = match &tx.active()?.server {
        ServerTransaction::Started(id) => Some(id.clone()),
        ServerTransaction::NotStarted => None,
        ServerTransaction::BeginInFlight
        | ServerTransaction::CommitInFlight(_)
        | ServerTransaction::RollbackInFlight(_) => {
            return Err(YdbError::InternalError(
                "query transaction operation is already in progress".to_string(),
            ));
        }
    };
    match transaction_id {
        None => {
            tx.take_active(TxState::Committed)?.lease.return_to_pool();
            return Ok(());
        }
        Some(id) => {
            tx.active_mut()?.server = ServerTransaction::CommitInFlight(id);
        }
    }
    let result = async {
        let active = tx.active()?;
        let ServerTransaction::CommitInFlight(tx_id) = &active.server else {
            return Err(YdbError::InternalError(
                "query transaction is not committing".to_string(),
            ));
        };
        let session_id = active.lease.session_id();
        tracing::Span::current()
            .record("ydb.session.id", session_id)
            .record("ydb.tx.id", tx_id.as_str());
        let mut client = tx
            .connection_manager
            .get_auth_service_to_node(RawQueryClient::new, active.lease.node_uri())
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

    let terminal = match &result {
        Ok(()) => TxState::Committed,
        Err(err) => TxState::Ambiguous(err.clone()),
    };
    let active = tx.take_active(terminal)?;
    // Do not retry commit: a transport timeout may mean the commit succeeded server-side.
    active.lease.finish(result)
}

#[instrument(name = "ydb.Rollback", skip_all, fields(db.system.name = "ydb", ydb.tx.id = tracing::field::Empty, ydb.session.id = tracing::field::Empty), err)]
pub(crate) async fn transaction_rollback(tx: &mut TransactionExecContext) -> YdbResult<()> {
    if !tx.state.is_active() {
        return Ok(());
    }
    let transaction_id = match &tx.active()?.server {
        ServerTransaction::Started(id) => Some(id.clone()),
        ServerTransaction::NotStarted => None,
        ServerTransaction::BeginInFlight
        | ServerTransaction::CommitInFlight(_)
        | ServerTransaction::RollbackInFlight(_) => {
            return Err(YdbError::InternalError(
                "query transaction operation is already in progress".to_string(),
            ));
        }
    };
    match transaction_id {
        None => {
            tx.take_active(TxState::RolledBack)?.lease.return_to_pool();
            return Ok(());
        }
        Some(id) => {
            tx.active_mut()?.server = ServerTransaction::RollbackInFlight(id);
        }
    }

    let result = async {
        let active = tx.active()?;
        let ServerTransaction::RollbackInFlight(tx_id) = &active.server else {
            return Err(YdbError::InternalError(
                "query transaction is not rolling back".to_string(),
            ));
        };
        let session_id = active.lease.session_id();
        tracing::Span::current()
            .record("ydb.session.id", session_id)
            .record("ydb.tx.id", tx_id.as_str());
        let mut client = tx
            .connection_manager
            .get_auth_service_to_node(RawQueryClient::new, active.lease.node_uri())
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
    let active = tx.take_active(terminal)?;
    active.lease.finish(result)
}

/// Best-effort rollback when [`super::Transaction`] is dropped without `commit`/`rollback`.
pub(crate) fn finish_query_tx_on_drop(
    connection_manager: GrpcConnectionManager,
    active: ActiveTransaction,
) {
    let ActiveTransaction { lease, server } = active;
    let tx_id = match server {
        ServerTransaction::NotStarted => {
            lease.return_to_pool();
            return;
        }
        ServerTransaction::Started(tx_id) => tx_id,
        ServerTransaction::BeginInFlight
        | ServerTransaction::CommitInFlight(_)
        | ServerTransaction::RollbackInFlight(_) => return,
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
        state: TxState::Active(ActiveTransaction {
            lease,
            server: ServerTransaction::NotStarted,
        }),
        hooks: Vec::new(),
        retry_deadline,
    }
}

pub(crate) fn apply_stream_tx_id(tx: &mut TransactionExecContext, tx_id: Option<String>) {
    let Some(id) = tx_id else {
        return;
    };
    let Ok(active) = tx.active_mut() else {
        tracing::warn!("query transaction received tx_id after it finished");
        return;
    };
    match &active.server {
        ServerTransaction::NotStarted | ServerTransaction::BeginInFlight => {
            active.server = ServerTransaction::Started(id);
        }
        ServerTransaction::Started(existing) => {
            if existing != &id {
                tracing::warn!(
                    existing = existing.as_str(),
                    incoming = id.as_str(),
                    "query transaction tx_id changed in stream; keeping first value"
                );
            }
        }
        ServerTransaction::CommitInFlight(_) | ServerTransaction::RollbackInFlight(_) => {
            tracing::warn!(
                incoming = id.as_str(),
                "query transaction received tx_id while finalization was in progress"
            );
        }
    }
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
    use crate::errors::{Idempotency, YdbError, YdbOrCustomerError};

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
        use crate::client_query::TransactionOptions;
        use crate::grpc_connection_manager::GrpcConnectionManager;
        use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
        use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};
        use crate::session_pool::{SessionPool, SessionPoolSettings};
        use http::Uri;
        use ydb_grpc::ydb_proto::status_ids::StatusCode;

        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let mut ctx = transaction_exec_context(
            GrpcConnectionManager::new(
                SharedLoadBalancer::new_with_balancer(Box::new(StaticLoadBalancer::new(
                    Uri::from_static("http://127.0.0.1/bench"),
                ))),
                "bench".to_string(),
                MultiInterceptor::new(),
                GrpcOptions::default(),
            ),
            lease,
            TransactionOptions::default(),
            None,
        );
        ctx.active_mut().expect("active transaction").server =
            ServerTransaction::Started("tx-1".to_string());
        transaction_handle_query_error(
            &mut ctx,
            &YdbError::YdbStatusError(crate::errors::YdbStatusError {
                message: "bad".into(),
                operation_status: StatusCode::GenericError as i32,
                issues: vec![],
            }),
        );
        assert!(!ctx.state.is_active());
        assert!(ctx.transaction_id().is_none());
        transaction_rollback(&mut ctx).await.expect("rollback nop");
    }

    #[tokio::test]
    async fn in_flight_transaction_cleanup_discards_its_session() {
        use crate::client_query::TransactionOptions;
        use crate::grpc_connection_manager::GrpcConnectionManager;
        use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
        use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};
        use crate::session_pool::{SessionPool, SessionPoolSettings};
        use http::Uri;

        let manager = GrpcConnectionManager::new(
            SharedLoadBalancer::new_with_balancer(Box::new(StaticLoadBalancer::new(
                Uri::from_static("http://127.0.0.1/bench"),
            ))),
            "bench".to_string(),
            MultiInterceptor::new(),
            GrpcOptions::default(),
        );
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let session_id = lease.session_id().to_string();
        let mut ctx =
            transaction_exec_context(manager.clone(), lease, TransactionOptions::default(), None);
        ctx.active_mut().expect("active transaction").server = ServerTransaction::BeginInFlight;
        let active = ctx
            .take_active(TxState::RolledBack)
            .expect("take active transaction");

        finish_query_tx_on_drop(manager, active);

        let replacement = pool
            .acquire_explicit()
            .await
            .expect("acquire replacement session");
        assert_ne!(replacement.session_id(), session_id);
        replacement.return_to_pool();
    }
}
