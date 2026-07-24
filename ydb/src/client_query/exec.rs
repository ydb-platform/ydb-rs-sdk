use std::collections::HashMap;
use std::future::Future;
use std::time::{Duration, Instant};

use http::Uri;
use tokio::time::timeout;

use crate::errors::{YdbError, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;
use crate::grpc_wrapper::raw_query_service::execute_query::RawExecuteQueryRequest;
use crate::grpc_wrapper::raw_query_service::stream::ExecuteQueryStream;
use crate::grpc_wrapper::raw_query_service::transaction_control::{
    RawTxMode, begin_tx_control, tx_id_control,
};
use crate::retry_budget::ArcRetrySettings;
use crate::traces::helpers::ensure_len_string;

use crate::types::Value;
use crate::{TransactionOptions, TxMode, closure};
use tracing::instrument;

use crate::session_pool::{SessionPool, SessionPoolLease, spawn_pool_release};

use super::hooks::QueryTxHook;

/// Tracks in-flight ExecuteQuery RPC on a pooled session held by [`ExecuteQueryStream`].
struct PooledQuerySessionGuard {
    lease: SessionPoolLease,
    rpc_finished: bool,
}

impl PooledQuerySessionGuard {
    fn finish_rpc(&mut self) {
        if !self.rpc_finished {
            self.lease.end_use();
            self.rpc_finished = true;
        }
    }
}

impl Drop for PooledQuerySessionGuard {
    fn drop(&mut self) {
        if !self.rpc_finished {
            self.lease.invalidate_session();
        }
    }
}

pub(crate) fn finish_pooled_query_stream(stream: &mut ExecuteQueryStream) {
    stream.finish_session_guard::<PooledQuerySessionGuard>(|guard| guard.finish_rpc());
}

#[derive(Clone, Debug, Default)]
pub(crate) struct CallOptions {
    pub timeout: Option<Duration>,
    pub idempotent: Option<bool>,
    pub collect_stats: bool,
    /// Override Query Service `commit_tx`. `None` uses context default.
    pub commit_tx: Option<bool>,
    /// Per-call isolation override. `None` → [`TxMode::Implicit`] on client,
    /// [`TransactionExecContext::tx_mode`] in interactive transactions.
    pub tx_mode: Option<TxMode>,
    /// One-shot [`QueryClient`] only: send `ExecuteQuery` with an empty `session_id`.
    pub implicit_session: bool,
}

#[derive(Clone)]
pub(crate) struct ClientExecContext {
    pub connection_manager: GrpcConnectionManager,
    pub session_pool: SessionPool,
    pub retry_settings: ArcRetrySettings,
}

#[derive(Clone, Debug)]
pub(crate) enum TxState {
    /// Still going: further queries are allowed, and a final commit is still pending.
    Active,
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
        matches!(self, Self::Active)
    }
}

pub(crate) struct TransactionExecContext {
    pub connection_manager: GrpcConnectionManager,
    pub tx_mode: TxMode,
    pub session_pool: SessionPool,
    /// When set, the first operation calls `BeginTransaction` RPC instead of lazy `BeginTx` in `ExecuteQuery`.
    pub begin: bool,
    pub pooled_lease: Option<SessionPoolLease>,
    pub query_node: Option<Uri>,
    pub tx_id: Option<String>,
    pub state: TxState,
    pub hooks: Vec<Box<dyn QueryTxHook>>,
    /// Absolute deadline from [`QueryClient::retry_tx`] `.timeout()`, propagated to every RPC in the callback.
    pub retry_deadline: Option<Instant>,
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
    if let Some(uri) = &tx.query_node {
        tx.connection_manager
            .get_auth_service_to_node(RawQueryClient::new, uri)
            .await
    } else {
        tx.connection_manager
            .get_auth_service(RawQueryClient::new)
            .await
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

fn client_tx_mode(opts: &CallOptions) -> TxMode {
    opts.tx_mode.unwrap_or(TxMode::Implicit)
}

fn interactive_tx_mode(tx: &TransactionExecContext, opts: &CallOptions) -> YdbResult<TxMode> {
    reject_per_call_tx_mode_override(tx, opts)?;
    ensure_interactive_tx_mode(opts.tx_mode.unwrap_or(tx.tx_mode))?;
    Ok(tx.tx_mode)
}

fn default_commit_tx_client(_mode: TxMode) -> bool {
    // All one-shot modes auto-commit today; revisit if a future mode should not.
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
    if !tx.state.is_active() {
        return Err(YdbError::Custom(
            "transaction already finished (committed or rolled back)".to_string(),
        ));
    }
    let commit_tx = opts.commit_tx.unwrap_or(false);
    Ok(Some(match &tx.tx_id {
        Some(id) => {
            interactive_tx_mode(tx, opts)?;
            tx_id_control(id, commit_tx)
        }
        None => {
            reject_per_call_tx_mode_override(tx, opts)?;
            ensure_interactive_tx_mode(tx.tx_mode)?;
            begin_tx_control(tx_mode_to_raw(tx.tx_mode)?, commit_tx)
        }
    }))
}

pub(crate) fn resolve_commit_tx(core: &super::internal::ExecCoreRef, opts: &CallOptions) -> bool {
    if let Some(v) = opts.commit_tx {
        return v;
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
    let mode = client_tx_mode(opts);
    if mode == TxMode::Implicit {
        return Ok(None);
    }
    let commit_tx = opts
        .commit_tx
        .unwrap_or_else(|| default_commit_tx_client(mode));
    Ok(Some(begin_tx_control(tx_mode_to_raw(mode)?, commit_tx)))
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
async fn client_begin_stream_once(
    ctx: &ClientExecContext,
    text: &str,
    params: &HashMap<String, Value>,
    opts: &CallOptions,
    concurrent_result_sets: bool,
) -> YdbResult<ExecuteQueryStream> {
    if opts.implicit_session {
        let (mut client, req) =
            client_implicit_session_request(ctx, text, params, opts, concurrent_result_sets)
                .await?;
        let stream = client.execute_query(req).await.map_err(YdbError::from)?;
        return Ok(ExecuteQueryStream::new(stream));
    }

    let lease = ctx.session_pool.acquire_explicit().await?;
    let mut pooled_lease = Some(lease);
    let lease_ref = pooled_lease
        .as_mut()
        .expect("lease set on successful acquire");
    let (mut client, req) =
        client_pooled_explicit_request(ctx, lease_ref, text, params, opts, concurrent_result_sets)
            .await?;
    let stream = client.execute_query(req).await.map_err(YdbError::from)?;
    let mut stream = ExecuteQueryStream::new(stream);
    if let Some(lease) = pooled_lease.take() {
        stream = stream.with_session_guard(PooledQuerySessionGuard {
            lease,
            rpc_finished: false,
        });
    }
    Ok(stream)
}

async fn client_pooled_explicit_request(
    ctx: &ClientExecContext,
    lease: &mut SessionPoolLease,
    text: &str,
    params: &HashMap<String, Value>,
    opts: &CallOptions,
    concurrent_result_sets: bool,
) -> YdbResult<(RawQueryClient, RawExecuteQueryRequest)> {
    lease.ensure_alive()?;
    lease.begin_use();
    let node_uri = lease.node_uri().clone();
    let client = ctx
        .connection_manager
        .get_auth_service_to_node(RawQueryClient::new, &node_uri)
        .await?;
    let mut req = RawExecuteQueryRequest::new(
        lease.session_id(),
        text,
        params.clone(),
        tx_control_for_client(opts)?,
        opts.collect_stats,
    );
    req.concurrent_result_sets = concurrent_result_sets;
    Ok((client, req))
}

#[instrument(name = "ydb.Query.BeginStream", skip_all, fields(db.system.name = "ydb", ydb.Query.text = %ensure_len_string(&text), ydb.Query.params = ?params, ydb.Query.opts = ?opts), err)]
pub(crate) async fn client_begin_stream(
    ctx: &ClientExecContext,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
    concurrent_result_sets: bool,
) -> YdbResult<ExecuteQueryStream> {
    ctx.retry_settings
        .as_ref()
        .with_deadline(opts.timeout)
        .retry_on_retriable_errors(
            opts.idempotent.unwrap_or(false).into(),
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

/// Interactive transactions need a stable attached session from the driver pool.
#[instrument(name = "ydb.Query.EnsureTxSession", skip_all, fields(db.system.name = "ydb"), err)]
async fn ensure_tx_session(tx: &mut TransactionExecContext) -> YdbResult<()> {
    if let Some(lease) = &tx.pooled_lease {
        lease.ensure_alive()?;
        return Ok(());
    }
    let lease = tx.session_pool.acquire_explicit().await?;
    tx.query_node = Some(lease.node_uri().clone());
    tx.pooled_lease = Some(lease);
    Ok(())
}

fn tx_session_id(tx: &TransactionExecContext) -> YdbResult<&str> {
    tx.pooled_lease
        .as_ref()
        .map(|lease| lease.session_id())
        .ok_or_else(|| YdbError::Custom("query transaction session is not initialized".to_string()))
}

/// Session and transaction ids for cross-service RPCs (e.g. topic `UpdateOffsetsInTransaction`).
pub(crate) async fn transaction_identity(
    tx: &mut TransactionExecContext,
) -> YdbResult<(String, String)> {
    transaction_ensure_begin(tx, false).await?;
    let session_id = tx_session_id(tx)?.to_string();
    let transaction_id = tx
        .tx_id
        .as_deref()
        .filter(|id| !id.is_empty())
        .ok_or_else(|| YdbError::Custom("query transaction id is not available".to_string()))?
        .to_string();
    Ok((session_id, transaction_id))
}

#[instrument(name = "ydb.Query.ReleaseTxSession", skip_all, fields(db.system.name = "ydb"))]
async fn release_tx_session(tx: &mut TransactionExecContext) {
    if let Some(lease) = tx.pooled_lease.take() {
        lease.return_to_pool().await;
    }
    tx.query_node = None;
}

async fn release_tx_session_handling_error(
    tx: &mut TransactionExecContext,
    err: Option<&YdbError>,
) {
    if let Some(err) = err
        && let Some(lease) = &mut tx.pooled_lease
    {
        lease.handle_pool_error(err);
    }
    release_tx_session(tx).await;
}

#[instrument(name = "ydb.ExecuteQuery", skip_all, fields(db.system.name = "ydb", ydb.Query.text = %ensure_len_string(&yql_text), ydb.Query.params = ?parameters, ydb.Query.opts = ?opts))]
async fn transaction_execute_request(
    tx: &TransactionExecContext,
    yql_text: String,
    parameters: HashMap<String, Value>,
    opts: &CallOptions,
    concurrent_result_sets: bool,
) -> YdbResult<(RawQueryClient, RawExecuteQueryRequest)> {
    let session_id = tx_session_id(tx)?.to_string();
    let client = query_client_from_tx(tx).await?;
    let mut req = RawExecuteQueryRequest::new(
        session_id,
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
pub(crate) async fn transaction_ensure_begin(
    tx: &mut TransactionExecContext,
    session_ready: bool,
) -> YdbResult<()> {
    if !tx.state.is_active() {
        return Err(YdbError::Custom(
            "transaction already finished (committed or rolled back)".to_string(),
        ));
    }
    if tx.tx_id.as_ref().is_some_and(|id| !id.is_empty()) {
        return Ok(());
    }
    ensure_interactive_tx_mode(tx.tx_mode)?;
    if !session_ready {
        ensure_tx_session(tx).await?;
    }
    let session_id = tx_session_id(tx)?.to_string();
    tracing::Span::current().record("ydb.session.id", &session_id);
    let mut client = query_client_from_tx(tx).await?;
    let tx_id =
        maybe_with_operation_timeout(resolve_effective_timeout(tx.retry_deadline, None), async {
            client
                .begin_transaction(&session_id, tx_mode_to_raw(tx.tx_mode)?)
                .await
                .map_err(Into::into)
        })
        .await?;
    apply_stream_tx_id(tx, Some(tx_id));
    Ok(())
}

/// Mark the transaction committed by the server as part of the last `ExecuteQuery` (`commit_tx: true`).
pub(crate) async fn transaction_finish_committed_via_query(tx: &mut TransactionExecContext) {
    tx.state = TxState::Committed;
    tx.tx_id = None;
    release_tx_session(tx).await;
}

async fn transaction_before_commit(tx: &mut TransactionExecContext) -> YdbResult<()> {
    for hook in &mut tx.hooks {
        hook.before_commit().await?;
    }
    Ok(())
}

/// Server ended the transaction after a definitive operation error on a query.
pub(crate) fn transaction_mark_invalidated_on_query_error(
    tx: &mut TransactionExecContext,
    err: &YdbError,
) {
    if tx.state.is_active() && err.invalidates_server_transaction() {
        tx.state = TxState::Invalidated(err.clone());
        tx.tx_id = None;
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
    if !tx.state.is_active() {
        return Err(YdbError::Custom(
            "transaction already finished (committed or rolled back)".to_string(),
        ));
    }
    let effective_timeout = resolve_effective_timeout(tx.retry_deadline, opts.timeout);
    let result: YdbResult<ExecuteQueryStream> =
        maybe_with_operation_timeout(effective_timeout, async {
            ensure_tx_session(tx).await?;
            if let Some(lease) = &mut tx.pooled_lease {
                lease.begin_use();
            }
            if let Ok(session_id) = tx_session_id(tx) {
                tracing::Span::current().record("ydb.session.id", session_id);
            }
            if tx.begin {
                transaction_ensure_begin(tx, true).await?;
            }
            if opts.commit_tx.unwrap_or(false) {
                transaction_before_commit(tx).await?;
            }
            let (mut client, req) =
                transaction_execute_request(tx, text, params, &opts, concurrent_result_sets)
                    .await?;
            let stream = client.execute_query(req).await.map_err(YdbError::from)?;
            let mut stream = ExecuteQueryStream::new(stream);
            stream.prime_first_part().await?;
            if let Some(id) = stream.take_captured_tx_id() {
                apply_stream_tx_id(tx, Some(id));
            }
            Ok(stream)
        })
        .await;
    if let Err(err) = &result {
        transaction_mark_invalidated_on_query_error(tx, err);
        if let Some(lease) = &mut tx.pooled_lease {
            lease.handle_pool_error(err);
            lease.end_use();
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
    if tx.tx_id.as_ref().is_none_or(String::is_empty) {
        tx.state = TxState::Committed;
        release_tx_session(tx).await;
        return Ok(());
    }
    ensure_tx_session(tx).await?;
    let tx_id = tx.tx_id.take().expect("checked Some");
    let session_id = tx_session_id(tx)?.to_string();
    tracing::Span::current()
        .record("ydb.session.id", &session_id)
        .record("ydb.tx.id", &tx_id);
    let mut client = query_client_from_tx(tx).await?;
    let result =
        maybe_with_operation_timeout(resolve_effective_timeout(tx.retry_deadline, None), async {
            client
                .commit_transaction(&session_id, &tx_id)
                .await
                .map_err(Into::into)
        })
        .await;
    release_tx_session_handling_error(tx, result.as_ref().err()).await;
    tx.state = match &result {
        Ok(()) => TxState::Committed,
        Err(err) => TxState::Ambiguous(err.clone()),
    };
    // Do not retry commit: a transport timeout may mean the commit succeeded server-side.
    result
}

#[instrument(name = "ydb.Rollback", skip_all, fields(db.system.name = "ydb", ydb.tx.id = tracing::field::Empty, ydb.session.id = tracing::field::Empty), err)]
pub(crate) async fn transaction_rollback(tx: &mut TransactionExecContext) -> YdbResult<()> {
    if !tx.state.is_active() {
        return Ok(());
    }
    let mut rollback_err: Option<YdbError> = None;
    if tx.tx_id.as_ref().is_some_and(|id| !id.is_empty()) && tx.pooled_lease.is_some() {
        let tx_id = tx.tx_id.take().expect("checked Some");
        if let Ok(session_id) = tx_session_id(tx)
            && let Ok(mut client) = query_client_from_tx(tx).await
        {
            tracing::Span::current()
                .record("ydb.session.id", session_id)
                .record("ydb.tx.id", &tx_id);
            let rollback_result = maybe_with_operation_timeout(
                resolve_effective_timeout(tx.retry_deadline, None),
                async {
                    client
                        .rollback_transaction(session_id, &tx_id)
                        .await
                        .map_err(Into::into)
                },
            )
            .await;
            if let Err(err) = rollback_result {
                rollback_err = Some(err);
            }
        }
    } else {
        tx.tx_id = None;
    }
    release_tx_session_handling_error(tx, rollback_err.as_ref()).await;
    if let Some(err) = rollback_err {
        tx.state = TxState::Ambiguous(err.clone());
        Err(err)
    } else {
        tx.state = TxState::RolledBack;
        Ok(())
    }
}

/// Best-effort rollback when [`super::Transaction`] is dropped without `commit`/`rollback`.
pub(crate) fn spawn_query_tx_rollback_on_drop(ctx: &mut TransactionExecContext) {
    let tx_id = ctx.tx_id.take();
    let Some(mut lease) = ctx.pooled_lease.take() else {
        ctx.query_node = None;
        return;
    };
    let connection_manager = ctx.connection_manager.clone();
    let query_node = ctx.query_node.take();

    if tx_id.as_ref().is_none_or(String::is_empty) {
        lease.invalidate_session();
        spawn_pool_release(async move {
            lease.return_to_pool().await;
        });
        return;
    }

    let tx_id = tx_id.expect("checked Some");
    spawn_pool_release(async move {
        let session_id = lease.session_id().to_string();
        let client_result = if let Some(uri) = query_node {
            connection_manager
                .get_auth_service_to_node(RawQueryClient::new, &uri)
                .await
        } else {
            connection_manager
                .get_auth_service(RawQueryClient::new)
                .await
        };
        let rollback_ok = match client_result {
            Ok(mut client) => client
                .rollback_transaction(&session_id, &tx_id)
                .await
                .is_ok(),
            Err(_) => false,
        };
        if !rollback_ok {
            lease.invalidate_session();
        }
        lease.return_to_pool().await;
    });
}

pub(crate) fn transaction_exec_context(
    connection_manager: GrpcConnectionManager,
    session_pool: SessionPool,
    options: TransactionOptions,
    retry_deadline: Option<Instant>,
) -> TransactionExecContext {
    TransactionExecContext {
        connection_manager,
        session_pool,
        tx_mode: options.mode(),
        begin: options.begin(),
        pooled_lease: None,
        query_node: None,
        tx_id: None,
        state: TxState::Active,
        hooks: Vec::new(),
        retry_deadline,
    }
}

pub(crate) fn apply_stream_tx_id(tx: &mut TransactionExecContext, tx_id: Option<String>) {
    let Some(id) = tx_id.filter(|id| !id.is_empty()) else {
        return;
    };
    if let Some(existing) = &tx.tx_id {
        if *existing != id {
            tracing::warn!(
                existing = existing.as_str(),
                incoming = id.as_str(),
                "query transaction tx_id changed in stream; keeping first value"
            );
        }
        return;
    }
    tx.tx_id = Some(id);
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

        let mut ctx = transaction_exec_context(
            GrpcConnectionManager::new(
                SharedLoadBalancer::new_with_balancer(Box::new(StaticLoadBalancer::new(
                    Uri::from_static("http://127.0.0.1/bench"),
                ))),
                "bench".to_string(),
                MultiInterceptor::new(),
                GrpcOptions::default(),
            ),
            SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1)),
            TransactionOptions::default(),
            None,
        );
        ctx.tx_id = Some("tx-1".into());
        transaction_mark_invalidated_on_query_error(
            &mut ctx,
            &YdbError::YdbStatusError(crate::errors::YdbStatusError {
                message: "bad".into(),
                operation_status: StatusCode::GenericError as i32,
                issues: vec![],
            }),
        );
        assert!(!ctx.state.is_active());
        assert!(ctx.tx_id.is_none());
        transaction_rollback(&mut ctx).await.expect("rollback nop");
    }
}
