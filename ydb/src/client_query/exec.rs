use std::collections::HashMap;
use std::future::Future;
use std::time::{Duration, Instant};

use futures_util::TryFutureExt;
use tokio::time::timeout;

use crate::client_metrics::names::MetricsNames;
use crate::driver_lifecycle::{DriverGuarded, DriverLifecycle};
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

use crate::session_pool::{SessionPool, SessionPoolLease};

use super::hooks::{QueryTxCommitStatus, QueryTxHook};

#[derive(Clone, Debug, Default)]
pub(crate) struct CallOptions {
    pub timeout: Option<Duration>,
    pub idempotent: Option<bool>,
    pub collect_stats: bool,
    /// Override Query Service `commit_tx`. `None` uses context default.
    pub commit_tx: Option<bool>,
    /// Per-call isolation override. `None` → [`TxMode::Implicit`] on client,
    /// [`TxExecContext::tx_mode`] in interactive transactions.
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
    inner: DriverGuarded<ClientExecContextInner>,
}

#[derive(Clone)]
pub(crate) struct ClientExecContextInner {
    pub connection_manager: GrpcConnectionManager,
    pub session_pool: SessionPool,
    pub retry_settings: RetrySettings,
    pub metrics_names: MetricsNames,
}

impl ClientExecContext {
    pub(crate) fn new(
        connection_manager: GrpcConnectionManager,
        session_pool: SessionPool,
        retry_settings: RetrySettings,
        metrics_names: MetricsNames,
        lifecycle: &DriverLifecycle,
    ) -> Self {
        Self {
            inner: lifecycle.guard(ClientExecContextInner {
                connection_manager,
                session_pool,
                retry_settings,
                metrics_names,
            }),
        }
    }

    pub(crate) fn access(&self) -> YdbResult<&ClientExecContextInner> {
        self.inner.access()
    }
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
    pub(crate) fn release(self) {
        if let Self::Pooled(lease) = self {
            lease.return_to_pool();
        }
    }
}

pub(crate) enum TxState {
    /// An unfinished transaction always owns exactly one exclusive session lease.
    Active(ActiveTx),
    /// No commit remains: no server transaction was created, `CommitTransaction` succeeded, or
    /// the final query completed with `commit_tx`.
    Committed,
    /// Rollback path was chosen and the SDK must not report a commit.
    RolledBack,
    /// A returned operation error ended the local transaction attempt. Its session ownership has
    /// already been resolved; retry policy decides whether to start another attempt.
    AttemptFailed(YdbError),
    /// A dispatched operation's server outcome was not conclusively observed.
    Undetermined(YdbError),
}

impl TxState {
    pub(crate) fn is_active(&self) -> bool {
        matches!(self, Self::Active(_))
    }
}

pub(crate) struct ActiveTx {
    client: Box<RawQueryClient>,
    lease: SessionPoolLease,
    server_progress: TxServerProgress,
    hooks: Vec<Box<dyn QueryTxHook>>,
}

pub(crate) enum ExecTarget<'a> {
    Client(&'a mut ClientExecContext),
    Tx(&'a mut TxExecContext),
}

/// Server-side progress within an active transaction.
///
/// Operation ownership and observed transaction identity are independent, so all four field
/// combinations are valid. Dispatch records `InFlight` before awaiting; only observed successful
/// completion restores `Ready`, making cancellation visible to transaction cleanup.
struct TxServerProgress {
    /// Whether a Query Service transaction RPC currently owns the transaction state.
    operation: TxOperationState,
    /// The transaction ID observed from the server, if one has been received.
    tx_id: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TxOperationState {
    Ready,
    InFlight,
}

impl TxOperationState {
    fn is_in_flight(self) -> bool {
        self == Self::InFlight
    }
}

impl TxServerProgress {
    fn new() -> Self {
        Self {
            operation: TxOperationState::Ready,
            tx_id: None,
        }
    }

    fn operation_in_progress_error() -> YdbError {
        YdbError::InternalError("query transaction operation is already in progress".to_string())
    }

    fn capture_query_transaction_id(&mut self, incoming: String) -> YdbResult<()> {
        if !self.operation.is_in_flight() {
            return Err(YdbError::InternalError(
                "query response contained a transaction id while no query was in progress"
                    .to_string(),
            ));
        }
        match &self.tx_id {
            None => {
                self.tx_id = Some(incoming);
                Ok(())
            }
            Some(existing) if existing == &incoming => Ok(()),
            Some(existing) => Err(YdbError::InternalError(format!(
                "query transaction id changed from {existing} to {incoming}"
            ))),
        }
    }

    fn ready_tx_id(&self) -> Option<&str> {
        if self.operation == TxOperationState::Ready {
            self.tx_id.as_deref()
        } else {
            None
        }
    }
}

impl ActiveTx {
    fn new(client: RawQueryClient, lease: SessionPoolLease) -> Self {
        Self {
            client: Box::new(client),
            lease,
            server_progress: TxServerProgress::new(),
            hooks: Vec::new(),
        }
    }

    fn notify_hooks(&mut self, status: QueryTxCommitStatus) {
        for hook in &mut self.hooks {
            hook.after_commit(status);
        }
    }
}

pub(crate) struct TxExecContext {
    pub tx_mode: TxMode,
    /// When set, the first operation calls `BeginTransaction` RPC instead of lazy `BeginTx` in `ExecuteQuery`.
    pub begin: bool,
    pub state: TxState,
    /// Absolute deadline from [`QueryClient::retry_tx`] `.timeout()`, propagated to every RPC in the callback.
    pub retry_deadline: Option<Instant>,
    pub metrics_names: MetricsNames,
}

impl TxExecContext {
    fn active(&self) -> YdbResult<&ActiveTx> {
        match &self.state {
            TxState::Active(active) => Ok(active),
            _ => Err(tx_finished_error()),
        }
    }

    fn active_mut(&mut self) -> YdbResult<&mut ActiveTx> {
        match &mut self.state {
            TxState::Active(active) => Ok(active),
            _ => Err(tx_finished_error()),
        }
    }

    pub(super) fn session_lease(&self) -> YdbResult<&SessionPoolLease> {
        Ok(&self.active()?.lease)
    }

    pub(super) fn transaction_id(&self) -> Option<&str> {
        match &self.state {
            TxState::Active(active) => active.server_progress.ready_tx_id(),
            _ => None,
        }
    }

    fn replace_active(&mut self, replacement: TxState) -> YdbResult<ActiveTx> {
        let previous = std::mem::replace(&mut self.state, replacement);
        match previous {
            TxState::Active(active) => Ok(active),
            state => {
                self.state = state;
                Err(tx_finished_error())
            }
        }
    }

    pub(super) fn register_hook(&mut self, hook: Box<dyn QueryTxHook>) -> YdbResult<()> {
        self.active_mut()?.hooks.push(hook);
        Ok(())
    }

    fn finish_tx(
        &mut self,
        replacement: TxState,
        status: QueryTxCommitStatus,
    ) -> YdbResult<SessionPoolLease> {
        let mut previous_active = self.replace_active(replacement)?;
        previous_active.notify_hooks(status);
        Ok(previous_active.lease)
    }

    /// End an attempt with an unknown server outcome and discard its session.
    ///
    /// The operation may still be running, so the lease is deliberately not returned regardless
    /// of the error's ordinary session policy.
    fn finish_undetermined(&mut self, error: YdbError) -> YdbResult<()> {
        let lease = self.finish_tx(TxState::Undetermined(error), QueryTxCommitStatus::Aborted)?;
        drop(lease);
        Ok(())
    }

    /// End an observed failed operation using the transaction progress recorded before dispatch.
    fn fail_attempt(&mut self, error: &YdbError) -> YdbResult<()> {
        let mut active = self.replace_active(TxState::AttemptFailed(error.clone()))?;
        active.notify_hooks(QueryTxCommitStatus::Aborted);

        if error.requires_session_discard() {
            drop(active);
            return Ok(());
        }

        let ActiveTx {
            lease,
            server_progress,
            ..
        } = active;
        match (server_progress.operation, server_progress.tx_id) {
            (TxOperationState::Ready, None) => {
                lease.return_to_pool();
            }
            (TxOperationState::Ready, Some(tx_id)) | (TxOperationState::InFlight, Some(tx_id)) => {
                lease.schedule_rollback(tx_id);
            }
            (TxOperationState::InFlight, None) => {}
        }
        Ok(())
    }

    fn handle_pre_dispatch_error(&mut self, error: &YdbError) -> YdbResult<()> {
        if error.requires_session_discard() {
            self.fail_attempt(error)?;
        }
        Ok(())
    }

    /// Resolve an operation that the user callback stopped observing.
    ///
    /// An `InFlight` state after the callback completes means an operation future or query stream
    /// was abandoned before its outcome was observed.
    pub(super) fn resolve_unobserved_operation(&mut self) -> YdbResult<()> {
        let in_flight = match &self.state {
            TxState::Active(active) => active.server_progress.operation.is_in_flight(),
            _ => false,
        };
        if in_flight {
            self.finish_undetermined(YdbError::InternalError(
                "transaction operation was cancelled before its outcome was observed".to_string(),
            ))?;
        }
        Ok(())
    }
}

impl Drop for TxExecContext {
    fn drop(&mut self) {
        let state = std::mem::replace(&mut self.state, TxState::RolledBack);
        let TxState::Active(mut active) = state else {
            return;
        };

        active.notify_hooks(QueryTxCommitStatus::Aborted);
        let ActiveTx {
            lease,
            server_progress,
            ..
        } = active;
        match (server_progress.operation, server_progress.tx_id) {
            (TxOperationState::Ready, None) => lease.return_to_pool(),
            (TxOperationState::Ready, Some(tx_id)) => lease.schedule_rollback(tx_id),
            (TxOperationState::InFlight, _) => {}
        }
    }
}

fn tx_finished_error() -> YdbError {
    YdbError::Custom("query transaction is no longer active".to_string())
}

fn remaining_retry_timeout(deadline: Option<Instant>) -> Option<Duration> {
    deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()))
}

/// Per-call timeout capped by the parent [`retry_tx`](crate::QueryClient::retry_tx) deadline.
fn resolve_operation_timeout(
    deadline: Option<Instant>,
    call_timeout: Option<Duration>,
) -> Option<Duration> {
    match (call_timeout, remaining_retry_timeout(deadline)) {
        (Some(call_timeout), Some(remaining)) => Some(call_timeout.min(remaining)),
        (Some(call_timeout), None) => Some(call_timeout),
        (None, remaining) => remaining,
    }
}

pub(crate) async fn with_optional_timeout<T, F>(
    timeout_duration: Option<Duration>,
    operation: F,
) -> YdbResult<T>
where
    F: Future<Output = YdbResult<T>>,
{
    match timeout_duration {
        Some(duration) => timeout(duration, operation).await?,
        None => operation.await,
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

pub(super) fn ensure_interactive_tx_mode(mode: TxMode) -> YdbResult<()> {
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

fn reject_per_call_tx_mode_override(tx: &TxExecContext, opts: &CallOptions) -> YdbResult<()> {
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

fn interactive_tx_mode(tx: &TxExecContext, opts: &CallOptions) -> YdbResult<TxMode> {
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
/// A transaction without an id starts lazily through `BeginTx`; after explicit begin or the first
/// query, subsequent requests address the transaction by id.
fn tx_control_for_transaction(
    tx: &TxExecContext,
    opts: &CallOptions,
) -> YdbResult<Option<ydb_grpc::ydb_proto::query::TransactionControl>> {
    let commit_tx = opts.commit_tx.unwrap_or(false);
    let progress = &tx.active()?.server_progress;
    if progress.operation.is_in_flight() {
        return Err(TxServerProgress::operation_in_progress_error());
    }
    let tx_mode = interactive_tx_mode(tx, opts)?;
    Ok(Some(match progress.tx_id.as_deref() {
        Some(id) => tx_id_control(id, commit_tx),
        None => begin_tx_control(tx_mode_to_raw(tx_mode)?, commit_tx),
    }))
}

pub(crate) fn resolve_commit_tx(core: &ExecTarget<'_>, opts: &CallOptions) -> bool {
    if let Some(commit_tx) = opts.commit_tx {
        return commit_tx;
    }
    match core {
        ExecTarget::Client(_) => default_commit_tx_client(client_tx_mode(opts)),
        ExecTarget::Tx(_) => false,
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
        .access()?
        .connection_manager
        .get_auth_service(RawQueryClient::new)
        .await?;
    let mut req = RawExecuteQueryRequest::new(
        "",
        text,
        params.clone(),
        tx_control_for_client(opts)?,
        opts.collect_stats,
    )?;
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
    let lease = ctx.access()?.session_pool.acquire_explicit().await?;
    let result = async {
        lease.ensure_healthy()?;
        let mut client = ctx
            .access()?
            .connection_manager
            .get_auth_service_to_node(RawQueryClient::new, lease.node_uri())
            .await?;
        let mut req = RawExecuteQueryRequest::new(
            lease.session_id(),
            text,
            params.clone(),
            tx_control,
            opts.collect_stats,
        )?;
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
    ctx.access()?
        .retry_settings
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

/// Materialize the transaction and return its session-scoped identity for a cross-service RPC.
///
/// This sends `BeginTransaction` when the transaction has not started yet. Both identifiers must
/// be passed together: the transaction exists within this session and retains its exclusive lease.
pub(crate) async fn tx_identity(tx: &mut TxExecContext) -> YdbResult<(String, String)> {
    tx_ensure_begin(tx).await?;
    let session_id = tx.session_lease()?.session_id().to_string();
    let transaction_id = tx
        .transaction_id()
        .ok_or_else(|| YdbError::Custom("query transaction id is not available".to_string()))?
        .to_string();
    Ok((session_id, transaction_id))
}

/// Open the transaction via `BeginTransaction` RPC (explicit begin).
#[instrument(name = "ydb.Query.TransactionEnsureBegin", skip_all, fields(db.system.name = "ydb", ydb.tx.mode = ?tx.tx_mode, ydb.session.id = tracing::field::Empty), err)]
pub(crate) async fn tx_ensure_begin(tx: &mut TxExecContext) -> YdbResult<()> {
    let progress = &tx.active()?.server_progress;
    if progress.operation.is_in_flight() {
        return Err(TxServerProgress::operation_in_progress_error());
    }
    if progress.tx_id.is_some() {
        return Ok(());
    }
    ensure_interactive_tx_mode(tx.tx_mode)?;
    let raw_tx_mode = tx_mode_to_raw(tx.tx_mode)?;
    if let Err(error) = tx.session_lease()?.ensure_healthy() {
        tx.handle_pre_dispatch_error(&error)?;
        return Err(error);
    }
    let progress = &mut tx.active_mut()?.server_progress;
    debug_assert_eq!(progress.operation, TxOperationState::Ready);
    progress.operation = TxOperationState::InFlight;
    let operation_timeout = remaining_retry_timeout(tx.retry_deadline);

    let result = {
        let active = tx.active_mut()?;
        let session_id = active.lease.session_id();
        tracing::Span::current().record("ydb.session.id", session_id);
        let operation = active
            .client
            .begin_transaction(session_id, raw_tx_mode)
            .map_err(YdbError::from);
        with_optional_timeout(operation_timeout, operation).await
    };

    match result {
        Ok(tx_id) => {
            let progress = &mut tx.active_mut()?.server_progress;
            progress.tx_id = Some(tx_id);
            progress.operation = TxOperationState::Ready;
            Ok(())
        }
        Err(err) => {
            tx.fail_attempt(&err)?;
            Err(err)
        }
    }
}

/// Finish a successful transaction query after its response stream reaches EOF.
pub(crate) fn tx_finish_query(tx: &mut TxExecContext, commit_at_end: bool) -> YdbResult<()> {
    if !tx.active()?.server_progress.operation.is_in_flight() {
        let error = YdbError::InternalError(
            "transaction query completed while no query was in progress".to_string(),
        );
        tx.finish_undetermined(error.clone())?;
        return Err(error);
    }

    if commit_at_end {
        tx.metrics_names
            .client_transaction_commit_counter
            .increment(1);
        tx.finish_tx(TxState::Committed, QueryTxCommitStatus::Committed)?
            .return_to_pool();
        return Ok(());
    }

    if tx.active()?.server_progress.tx_id.is_none() {
        let error =
            YdbError::InternalError("ExecuteQuery response missing transaction id".to_string());
        tx.finish_undetermined(error.clone())?;
        return Err(error);
    }
    tx.active_mut()?.server_progress.operation = TxOperationState::Ready;
    Ok(())
}

async fn tx_before_commit(tx: &mut TxExecContext) -> YdbResult<()> {
    for hook in &mut tx.active_mut()?.hooks {
        hook.before_commit().await?;
    }
    Ok(())
}

/// Resolve an error from a transaction query at the RPC dispatch boundary.
///
/// [`TxOperationState::InFlight`] is recorded immediately before awaiting a transaction RPC.
/// It remains set until the response stream completes successfully. Errors before dispatch
/// cannot have changed the server transaction; they end the attempt only when its session is
/// unusable.
pub(crate) fn tx_handle_query_error(tx: &mut TxExecContext, err: &YdbError) -> YdbResult<()> {
    if tx.active()?.server_progress.operation.is_in_flight() {
        return tx.fail_attempt(err);
    }
    tx.handle_pre_dispatch_error(err)
}

/// Cancel an unfinished transaction query.
pub(crate) fn tx_cancel_query(tx: &mut TxExecContext) {
    let TxState::Active(active) = &tx.state else {
        return;
    };
    if !active.server_progress.operation.is_in_flight() {
        tracing::error!("attempted to cancel a transaction query while no query was in progress");
        return;
    }
    if let Err(error) = tx.finish_undetermined(YdbError::InternalError(
        "query response stream was cancelled before completion".to_string(),
    )) {
        tracing::error!(%error, "failed to cancel query transaction stream");
    }
}

#[instrument(name = "ydb.ExecuteQuery", skip_all, fields(db.system.name = "ydb", ydb.Query.text = %ensure_len_string(&text), ydb.Query.params = ?params, ydb.Query.opts = ?opts))]
fn tx_execute_request(
    tx: &TxExecContext,
    text: String,
    params: HashMap<String, Value>,
    opts: &CallOptions,
    concurrent_result_sets: bool,
) -> YdbResult<RawExecuteQueryRequest> {
    let mut request = RawExecuteQueryRequest::new(
        tx.session_lease()?.session_id(),
        text,
        params,
        tx_control_for_transaction(tx, opts)?,
        opts.collect_stats,
    )?;
    request.concurrent_result_sets = concurrent_result_sets;
    Ok(request)
}

#[instrument(name = "ydb.Query.TransactionBeginStream", skip_all, fields(db.system.name = "ydb", ydb.tx.mode = ?tx.tx_mode, ydb.session.id = tracing::field::Empty), err)]
pub(crate) async fn tx_begin_stream(
    tx: &mut TxExecContext,
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
    let operation_timeout = resolve_operation_timeout(tx.retry_deadline, opts.timeout);
    let result: YdbResult<ExecuteQueryStream> = with_optional_timeout(operation_timeout, async {
        tx.session_lease()?.ensure_healthy()?;
        tracing::Span::current().record("ydb.session.id", tx.session_lease()?.session_id());
        if tx.begin {
            tx_ensure_begin(tx).await?;
        }
        if opts.commit_tx.unwrap_or(false) {
            tx_before_commit(tx).await?;
        }
        let request = tx_execute_request(tx, text, params, &opts, concurrent_result_sets)?;
        let progress = &mut tx.active_mut()?.server_progress;
        debug_assert_eq!(progress.operation, TxOperationState::Ready);
        progress.operation = TxOperationState::InFlight;
        let stream = tx
            .active_mut()?
            .client
            .execute_query(request)
            .await
            .map_err(YdbError::from)?;
        let mut stream = ExecuteQueryStream::new(stream);
        stream.prime_first_part().await?;
        if !stream.in_progress() {
            let error = YdbError::InternalError(
                "ExecuteQuery response stream closed before the first part".to_string(),
            );
            return Err(error);
        }
        let tx_id = stream.take_captured_tx_id();
        apply_stream_tx_id(tx, tx_id)?;
        Ok(stream)
    })
    .await;
    if let Err(err) = &result
        && tx.state.is_active()
    {
        tx_handle_query_error(tx, err)?;
    }
    result
}

#[instrument(name = "ydb.Commit", skip_all, fields(db.system.name = "ydb", ydb.tx.id = tracing::field::Empty, ydb.session.id = tracing::field::Empty), err)]
pub(crate) async fn tx_commit(tx: &mut TxExecContext) -> YdbResult<()> {
    if !tx.state.is_active() {
        return Ok(());
    }
    if let Err(err) = tx_before_commit(tx).await {
        if let Err(error) = tx_rollback(tx).await {
            tracing::warn!(
                %error,
                "rollback after transaction before-commit hook failure did not complete"
            );
        }
        return Err(err);
    }
    let operation_timeout = remaining_retry_timeout(tx.retry_deadline);
    let commit_counter = tx.metrics_names.client_transaction_commit_counter.clone();
    let active = tx.active_mut()?;
    if active.server_progress.operation.is_in_flight() {
        return Err(TxServerProgress::operation_in_progress_error());
    }
    let Some(tx_id) = active.server_progress.tx_id.as_deref() else {
        tx.finish_tx(TxState::Committed, QueryTxCommitStatus::Committed)?
            .return_to_pool();
        return Ok(());
    };

    commit_counter.increment(1);

    debug_assert_eq!(active.server_progress.operation, TxOperationState::Ready);
    active.server_progress.operation = TxOperationState::InFlight;
    let session_id = active.lease.session_id();
    tracing::Span::current()
        .record("ydb.session.id", session_id)
        .record("ydb.tx.id", tx_id);
    let operation = active
        .client
        .commit_transaction(session_id, tx_id)
        .map_err(YdbError::from);
    let result = with_optional_timeout(operation_timeout, operation).await;

    match result {
        Ok(()) => {
            tx.finish_tx(TxState::Committed, QueryTxCommitStatus::Committed)?
                .return_to_pool();
            Ok(())
        }
        Err(error) => {
            tx.fail_attempt(&error)?;
            Err(error)
        }
    }
}

#[instrument(name = "ydb.Rollback", skip_all, fields(db.system.name = "ydb", ydb.tx.id = tracing::field::Empty, ydb.session.id = tracing::field::Empty), err)]
pub(crate) async fn tx_rollback(tx: &mut TxExecContext) -> YdbResult<()> {
    if !tx.state.is_active() {
        return Ok(());
    }
    tx.metrics_names
        .client_transaction_rollback_counter
        .increment(1);
    let operation_timeout = remaining_retry_timeout(tx.retry_deadline);
    let active = tx.active_mut()?;
    if active.server_progress.operation.is_in_flight() {
        return Err(TxServerProgress::operation_in_progress_error());
    }
    let Some(tx_id) = active.server_progress.tx_id.as_deref() else {
        tx.finish_tx(TxState::RolledBack, QueryTxCommitStatus::Aborted)?
            .return_to_pool();
        return Ok(());
    };

    debug_assert_eq!(active.server_progress.operation, TxOperationState::Ready);
    active.server_progress.operation = TxOperationState::InFlight;
    let session_id = active.lease.session_id();
    tracing::Span::current()
        .record("ydb.session.id", session_id)
        .record("ydb.tx.id", tx_id);
    let operation = active
        .client
        .rollback_transaction(session_id, tx_id)
        .map_err(YdbError::from);
    let result = with_optional_timeout(operation_timeout, operation).await;

    match result {
        Ok(()) => {
            tx.finish_tx(TxState::RolledBack, QueryTxCommitStatus::Aborted)?
                .return_to_pool();
            Ok(())
        }
        Err(error) => {
            tx.fail_attempt(&error)?;
            Err(error)
        }
    }
}

pub(crate) fn tx_exec_context(
    client: RawQueryClient,
    lease: SessionPoolLease,
    options: TransactionOptions,
    retry_deadline: Option<Instant>,
    metrics_names: MetricsNames,
) -> TxExecContext {
    TxExecContext {
        tx_mode: options.mode(),
        begin: options.begin(),
        state: TxState::Active(ActiveTx::new(client, lease)),
        retry_deadline,
        metrics_names,
    }
}

pub(crate) fn apply_stream_tx_id(tx: &mut TxExecContext, tx_id: Option<String>) -> YdbResult<()> {
    let Some(id) = tx_id.filter(|id| !id.is_empty()) else {
        return Ok(());
    };
    if let Err(error) = tx
        .active_mut()?
        .server_progress
        .capture_query_transaction_id(id)
    {
        tx.finish_undetermined(error.clone())?;
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
impl TxExecContext {
    pub(super) fn mark_query_in_flight_for_test(&mut self, transaction_id: &str) {
        let progress = &mut self
            .active_mut()
            .expect("active test transaction")
            .server_progress;
        progress.operation = TxOperationState::InFlight;
        progress.tx_id = Some(transaction_id.to_string());
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
    )
    .expect("valid test request");
    req.concurrent_result_sets = concurrent_result_sets;
    req
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use http::Uri;
    use ydb_grpc::ydb_proto::status_ids::StatusCode;

    use crate::GrpcOptions;
    use crate::client_query::TransactionOptions;
    use crate::errors::{Idempotency, YdbError, YdbOrCustomerError};
    use crate::grpc_connection_manager::GrpcConnectionManager;
    use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
    use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};
    use crate::session_pool::{SessionPool, SessionPoolSettings};

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

    async fn test_transaction_context(lease: SessionPoolLease) -> TxExecContext {
        let manager = test_connection_manager();
        let client = manager
            .get_auth_service_to_node(RawQueryClient::new, lease.node_uri())
            .await
            .expect("create test query client");
        tx_exec_context(
            client,
            lease,
            TransactionOptions::default(),
            None,
            MetricsNames::new(None),
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
    async fn rejected_query_reuses_session_only_after_known_tx_is_rolled_back() {
        for (tx_id, expect_reuse) in [(None, false), (Some("tx-1".to_string()), true)] {
            let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
            let lease = pool.acquire_explicit().await.expect("acquire test session");
            let session_id = lease.session_id().to_string();
            let mut ctx = test_transaction_context(lease).await;
            let progress = &mut ctx
                .active_mut()
                .expect("active transaction")
                .server_progress;
            progress.operation = TxOperationState::InFlight;
            progress.tx_id = tx_id;
            let error = YdbError::YdbStatusError(crate::errors::YdbStatusError::new(
                "transaction rejected",
                StatusCode::Aborted as i32,
                vec![],
            ));

            ctx.fail_attempt(&error)
                .expect("rejected operation must finish the transaction");

            assert!(matches!(ctx.state, TxState::AttemptFailed(_)));
            let acquired = pool
                .acquire_explicit()
                .await
                .expect("a session must become available after failed-attempt cleanup");
            assert_eq!(acquired.session_id() == session_id, expect_reuse);
            acquired.return_to_pool();
        }
    }

    #[tokio::test]
    async fn transient_dispatched_error_cleans_up_possibly_active_transaction() {
        for status in [StatusCode::Unavailable, StatusCode::Overloaded] {
            let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
            let lease = pool.acquire_explicit().await.expect("acquire test session");
            let session_id = lease.session_id().to_string();
            let mut ctx = test_transaction_context(lease).await;
            ctx.mark_query_in_flight_for_test("tx-1");
            let error = YdbError::YdbStatusError(crate::errors::YdbStatusError::new(
                "temporary query failure",
                status as i32,
                vec![],
            ));

            ctx.fail_attempt(&error)
                .expect("temporary failure must finish the local transaction attempt");

            assert!(matches!(ctx.state, TxState::AttemptFailed(_)));
            let reused = pool
                .acquire_explicit()
                .await
                .expect("session must become available after cleanup");
            assert_eq!(reused.session_id(), session_id);
            reused.return_to_pool();
        }
    }

    #[tokio::test]
    async fn in_flight_transaction_cleanup_discards_its_session() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let session_id = {
            let lease = pool.acquire_explicit().await.expect("acquire test session");
            let session_id = lease.session_id().to_string();
            let mut ctx = test_transaction_context(lease).await;
            ctx.active_mut()
                .expect("active transaction")
                .server_progress
                .operation = TxOperationState::InFlight;
            session_id
        };

        let replacement = pool
            .acquire_explicit()
            .await
            .expect("acquire replacement session");
        assert_ne!(replacement.session_id(), session_id);
        replacement.return_to_pool();
    }

    #[tokio::test]
    async fn unhealthy_session_before_query_ends_attempt() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let mut lease = pool.acquire_explicit().await.expect("acquire test session");
        let session_id = lease.session_id().to_string();
        lease.invalidate();
        let mut ctx = test_transaction_context(lease).await;

        let result = tx_begin_stream(
            &mut ctx,
            "SELECT 1".to_string(),
            HashMap::new(),
            CallOptions::default(),
            false,
        )
        .await;

        assert!(result.is_err());
        assert!(matches!(ctx.state, TxState::AttemptFailed(_)));
        let replacement = pool
            .acquire_explicit()
            .await
            .expect("acquire replacement session");
        assert_ne!(replacement.session_id(), session_id);
        replacement.return_to_pool();
    }

    #[tokio::test]
    async fn request_conversion_failure_precedes_query_dispatch() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let session_id = lease.session_id().to_string();
        let mut ctx = test_transaction_context(lease).await;
        let mut params = HashMap::new();
        params.insert(
            "$invalid".to_string(),
            Value::Struct(crate::types::ValueStruct {
                fields_name: vec!["field".to_string()],
                values: Vec::new(),
            }),
        );

        let result = tx_begin_stream(
            &mut ctx,
            "SELECT $invalid".to_string(),
            params,
            CallOptions::default(),
            false,
        )
        .await;

        assert!(result.is_err());
        let progress = &ctx.active().expect("active transaction").server_progress;
        assert_eq!(progress.operation, TxOperationState::Ready);
        assert!(progress.tx_id.is_none());
        assert_eq!(
            ctx.session_lease().expect("active lease").session_id(),
            session_id
        );
    }

    #[tokio::test]
    async fn unhealthy_session_before_explicit_begin_ends_attempt() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let mut lease = pool.acquire_explicit().await.expect("acquire test session");
        let session_id = lease.session_id().to_string();
        lease.invalidate();
        let mut ctx = test_transaction_context(lease).await;

        let result = tx_ensure_begin(&mut ctx).await;

        assert!(result.is_err());
        assert!(matches!(ctx.state, TxState::AttemptFailed(_)));
        let replacement = pool
            .acquire_explicit()
            .await
            .expect("acquire replacement session");
        assert_ne!(replacement.session_id(), session_id);
        replacement.return_to_pool();
    }

    #[tokio::test]
    async fn dispatched_error_without_tx_id_discards_session() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let session_id = lease.session_id().to_string();
        let mut ctx = test_transaction_context(lease).await;
        ctx.active_mut()
            .expect("active transaction")
            .server_progress
            .operation = TxOperationState::InFlight;

        tx_handle_query_error(&mut ctx, &YdbError::Transport("timeout".to_string()))
            .expect("dispatched begin failure must finish the transaction");

        assert!(matches!(ctx.state, TxState::AttemptFailed(_)));
        let replacement = pool
            .acquire_explicit()
            .await
            .expect("acquire replacement session");
        assert_ne!(replacement.session_id(), session_id);
        replacement.return_to_pool();
    }

    #[tokio::test]
    async fn unobserved_transaction_operation_is_undetermined() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let session_id = lease.session_id().to_string();
        let mut ctx = test_transaction_context(lease).await;
        ctx.mark_query_in_flight_for_test("tx-1");

        ctx.resolve_unobserved_operation()
            .expect("unobserved operation must finish the transaction");

        assert!(matches!(ctx.state, TxState::Undetermined(_)));
        let replacement = pool
            .acquire_explicit()
            .await
            .expect("acquire replacement session");
        assert_ne!(replacement.session_id(), session_id);
        replacement.return_to_pool();
    }
}
