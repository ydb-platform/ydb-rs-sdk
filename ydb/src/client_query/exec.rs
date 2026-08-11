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

use super::hooks::QueryTxHook;

const TRANSACTION_ROLLBACK_ON_DROP_TIMEOUT: Duration = Duration::from_secs(5);

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

impl ActiveTransaction {
    /// Resolve a commit or rollback RPC. Only a confirmed success makes the session reusable.
    fn finish_finalization<T>(self, result: YdbResult<T>) -> YdbResult<T> {
        match result {
            Ok(value) => {
                self.lease.return_to_pool();
                Ok(value)
            }
            Err(error) => Err(error),
        }
    }
}

/// Server-side progress within an active transaction.
///
/// In-flight states retain the lease in the transaction so cancellation is conservative: dropping
/// the transaction discards the session instead of issuing a second finalization RPC.
enum ServerTransaction {
    NotStarted,
    BeginInFlight,
    Started(TransactionId),
    QueryInFlight(TransactionId),
    CommitInFlight(TransactionId),
    RollbackInFlight(TransactionId),
}

impl ServerTransaction {
    fn id_for_finalization(&self) -> YdbResult<Option<&TransactionId>> {
        match self {
            Self::NotStarted => Ok(None),
            Self::Started(id) => Ok(Some(id)),
            Self::BeginInFlight
            | Self::QueryInFlight(_)
            | Self::CommitInFlight(_)
            | Self::RollbackInFlight(_) => Err(YdbError::InternalError(
                "query transaction operation is already in progress".to_string(),
            )),
        }
    }

    fn mark_query_dispatched(&mut self) -> YdbResult<()> {
        let previous = std::mem::replace(self, Self::NotStarted);
        match previous {
            Self::NotStarted => {
                *self = Self::BeginInFlight;
                Ok(())
            }
            Self::Started(id) => {
                *self = Self::QueryInFlight(id);
                Ok(())
            }
            in_flight => {
                *self = in_flight;
                Err(YdbError::InternalError(
                    "query transaction operation is already in progress".to_string(),
                ))
            }
        }
    }

    fn observe_query_response(&mut self, incoming: Option<TransactionId>) {
        let previous = std::mem::replace(self, Self::NotStarted);
        *self = match previous {
            Self::NotStarted => incoming.map_or(Self::NotStarted, Self::Started),
            Self::BeginInFlight => incoming.map_or(Self::BeginInFlight, Self::Started),
            Self::Started(existing) | Self::QueryInFlight(existing) => {
                if let Some(incoming) = incoming
                    && incoming != existing
                {
                    tracing::warn!(
                        existing = existing.as_str(),
                        incoming = incoming.as_str(),
                        "query transaction tx_id changed in stream; keeping first value"
                    );
                }
                Self::Started(existing)
            }
            state @ (Self::CommitInFlight(_) | Self::RollbackInFlight(_)) => {
                if let Some(incoming) = incoming {
                    tracing::warn!(
                        incoming = incoming.as_str(),
                        "query transaction received tx_id while finalization was in progress"
                    );
                }
                state
            }
        };
    }
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

    pub(super) fn transaction_id(&self) -> Option<&TransactionId> {
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

    /// Apply a query error to the retained session and transaction state.
    ///
    /// A definitive server status means that the server already ended the transaction, so no
    /// rollback is needed and the lease is resolved immediately. Ambiguous errors leave the
    /// transaction active for the explicit rollback or `Drop` path; session-breaking errors mark
    /// its retained lease as non-reusable.
    pub(super) fn apply_query_error(&mut self, err: &YdbError) {
        let discard_session = err.requires_session_discard();
        if !err.invalidates_server_transaction() {
            if let TxState::Active(active) = &mut self.state
                && discard_session
            {
                active.lease.invalidate();
            }
            return;
        }

        let previous = std::mem::replace(&mut self.state, TxState::Invalidated(err.clone()));
        match previous {
            TxState::Active(active) => {
                if !discard_session {
                    active.lease.return_to_pool();
                }
            }
            terminal => self.state = terminal,
        }
    }

    /// End a transaction after a dispatched operation completed ambiguously.
    pub(super) fn abort_unconfirmed(&mut self, error: YdbError) {
        let previous = std::mem::replace(&mut self.state, TxState::Ambiguous(error));
        match previous {
            TxState::Active(mut active) => {
                active.lease.invalidate();
                schedule_transaction_rollback(self.connection_manager.clone(), active);
            }
            terminal => self.state = terminal,
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

fn interactive_tx_mode(tx: &TransactionExecContext, opts: &CallOptions) -> YdbResult<TxMode> {
    reject_per_call_tx_mode_override(tx, opts)?;
    ensure_interactive_tx_mode(tx.tx_mode)?;
    Ok(tx.tx_mode)
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
    Ok(Some(match &tx.active()?.server {
        ServerTransaction::Started(id) => {
            interactive_tx_mode(tx, opts)?;
            tx_id_control(id, opts.commit_tx)
        }
        ServerTransaction::NotStarted => {
            reject_per_call_tx_mode_override(tx, opts)?;
            ensure_interactive_tx_mode(tx.tx_mode)?;
            begin_tx_control(tx_mode_to_raw(tx.tx_mode)?, opts.commit_tx)
        }
        ServerTransaction::BeginInFlight
        | ServerTransaction::QueryInFlight(_)
        | ServerTransaction::CommitInFlight(_)
        | ServerTransaction::RollbackInFlight(_) => {
            return Err(YdbError::InternalError(
                "query transaction operation is already in progress".to_string(),
            ));
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
    if !opts.commit_tx {
        return Err(YdbError::Custom(
            "one-shot queries with an explicit transaction mode must commit the transaction"
                .to_string(),
        ));
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
        | ServerTransaction::QueryInFlight(_)
        | ServerTransaction::CommitInFlight(_)
        | ServerTransaction::RollbackInFlight(_) => {
            return Err(YdbError::InternalError(
                "query transaction operation is already in progress".to_string(),
            ));
        }
    }
    ensure_interactive_tx_mode(tx.tx_mode)?;
    tx.session_lease()?.ensure_healthy()?;
    let raw_mode = tx_mode_to_raw(tx.tx_mode)?;
    let client = {
        let active = tx.active()?;
        tracing::Span::current().record("ydb.session.id", active.lease.session_id());
        tx.connection_manager
            .get_auth_service_to_node(RawQueryClient::new, active.lease.node_uri())
            .await
    };
    let mut client = match client {
        Ok(client) => client,
        Err(error) => {
            if error.requires_session_discard() {
                tx.active_mut()?.lease.invalidate();
            }
            return Err(error);
        }
    };

    tx.active_mut()?.server = ServerTransaction::BeginInFlight;
    let result =
        maybe_with_operation_timeout(resolve_effective_timeout(tx.retry_deadline, None), async {
            client
                .begin_transaction(tx.session_lease()?.session_id(), raw_mode)
                .await
                .map_err(Into::into)
        })
        .await;

    match result {
        Ok(tx_id) => {
            tx.active_mut()?.server = ServerTransaction::Started(tx_id);
            Ok(())
        }
        Err(err) => {
            if err.invalidates_server_transaction() {
                tx.apply_query_error(&err);
            } else {
                tx.abort_unconfirmed(err.clone());
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
        | ServerTransaction::QueryInFlight(_)
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
    let effective_timeout = resolve_effective_timeout(tx.retry_deadline, opts.timeout);
    let result = maybe_with_operation_timeout(
        effective_timeout,
        open_transaction_stream(tx, text, params, &opts, concurrent_result_sets),
    )
    .await;
    resolve_transaction_stream_open(tx, result)
}

async fn open_transaction_stream(
    tx: &mut TransactionExecContext,
    text: String,
    params: HashMap<String, Value>,
    opts: &CallOptions,
    concurrent_result_sets: bool,
) -> YdbResult<ExecuteQueryStream> {
    tx.session_lease()?.ensure_healthy()?;
    tracing::Span::current().record("ydb.session.id", tx.session_lease()?.session_id());
    if tx.begin {
        transaction_ensure_begin(tx).await?;
    }
    if opts.commit_tx {
        transaction_before_commit(tx).await?;
    }
    let (mut client, req) =
        transaction_execute_request(tx, text, params, opts, concurrent_result_sets).await?;
    let req = req.into_proto().map_err(YdbError::from)?;
    tx.active_mut()?.server.mark_query_dispatched()?;
    let stream = client
        .execute_query_proto(req)
        .await
        .map_err(YdbError::from)?;
    let mut stream = ExecuteQueryStream::new(stream);
    let first_part = stream.prime_first_part().await.map_err(YdbError::from);
    let transaction_id = stream.take_captured_tx_id();
    let server_response_was_classified =
        first_part.is_ok() || matches!(&first_part, Err(YdbError::YdbStatusError(_)));
    if server_response_was_classified && stream.received_part() {
        apply_stream_tx_id(tx, transaction_id);
    }
    first_part?;
    if !stream.is_active() {
        let error = YdbError::InternalError(
            "ExecuteQuery response stream closed before the first part".to_string(),
        );
        let mut active = tx.take_active(TxState::Ambiguous(error.clone()))?;
        active.lease.invalidate();
        return Err(error);
    }
    Ok(stream)
}

fn resolve_transaction_stream_open(
    tx: &mut TransactionExecContext,
    result: YdbResult<ExecuteQueryStream>,
) -> YdbResult<ExecuteQueryStream> {
    if let Err(err) = &result {
        let query_was_dispatched = matches!(
            tx.state,
            TxState::Active(ActiveTransaction {
                server: ServerTransaction::BeginInFlight | ServerTransaction::QueryInFlight(_),
                ..
            })
        );
        if query_was_dispatched && !err.invalidates_server_transaction() {
            tx.abort_unconfirmed(err.clone());
        } else {
            tx.apply_query_error(err);
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
    let transaction_id = tx.active()?.server.id_for_finalization()?.cloned();
    match transaction_id {
        None => {
            tx.take_active(TxState::Committed)?.lease.return_to_pool();
            return Ok(());
        }
        Some(id) => {
            if let Err(err) = tx.active()?.lease.ensure_healthy() {
                let active = tx.take_active(TxState::Ambiguous(err.clone()))?;
                return active.lease.finish(Err(err));
            }
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
    active.finish_finalization(result)
}

#[instrument(name = "ydb.Rollback", skip_all, fields(db.system.name = "ydb", ydb.tx.id = tracing::field::Empty, ydb.session.id = tracing::field::Empty), err)]
pub(crate) async fn transaction_rollback(tx: &mut TransactionExecContext) -> YdbResult<()> {
    if !tx.state.is_active() {
        return Ok(());
    }
    let transaction_id = tx.active()?.server.id_for_finalization()?.cloned();
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
    active.finish_finalization(result)
}

/// Schedule best-effort rollback for a transaction terminated without explicit finalization.
pub(crate) fn schedule_transaction_rollback(
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
        | ServerTransaction::QueryInFlight(_)
        | ServerTransaction::CommitInFlight(_)
        | ServerTransaction::RollbackInFlight(_) => return,
    };

    spawn_pool_release(async move {
        let rollback = async {
            let mut client = connection_manager
                .get_auth_service_to_node(RawQueryClient::new, lease.node_uri())
                .await?;
            client
                .rollback_transaction(lease.session_id(), tx_id.as_str())
                .await
                .map_err(YdbError::from)
        };
        let rollback_ok = timeout(TRANSACTION_ROLLBACK_ON_DROP_TIMEOUT, rollback)
            .await
            .is_ok_and(|result| result.is_ok());
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

pub(crate) fn apply_stream_tx_id(tx: &mut TransactionExecContext, tx_id: Option<TransactionId>) {
    let Ok(active) = tx.active_mut() else {
        if tx_id.is_some() {
            tracing::warn!("query transaction received tx_id after it finished");
        }
        return;
    };
    active.server.observe_query_response(tx_id);
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

    #[test]
    fn one_shot_explicit_transaction_requires_commit() {
        let opts = CallOptions {
            commit_tx: false,
            tx_mode_override: Some(TxMode::SerializableReadWrite),
            ..CallOptions::default()
        };

        assert!(matches!(
            tx_control_for_client(&opts),
            Err(YdbError::Custom(_))
        ));
    }

    #[tokio::test]
    async fn failed_finalization_discards_session_for_ambiguous_status() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let session_id = lease.session_id().to_string();
        let active = ActiveTransaction {
            lease,
            server: ServerTransaction::CommitInFlight(
                TransactionId::from_server("tx-1".into()).expect("non-empty transaction id"),
            ),
        };
        let error = YdbError::YdbStatusError(crate::errors::YdbStatusError {
            message: "commit outcome is unknown".into(),
            operation_status: StatusCode::Unavailable as i32,
            issues: vec![],
        });
        assert!(!error.requires_session_discard());

        let result: YdbResult<()> = active.finish_finalization(Err(error));
        assert!(result.is_err());
        let replacement = pool
            .acquire_explicit()
            .await
            .expect("acquire replacement session");
        assert_ne!(replacement.session_id(), session_id);
        replacement.return_to_pool();
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
        ctx.active_mut().expect("active transaction").server = ServerTransaction::Started(
            TransactionId::from_server("tx-1".into()).expect("non-empty transaction id"),
        );
        ctx.apply_query_error(&YdbError::YdbStatusError(crate::errors::YdbStatusError {
            message: "bad".into(),
            operation_status: StatusCode::GenericError as i32,
            issues: vec![],
        }));
        assert!(!ctx.state.is_active());
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
        ctx.active_mut().expect("active transaction").server = ServerTransaction::BeginInFlight;
        let active = ctx
            .take_active(TxState::RolledBack)
            .expect("take active transaction");

        schedule_transaction_rollback(manager, active);

        let replacement = pool
            .acquire_explicit()
            .await
            .expect("acquire replacement session");
        assert_ne!(replacement.session_id(), session_id);
        replacement.return_to_pool();
    }

    #[tokio::test]
    async fn dispatched_query_on_started_transaction_discards_its_session() {
        let manager = test_connection_manager();
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let session_id = lease.session_id().to_string();
        let mut ctx =
            transaction_exec_context(manager.clone(), lease, TransactionOptions::default(), None);
        let active = ctx.active_mut().expect("active transaction");
        active.server = ServerTransaction::Started(
            TransactionId::from_server("tx-1".into()).expect("non-empty transaction id"),
        );
        active
            .server
            .mark_query_dispatched()
            .expect("started transaction can dispatch a query");
        assert!(matches!(active.server, ServerTransaction::QueryInFlight(_)));
        let active = ctx
            .take_active(TxState::RolledBack)
            .expect("take active transaction");

        schedule_transaction_rollback(manager, active);

        let replacement = pool
            .acquire_explicit()
            .await
            .expect("acquire replacement session");
        assert_ne!(replacement.session_id(), session_id);
        replacement.return_to_pool();
    }

    #[test]
    fn first_query_response_restores_started_transaction() {
        let tx_id = TransactionId::from_server("tx-1".into()).expect("non-empty transaction id");
        let mut state = ServerTransaction::QueryInFlight(tx_id.clone());

        state.observe_query_response(None);

        assert!(matches!(state, ServerTransaction::Started(id) if id == tx_id));
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
        ctx.active_mut().expect("active transaction").server = ServerTransaction::Started(
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
}
