use std::collections::HashMap;
use std::future::Future;
use std::time::{Duration, Instant};

use http::Uri;
use rand::Rng;
use tokio::time::{sleep, timeout};

use crate::client::TimeoutSettings;
use crate::errors::{NeedRetry, YdbError, YdbOrCustomerError, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;
use crate::grpc_wrapper::raw_query_service::execute_query::RawExecuteQueryRequest;
use crate::grpc_wrapper::raw_query_service::stream::ExecuteQueryStream;
use crate::grpc_wrapper::raw_query_service::transaction_control::{
    begin_tx_control, tx_id_control, RawTxMode,
};
use crate::types::Value;
use crate::{TransactionOptions, TxMode};

use crate::session_pool::{spawn_pool_release, SessionPool, SessionPoolLease};

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

const DEFAULT_RETRY_BUDGET: Duration = Duration::from_secs(5);
const INITIAL_RETRY_BACKOFF_MILLISECONDS: u64 = 1;
const MAX_RETRY_BACKOFF_MILLISECONDS: u64 = 1_000;

#[derive(Clone, Debug, Default)]
pub(crate) struct CallOptions {
    pub timeout: Option<Duration>,
    pub retry_budget: Option<Duration>,
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
    pub idempotent_operation: bool,
    pub session_pool: SessionPool,
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
    pub finished: bool,
}

fn operation_timeout(opts: &CallOptions) -> Duration {
    opts.timeout
        .unwrap_or_else(|| TimeoutSettings::default().operation_timeout)
}

pub(crate) fn call_operation_timeout(opts: &CallOptions) -> Duration {
    operation_timeout(opts)
}

pub(crate) fn resolve_retry_budget(opts: &CallOptions) -> Duration {
    opts.retry_budget.unwrap_or(DEFAULT_RETRY_BUDGET)
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

pub(crate) async fn run_with_retry<T, F, Fut>(
    opts: &CallOptions,
    idempotent: bool,
    attempt_fn: F,
) -> YdbResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = YdbResult<T>>,
{
    retry_with_budget(idempotent, resolve_retry_budget(opts), attempt_fn).await
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
    if let Some(override_mode) = opts.tx_mode {
        if override_mode != tx.tx_mode {
            return Err(YdbError::Custom(format!(
                "per-call tx_mode {:?} does not match transaction mode {:?}",
                override_mode, tx.tx_mode
            )));
        }
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
    if tx.finished {
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

async fn retry_with_budget<T, F, Fut>(
    idempotent: bool,
    retry_budget: Duration,
    mut attempt_fn: F,
) -> YdbResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = YdbResult<T>>,
{
    let start = Instant::now();
    let mut attempt = 0usize;
    loop {
        attempt += 1;
        match attempt_fn().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if !should_retry_ydb_error(idempotent, &err) {
                    return Err(err);
                }
                match retry_wait(attempt, start.elapsed(), retry_budget) {
                    Some(wait) if wait > Duration::ZERO => sleep(wait).await,
                    Some(_) => {}
                    None => return Err(err),
                }
            }
        }
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

async fn client_begin_stream_once(
    ctx: &ClientExecContext,
    text: &str,
    params: &HashMap<String, Value>,
    opts: &CallOptions,
    concurrent_result_sets: bool,
) -> YdbResult<ExecuteQueryStream> {
    let timeout_duration = operation_timeout(opts);

    if opts.implicit_session {
        return with_operation_timeout(timeout_duration, async {
            let (mut client, req) =
                client_implicit_session_request(ctx, text, params, opts, concurrent_result_sets)
                    .await?;
            let stream = client.execute_query(req).await.map_err(YdbError::from)?;
            Ok(ExecuteQueryStream::new(stream))
        })
        .await;
    }

    let mut pooled_lease: Option<SessionPoolLease> = None;
    let result: YdbResult<ExecuteQueryStream> = with_operation_timeout(timeout_duration, async {
        let lease = ctx.session_pool.acquire_explicit().await?;
        pooled_lease = Some(lease);
        let lease_ref = pooled_lease
            .as_mut()
            .expect("lease set on successful acquire");
        let (mut client, req) = client_pooled_explicit_request(
            ctx,
            lease_ref,
            text,
            params,
            opts,
            concurrent_result_sets,
        )
        .await?;
        let stream = client.execute_query(req).await.map_err(YdbError::from)?;
        Ok(ExecuteQueryStream::new(stream))
    })
    .await;

    match result {
        Ok(mut stream) => {
            if let Some(lease) = pooled_lease.take() {
                stream = stream.with_session_guard(PooledQuerySessionGuard {
                    lease,
                    rpc_finished: false,
                });
            }
            Ok(stream)
        }
        Err(err) => {
            if let Some(lease) = &mut pooled_lease {
                lease.handle_pool_error(&err);
                lease.end_use();
            }
            Err(err)
        }
    }
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

pub(crate) async fn client_begin_stream(
    ctx: &ClientExecContext,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
    concurrent_result_sets: bool,
) -> YdbResult<ExecuteQueryStream> {
    let idempotent = opts.idempotent.unwrap_or(ctx.idempotent_operation);
    retry_with_budget(idempotent, resolve_retry_budget(&opts), || {
        client_begin_stream_once(ctx, &text, &params, &opts, concurrent_result_sets)
    })
    .await
}

/// Interactive transactions need a stable attached session from the driver pool.
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
    if let Some(err) = err {
        if let Some(lease) = &mut tx.pooled_lease {
            lease.handle_pool_error(err);
        }
    }
    release_tx_session(tx).await;
}

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
pub(crate) async fn transaction_ensure_begin(
    tx: &mut TransactionExecContext,
    session_ready: bool,
) -> YdbResult<()> {
    if tx.finished {
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
    let mut client = query_client_from_tx(tx).await?;
    let timeout_duration = TimeoutSettings::default().operation_timeout;
    let tx_id = with_operation_timeout(timeout_duration, async {
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
    tx.finished = true;
    tx.tx_id = None;
    release_tx_session(tx).await;
}

/// Server ended the transaction after a definitive operation error on a query.
pub(crate) fn transaction_mark_invalidated_on_query_error(
    tx: &mut TransactionExecContext,
    err: &YdbError,
) {
    if err.invalidates_server_transaction() {
        tx.finished = true;
        tx.tx_id = None;
    }
}

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
    if tx.finished {
        return Err(YdbError::Custom(
            "transaction already finished (committed or rolled back)".to_string(),
        ));
    }
    let timeout_duration = operation_timeout(&opts);
    let result: YdbResult<ExecuteQueryStream> = with_operation_timeout(timeout_duration, async {
        ensure_tx_session(tx).await?;
        if let Some(lease) = &mut tx.pooled_lease {
            lease.begin_use();
        }
        if tx.begin {
            transaction_ensure_begin(tx, true).await?;
        }
        let (mut client, req) =
            transaction_execute_request(tx, text, params, &opts, concurrent_result_sets).await?;
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

pub(crate) async fn transaction_commit(tx: &mut TransactionExecContext) -> YdbResult<()> {
    if tx.finished {
        return Ok(());
    }
    if tx.tx_id.as_ref().is_none_or(String::is_empty) {
        tx.finished = true;
        release_tx_session(tx).await;
        return Ok(());
    }
    ensure_tx_session(tx).await?;
    let tx_id = tx.tx_id.take().expect("checked Some");
    let session_id = tx_session_id(tx)?.to_string();
    let mut client = query_client_from_tx(tx).await?;
    let timeout_duration = TimeoutSettings::default().operation_timeout;
    let result = with_operation_timeout(timeout_duration, async {
        client
            .commit_transaction(&session_id, &tx_id)
            .await
            .map_err(Into::into)
    })
    .await;
    release_tx_session_handling_error(tx, result.as_ref().err()).await;
    tx.finished = true;
    // Do not retry commit: a transport timeout may mean the commit succeeded server-side.
    result
}

pub(crate) async fn transaction_rollback(tx: &mut TransactionExecContext) -> YdbResult<()> {
    if tx.finished {
        return Ok(());
    }
    let mut rollback_err: Option<YdbError> = None;
    if tx.tx_id.as_ref().is_some_and(|id| !id.is_empty()) && tx.pooled_lease.is_some() {
        let tx_id = tx.tx_id.take().expect("checked Some");
        if let Ok(session_id) = tx_session_id(tx) {
            if let Ok(mut client) = query_client_from_tx(tx).await {
                let timeout_duration = TimeoutSettings::default().operation_timeout;
                let rollback_result = with_operation_timeout(timeout_duration, async {
                    client
                        .rollback_transaction(session_id, &tx_id)
                        .await
                        .map_err(Into::into)
                })
                .await;
                if let Err(err) = rollback_result {
                    rollback_err = Some(err);
                }
            }
        }
    } else {
        tx.tx_id = None;
    }
    release_tx_session_handling_error(tx, rollback_err.as_ref()).await;
    tx.finished = true;
    match rollback_err {
        Some(err) => Err(err),
        None => Ok(()),
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
) -> TransactionExecContext {
    TransactionExecContext {
        connection_manager,
        session_pool,
        tx_mode: options.mode(),
        begin: options.begin(),
        pooled_lease: None,
        query_node: None,
        tx_id: None,
        finished: false,
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

pub(crate) fn check_retry_transaction_error(err: &YdbOrCustomerError) -> bool {
    match err {
        YdbOrCustomerError::Customer(_) => false,
        YdbOrCustomerError::YDB(err) => !matches!(err.need_retry(), NeedRetry::False),
    }
}

pub(crate) fn should_retry_ydb_error(idempotent: bool, err: &YdbError) -> bool {
    match err.need_retry() {
        NeedRetry::True => true,
        NeedRetry::IdempotentOnly => idempotent,
        NeedRetry::False => false,
    }
}

/// Sleep duration before the next retry attempt, or `None` when the retry budget is exhausted.
pub(crate) fn retry_wait(
    attempt: usize,
    time_from_start: Duration,
    retry_budget: Duration,
) -> Option<Duration> {
    if time_from_start >= retry_budget {
        return None;
    }
    let wait = if attempt > 0 {
        let exp_shift = (attempt - 1).min(63) as u32;
        let base_ms = INITIAL_RETRY_BACKOFF_MILLISECONDS
            .saturating_mul(1u64 << exp_shift)
            .min(MAX_RETRY_BACKOFF_MILLISECONDS);
        let base = Duration::from_millis(base_ms);
        let half = base / 2;
        if half.is_zero() {
            base
        } else {
            half + Duration::from_millis(rand::thread_rng().gen_range(0..=half.as_millis() as u64))
        }
    } else {
        Duration::ZERO
    };
    if time_from_start + wait < retry_budget {
        Some(wait)
    } else {
        None
    }
}

pub(crate) const DEFAULT_QUERY_RETRY_BUDGET: Duration = DEFAULT_RETRY_BUDGET;

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
    use crate::errors::{YdbError, YdbOrCustomerError};

    #[test]
    fn retry_helpers_and_wait() {
        let transport = YdbOrCustomerError::YDB(YdbError::Transport("timeout".into()));
        assert!(check_retry_transaction_error(&transport));
        assert!(should_retry_ydb_error(
            true,
            &YdbError::Transport("timeout".into())
        ));
        assert!(!should_retry_ydb_error(
            false,
            &YdbError::Transport("timeout".into())
        ));
        assert!(!check_retry_transaction_error(
            &YdbOrCustomerError::from_mess("customer")
        ));

        let budget = Duration::from_millis(100);
        let wait1 = retry_wait(1, Duration::ZERO, budget).expect("wait");
        assert!(wait1 > Duration::ZERO);
        let wait2 = retry_wait(2, Duration::ZERO, budget).expect("wait");
        assert!(wait2 >= wait1);
        assert!(retry_wait(10, budget, budget).is_none());
    }

    #[tokio::test]
    async fn transaction_rollback_is_nop_when_finished() {
        use crate::client_query::TransactionOptions;
        use crate::grpc_connection_manager::GrpcConnectionManager;
        use crate::grpc_wrapper::grpc_limits::DEFAULT_GRPC_MESSAGE_SIZE_LIMIT_BYTES;
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
                None,
                DEFAULT_GRPC_MESSAGE_SIZE_LIMIT_BYTES,
            ),
            SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1)),
            TransactionOptions::default(),
        );
        ctx.tx_id = Some("tx-1".into());
        ctx.finished = true;
        transaction_mark_invalidated_on_query_error(
            &mut ctx,
            &YdbError::YdbStatusError(crate::errors::YdbStatusError {
                message: "bad".into(),
                operation_status: StatusCode::GenericError as i32,
                issues: vec![],
            }),
        );
        assert!(ctx.finished);
        assert!(ctx.tx_id.is_none());
        transaction_rollback(&mut ctx).await.expect("rollback nop");
    }
}
