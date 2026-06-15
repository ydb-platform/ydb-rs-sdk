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
use crate::grpc_wrapper::raw_query_service::session::AttachedQuerySession;
use crate::grpc_wrapper::raw_query_service::stream::ExecuteQueryStream;
use crate::grpc_wrapper::raw_query_service::transaction_control::{
    begin_tx_control, implicit_tx_control, tx_id_control, RawQueryTxMode,
};
use crate::grpc_wrapper::raw_services::Service;
use crate::types::Value;
use crate::{QuerySessionMode, QueryTransactionOptions, QueryTxMode};

const DEFAULT_RETRY_BUDGET: Duration = Duration::from_secs(5);
const INITIAL_RETRY_BACKOFF_MILLISECONDS: u64 = 1;
const MAX_RETRY_BACKOFF_MILLISECONDS: u64 = 1_000;

#[derive(Clone, Debug, Default)]
pub(crate) struct CallOptions {
    pub timeout: Option<Duration>,
    pub idempotent: Option<bool>,
    pub collect_stats: bool,
    pub session_mode: Option<QuerySessionMode>,
}

#[derive(Clone)]
pub(crate) struct ClientExecContext {
    pub connection_manager: GrpcConnectionManager,
    pub timeouts: TimeoutSettings,
    pub session_mode: QuerySessionMode,
    pub idempotent_operation: bool,
    /// Total wall-clock budget for automatic retries (same idea as [`crate::TableClient::clone_with_retry_timeout`]).
    pub retry_budget: Duration,
}

pub(crate) struct TransactionExecContext {
    pub connection_manager: GrpcConnectionManager,
    pub timeouts: TimeoutSettings,
    pub session_mode: QuerySessionMode,
    pub tx_mode: QueryTxMode,
    pub attached_session: Option<AttachedQuerySession>,
    pub query_node: Option<Uri>,
    pub tx_id: Option<String>,
    pub finished: bool,
}

fn operation_timeout(opts: &CallOptions, defaults: &TimeoutSettings) -> Duration {
    opts.timeout.unwrap_or(defaults.operation_timeout)
}

pub(crate) fn call_operation_timeout(opts: &CallOptions, defaults: &TimeoutSettings) -> Duration {
    operation_timeout(opts, defaults)
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

async fn query_client(ctx: &ClientExecContext) -> YdbResult<RawQueryClient> {
    ctx.connection_manager
        .get_auth_service(RawQueryClient::new)
        .await
}

pub(crate) async fn run_with_retry<T, F, Fut>(
    ctx: &ClientExecContext,
    idempotent: bool,
    attempt_fn: F,
) -> YdbResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = YdbResult<T>>,
{
    retry_with_budget(idempotent, ctx.retry_budget, attempt_fn).await
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

fn session_id_for_mode(mode: QuerySessionMode) -> YdbResult<String> {
    match mode {
        QuerySessionMode::Implicit => Ok(String::new()),
        QuerySessionMode::Pool => Err(YdbError::Custom(
            "query session pool is not implemented yet".to_string(),
        )),
    }
}

fn tx_mode_to_raw(mode: QueryTxMode) -> RawQueryTxMode {
    match mode {
        QueryTxMode::SerializableReadWrite => RawQueryTxMode::SerializableReadWrite,
        QueryTxMode::SnapshotReadOnly => RawQueryTxMode::SnapshotReadOnly,
        QueryTxMode::StaleReadOnly => RawQueryTxMode::StaleReadOnly,
        QueryTxMode::OnlineReadOnly => RawQueryTxMode::OnlineReadOnly,
    }
}

fn tx_control_for_transaction(
    tx: &TransactionExecContext,
) -> YdbResult<Option<ydb_grpc::ydb_proto::query::TransactionControl>> {
    if tx.finished {
        return Err(YdbError::Custom(
            "transaction already finished (committed or rolled back)".to_string(),
        ));
    }
    Ok(Some(match &tx.tx_id {
        Some(id) => tx_id_control(id),
        None => begin_tx_control(tx_mode_to_raw(tx.tx_mode)),
    }))
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

async fn client_implicit_request(
    ctx: &ClientExecContext,
    text: &str,
    params: &HashMap<String, Value>,
    opts: &CallOptions,
) -> YdbResult<(RawQueryClient, RawExecuteQueryRequest)> {
    let session_id = session_id_for_mode(opts.session_mode.unwrap_or(ctx.session_mode))?;
    let client = query_client(ctx).await?;
    let req = RawExecuteQueryRequest::new(
        session_id,
        text,
        params.clone(),
        implicit_tx_control(),
        opts.collect_stats,
    );
    Ok((client, req))
}

async fn client_begin_stream_once(
    ctx: &ClientExecContext,
    text: &str,
    params: &HashMap<String, Value>,
    opts: &CallOptions,
) -> YdbResult<ExecuteQueryStream> {
    let (mut client, req) = client_implicit_request(ctx, text, params, opts).await?;
    let timeout_duration = operation_timeout(opts, &ctx.timeouts);
    let stream = with_operation_timeout(timeout_duration, async {
        client.execute_query(req).await.map_err(Into::into)
    })
    .await?;
    Ok(ExecuteQueryStream::new(stream))
}

pub(crate) async fn client_begin_stream(
    ctx: &ClientExecContext,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
) -> YdbResult<ExecuteQueryStream> {
    let idempotent = opts.idempotent.unwrap_or(ctx.idempotent_operation);
    retry_with_budget(idempotent, ctx.retry_budget, || {
        client_begin_stream_once(ctx, &text, &params, &opts)
    })
    .await
}

/// Interactive transactions need a stable attached session; implicit one-shot queries do not.
async fn ensure_tx_session(tx: &mut TransactionExecContext) -> YdbResult<()> {
    if let Some(session) = &tx.attached_session {
        session.ensure_alive().map_err(YdbError::from)?;
        return Ok(());
    }
    match tx.session_mode {
        QuerySessionMode::Implicit => {
            let uri = tx.connection_manager.endpoint(Service::Query)?;
            let mut client = tx
                .connection_manager
                .get_auth_service_to_node(RawQueryClient::new, &uri)
                .await?;
            tx.attached_session = Some(AttachedQuerySession::open(&mut client).await?);
            tx.query_node = Some(uri);
            Ok(())
        }
        QuerySessionMode::Pool => Err(YdbError::Custom(
            "query session pool is not implemented yet".to_string(),
        )),
    }
}

fn tx_session_id(tx: &TransactionExecContext) -> YdbResult<&str> {
    tx.attached_session
        .as_ref()
        .map(|s| s.session_id())
        .ok_or_else(|| YdbError::Custom("query transaction session is not initialized".to_string()))
}

async fn release_tx_session(tx: &mut TransactionExecContext) {
    let Some(session) = tx.attached_session.take() else {
        return;
    };
    if let Ok(mut client) = query_client_from_tx(tx).await {
        session.close(&mut client).await;
    }
    tx.query_node = None;
}

async fn transaction_execute_request(
    tx: &TransactionExecContext,
    yql_text: String,
    parameters: HashMap<String, Value>,
    opts: &CallOptions,
) -> YdbResult<(RawQueryClient, RawExecuteQueryRequest)> {
    let session_id = tx_session_id(tx)?.to_string();
    let client = query_client_from_tx(tx).await?;
    let req = RawExecuteQueryRequest::new(
        session_id,
        yql_text,
        parameters,
        tx_control_for_transaction(tx)?,
        opts.collect_stats,
    );
    Ok((client, req))
}

pub(crate) async fn transaction_begin_stream(
    tx: &mut TransactionExecContext,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
) -> YdbResult<ExecuteQueryStream> {
    ensure_tx_session(tx).await?;
    let (mut client, req) = transaction_execute_request(tx, text, params, &opts).await?;
    let timeout_duration = operation_timeout(&opts, &tx.timeouts);
    let stream = with_operation_timeout(timeout_duration, async {
        client.execute_query(req).await.map_err(Into::into)
    })
    .await?;
    let mut stream = ExecuteQueryStream::new(stream);
    stream.prime_first_part().await?;
    if let Some(id) = stream.take_captured_tx_id() {
        apply_stream_tx_id(tx, Some(id));
    }
    Ok(stream)
}

pub(crate) async fn transaction_commit(tx: &mut TransactionExecContext) -> YdbResult<()> {
    if tx.tx_id.as_ref().is_none_or(String::is_empty) {
        tx.finished = true;
        release_tx_session(tx).await;
        return Ok(());
    }
    ensure_tx_session(tx).await?;
    let tx_id = tx.tx_id.take().expect("checked Some");
    let session_id = tx_session_id(tx)?.to_string();
    let mut client = query_client_from_tx(tx).await?;
    let timeout_duration = tx.timeouts.operation_timeout;
    let result = with_operation_timeout(timeout_duration, async {
        client
            .commit_transaction(&session_id, &tx_id)
            .await
            .map_err(Into::into)
    })
    .await;
    release_tx_session(tx).await;
    tx.finished = true;
    // Do not retry commit: a transport timeout may mean the commit succeeded server-side.
    result
}

pub(crate) async fn transaction_rollback(tx: &mut TransactionExecContext) -> YdbResult<()> {
    if tx.finished {
        return Ok(());
    }
    if tx.tx_id.as_ref().is_some_and(|id| !id.is_empty()) && tx.attached_session.is_some() {
        let tx_id = tx.tx_id.take().expect("checked Some");
        if let Ok(session_id) = tx_session_id(tx) {
            if let Ok(mut client) = query_client_from_tx(tx).await {
                let timeout_duration = tx.timeouts.operation_timeout;
                let _ = with_operation_timeout(timeout_duration, async {
                    client
                        .rollback_transaction(session_id, &tx_id)
                        .await
                        .map_err(Into::into)
                })
                .await;
            }
        }
    } else {
        tx.tx_id = None;
    }
    release_tx_session(tx).await;
    tx.finished = true;
    Ok(())
}

pub(crate) fn transaction_exec_context(
    connection_manager: GrpcConnectionManager,
    timeouts: TimeoutSettings,
    session_mode: QuerySessionMode,
    options: QueryTransactionOptions,
) -> TransactionExecContext {
    TransactionExecContext {
        connection_manager,
        timeouts,
        session_mode,
        tx_mode: options.mode(),
        attached_session: None,
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
}
