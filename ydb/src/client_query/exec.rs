use std::collections::HashMap;
use std::future::Future;
use std::time::{Duration, Instant};

use tokio::time::timeout;
use tracing::instrument;

use crate::errors::{Idempotency, YdbError, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;
use crate::grpc_wrapper::raw_query_service::execute_query::RawExecuteQueryRequest;
use crate::grpc_wrapper::raw_query_service::stream::ExecuteQueryStream;
use crate::grpc_wrapper::raw_query_service::transaction_control::{RawTxMode, begin_tx_control};
use crate::retry_settings::RetrySettings;
use crate::session_pool::{SessionPool, SessionPoolLease};
use crate::traces::helpers::ensure_len_string;
use crate::types::Value;
use crate::{TxMode, closure};

#[derive(Clone, Debug)]
pub(crate) struct CallOptions {
    pub timeout: Option<Duration>,
    pub idempotency: Idempotency,
    pub collect_stats: bool,
    pub commit_tx: bool,
    /// Explicit per-call transaction mode. `None` uses the surrounding context default.
    pub tx_mode_override: Option<TxMode>,
    /// One-shot [`QueryClient`](super::QueryClient) only: send `ExecuteQuery` with an empty `session_id`.
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

/// Per-call timeout capped by the parent [`QueryClient::retry_tx`](super::QueryClient::retry_tx)
/// deadline when set.
pub(crate) fn resolve_effective_timeout(
    deadline: Option<Instant>,
    call_timeout: Option<Duration>,
) -> Option<Duration> {
    let remaining = deadline.and_then(|deadline| deadline.checked_duration_since(Instant::now()));
    match (call_timeout, remaining) {
        (None, None) => None,
        (Some(call), None) => Some(call),
        (None, Some(remaining)) => Some(remaining),
        (Some(call), Some(remaining)) => Some(call.min(remaining)),
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

pub(super) fn tx_mode_to_raw(mode: TxMode) -> YdbResult<RawTxMode> {
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

/// Build `tx_control` for one-shot [`QueryClient`](super::QueryClient) calls.
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
    let mut request = RawExecuteQueryRequest::new(
        "",
        text,
        params.clone(),
        tx_control_for_client(opts)?,
        opts.collect_stats,
    );
    request.concurrent_result_sets = concurrent_result_sets;
    Ok((client, request))
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
        let (mut client, request) =
            client_implicit_session_request(ctx, text, params, opts, concurrent_result_sets)
                .await?;
        let stream = client
            .execute_query(request)
            .await
            .map_err(YdbError::from)?;
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
        let mut request = RawExecuteQueryRequest::new(
            lease.session_id(),
            text,
            params.clone(),
            tx_control,
            opts.collect_stats,
        );
        request.concurrent_result_sets = concurrent_result_sets;
        let stream = client
            .execute_query(request)
            .await
            .map_err(YdbError::from)?;
        Ok(ExecuteQueryStream::new(stream))
    }
    .await;

    match result {
        Ok(stream) => Ok(OpenedClientQueryStream {
            stream,
            session: ClientQuerySession::Pooled(lease),
        }),
        Err(error) => lease.finish(Err(error)),
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

#[cfg(test)]
pub(super) fn build_client_execute_request_for_test(
    opts: &CallOptions,
    concurrent_result_sets: bool,
) -> RawExecuteQueryRequest {
    let mut request = RawExecuteQueryRequest::new(
        String::new(),
        "SELECT 1".to_string(),
        HashMap::new(),
        tx_control_for_client(opts).expect("valid test tx_control"),
        opts.collect_stats,
    );
    request.concurrent_result_sets = concurrent_result_sets;
    request
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::YdbOrCustomerError;

    #[test]
    fn retry_helpers_and_wait() {
        let transport = YdbOrCustomerError::YDB(YdbError::Transport("timeout".into()));
        assert!(transport.is_retriable(Idempotency::Idempotent));
        assert!(YdbError::Transport("timeout".into()).is_retriable(Idempotency::Idempotent));
        assert!(!YdbError::Transport("timeout".into()).is_retriable(Idempotency::NonIdempotent));
        assert!(!YdbOrCustomerError::from_mess("customer").is_retriable(Idempotency::Idempotent));
    }
}
