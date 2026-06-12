use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use http::Uri;
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
use crate::result::ResultSet;
use crate::types::Value;
use crate::{QuerySessionMode, QueryTransactionOptions, QueryTxMode};

use super::private::CallOptions;

#[derive(Clone)]
pub(crate) struct ClientExecContext {
    pub connection_manager: GrpcConnectionManager,
    pub timeouts: TimeoutSettings,
    pub session_mode: QuerySessionMode,
    pub idempotent_operation: bool,
    pub retry_timeout: Duration,
    pub max_attempts: usize,
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

async fn with_operation_timeout<T, F>(timeout_duration: Duration, operation: F) -> YdbResult<T>
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

fn raw_sets_to_result_sets(
    sets: Vec<crate::grpc_wrapper::raw_table_service::value::RawResultSet>,
) -> YdbResult<Vec<ResultSet>> {
    sets.into_iter().map(ResultSet::try_from).collect()
}

pub(crate) async fn client_run(
    ctx: &ClientExecContext,
    text: &str,
    params: &HashMap<String, Value>,
    opts: &CallOptions,
) -> YdbResult<Vec<ResultSet>> {
    let idempotent = opts.idempotent.unwrap_or(ctx.idempotent_operation);
    let mut attempt = 0usize;
    loop {
        attempt += 1;
        match client_run_once(ctx, text, params, opts).await {
            Ok(sets) => return Ok(sets),
            Err(err) => {
                if !should_retry_ydb_error(idempotent, &err) || attempt >= ctx.max_attempts {
                    return Err(err);
                }
                sleep(backoff(ctx.retry_timeout, attempt)).await;
            }
        }
    }
}

async fn client_run_once_raw(
    ctx: &ClientExecContext,
    text: &str,
    params: &HashMap<String, Value>,
    opts: &CallOptions,
) -> YdbResult<crate::grpc_wrapper::raw_query_service::execute_query::RawExecuteQueryResult> {
    let session_id = session_id_for_mode(opts.session_mode.unwrap_or(ctx.session_mode))?;
    let mut client = query_client(ctx).await?;
    let req = RawExecuteQueryRequest {
        session_id,
        yql_text: text.to_string(),
        parameters: params.clone(),
        tx_control: implicit_tx_control(),
        collect_stats: opts.collect_stats,
    };
    let timeout_duration = operation_timeout(opts, &ctx.timeouts);
    with_operation_timeout(timeout_duration, async {
        client
            .execute_query_collect(req)
            .await
            .map_err(|e| YdbError::from(e.err))
    })
    .await
}

async fn client_run_once(
    ctx: &ClientExecContext,
    text: &str,
    params: &HashMap<String, Value>,
    opts: &CallOptions,
) -> YdbResult<Vec<ResultSet>> {
    let raw = client_run_once_raw(ctx, text, params, opts).await?;
    raw_sets_to_result_sets(raw.result_sets)
}

async fn client_begin_stream_once(
    ctx: &ClientExecContext,
    text: &str,
    params: &HashMap<String, Value>,
    opts: &CallOptions,
) -> YdbResult<ExecuteQueryStream> {
    let session_id = session_id_for_mode(opts.session_mode.unwrap_or(ctx.session_mode))?;
    let mut client = query_client(ctx).await?;
    let req = RawExecuteQueryRequest {
        session_id,
        yql_text: text.to_string(),
        parameters: params.clone(),
        tx_control: implicit_tx_control(),
        collect_stats: opts.collect_stats,
    };
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
    let mut attempt = 0usize;
    loop {
        attempt += 1;
        match client_begin_stream_once(ctx, &text, &params, &opts).await {
            Ok(stream) => return Ok(stream),
            Err(err) => {
                if !should_retry_ydb_error(idempotent, &err) || attempt >= ctx.max_attempts {
                    return Err(err);
                }
                sleep(backoff(ctx.retry_timeout, attempt)).await;
            }
        }
    }
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

pub(crate) async fn transaction_run(
    tx: &mut TransactionExecContext,
    text: &str,
    params: &HashMap<String, Value>,
    opts: &CallOptions,
) -> YdbResult<Vec<ResultSet>> {
    ensure_tx_session(tx).await?;
    let session_id = tx_session_id(tx)?.to_string();
    let mut client = query_client_from_tx(tx).await?;
    let req = RawExecuteQueryRequest {
        session_id,
        yql_text: text.to_string(),
        parameters: params.clone(),
        tx_control: tx_control_for_transaction(tx)?,
        collect_stats: opts.collect_stats,
    };
    let timeout_duration = operation_timeout(opts, &tx.timeouts);
    let raw = with_operation_timeout(timeout_duration, async {
        match client.execute_query_collect(req).await {
            Ok(raw) => Ok(raw),
            Err(e) => {
                if let Some(id) = e.tx_id.filter(|id| !id.is_empty()) {
                    tx.tx_id = Some(id);
                }
                Err(YdbError::from(e.err))
            }
        }
    })
    .await?;
    if let Some(id) = raw.tx_id {
        tx.tx_id = Some(id);
    }
    raw_sets_to_result_sets(raw.result_sets)
}

pub(crate) async fn transaction_begin_stream(
    tx: &mut TransactionExecContext,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
) -> YdbResult<ExecuteQueryStream> {
    ensure_tx_session(tx).await?;
    let session_id = tx_session_id(tx)?.to_string();
    let mut client = query_client_from_tx(tx).await?;
    let req = RawExecuteQueryRequest {
        session_id,
        yql_text: text,
        parameters: params,
        tx_control: tx_control_for_transaction(tx)?,
        collect_stats: opts.collect_stats,
    };
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
    if tx.tx_id.is_none() {
        tx.tx_id = tx_id.filter(|id| !id.is_empty());
    }
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

pub(crate) fn backoff(max_backoff: Duration, attempt: usize) -> Duration {
    use rand::Rng;

    let exp = Duration::from_millis(10) * 2u32.pow(attempt.min(10) as u32);
    let capped = exp.min(max_backoff);
    let half = capped / 2;
    half + rand::thread_rng().gen_range(Duration::ZERO..=half)
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::errors::{YdbError, YdbOrCustomerError};

    #[test]
    fn retry_helpers_and_backoff() {
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

        let delay = backoff(Duration::from_millis(100), 5);
        assert!(delay > Duration::ZERO);
        assert!(delay <= Duration::from_millis(100));
    }
}
