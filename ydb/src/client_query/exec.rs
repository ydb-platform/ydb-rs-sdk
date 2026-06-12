use std::collections::HashMap;
use std::time::Duration;

use tokio::time::sleep;

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
use crate::result::ResultSet;
use crate::types::Value;
use crate::{QuerySessionMode, QueryTransactionOptions, QueryTxMode};

use super::private::CallOptions;

#[derive(Clone)]
pub(crate) struct ClientExecContext {
    pub connection_manager: GrpcConnectionManager,
    #[allow(dead_code)]
    pub timeouts: TimeoutSettings,
    pub session_mode: QuerySessionMode,
    pub idempotent_operation: bool,
    pub retry_timeout: Duration,
    pub max_attempts: usize,
}

pub(crate) struct TransactionExecContext {
    pub connection_manager: GrpcConnectionManager,
    #[allow(dead_code)]
    pub timeouts: TimeoutSettings,
    pub session_mode: QuerySessionMode,
    pub tx_mode: QueryTxMode,
    pub attached_session: Option<AttachedQuerySession>,
    pub tx_id: Option<String>,
    pub finished: bool,
}

async fn query_client(ctx: &ClientExecContext) -> YdbResult<RawQueryClient> {
    ctx.connection_manager
        .get_auth_service(RawQueryClient::new)
        .await
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
                let need = err.need_retry();
                let retry = match need {
                    NeedRetry::True => true,
                    NeedRetry::IdempotentOnly => idempotent,
                    NeedRetry::False => false,
                };
                if !retry || attempt >= ctx.max_attempts {
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
    client.execute_query_collect(req).await.map_err(Into::into)
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
        match client_run_once_raw(ctx, &text, &params, &opts).await {
            Ok(raw) => return Ok(ExecuteQueryStream::from_buffered(raw.result_sets)),
            Err(err) => {
                let retry = match err.need_retry() {
                    NeedRetry::True => true,
                    NeedRetry::IdempotentOnly => idempotent,
                    NeedRetry::False => false,
                };
                if !retry || attempt >= ctx.max_attempts {
                    return Err(err);
                }
                sleep(backoff(ctx.retry_timeout, attempt)).await;
            }
        }
    }
}

async fn query_client_from_tx(tx: &TransactionExecContext) -> YdbResult<RawQueryClient> {
    tx.connection_manager
        .get_auth_service(RawQueryClient::new)
        .await
}

/// Interactive transactions need a stable attached session; implicit one-shot queries do not.
async fn ensure_tx_session(tx: &mut TransactionExecContext) -> YdbResult<()> {
    if tx.attached_session.is_some() {
        return Ok(());
    }
    match tx.session_mode {
        QuerySessionMode::Implicit => {
            let mut client = query_client_from_tx(tx).await?;
            tx.attached_session = Some(AttachedQuerySession::open(&mut client).await?);
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
    let raw = client.execute_query_collect(req).await?;
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
    let stream = client.execute_query(req).await?;
    Ok(ExecuteQueryStream::new(stream))
}

pub(crate) async fn transaction_commit(tx: &mut TransactionExecContext) -> YdbResult<()> {
    if tx.tx_id.is_none() {
        tx.finished = true;
        release_tx_session(tx).await;
        return Ok(());
    }
    ensure_tx_session(tx).await?;
    let tx_id = tx.tx_id.take().expect("checked Some");
    let session_id = tx_session_id(tx)?.to_string();
    let mut client = query_client_from_tx(tx).await?;
    let result = client.commit_transaction(&session_id, &tx_id).await;
    release_tx_session(tx).await;
    tx.finished = true;
    result.map_err(Into::into)
}

pub(crate) async fn transaction_rollback(tx: &mut TransactionExecContext) -> YdbResult<()> {
    if tx.finished {
        return Ok(());
    }
    if tx.tx_id.is_some() && tx.attached_session.is_some() {
        let tx_id = tx.tx_id.take().expect("checked Some");
        if let Ok(session_id) = tx_session_id(tx) {
            if let Ok(mut client) = query_client_from_tx(tx).await {
                let _ = client.rollback_transaction(session_id, &tx_id).await;
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
        tx_id: None,
        finished: false,
    }
}

pub(crate) fn apply_stream_tx_id(tx: &mut TransactionExecContext, tx_id: Option<String>) {
    if tx.tx_id.is_none() {
        tx.tx_id = tx_id;
    }
}

pub(crate) fn check_retry_error(idempotent: bool, err: &YdbOrCustomerError) -> bool {
    let ydb_err = match err {
        YdbOrCustomerError::Customer(_) => return false,
        YdbOrCustomerError::YDB(err) => err,
    };
    match ydb_err.need_retry() {
        NeedRetry::True => true,
        NeedRetry::IdempotentOnly => idempotent,
        NeedRetry::False => false,
    }
}

pub(crate) fn backoff(retry_timeout: Duration, attempt: usize) -> Duration {
    (Duration::from_millis(10) * 2u32.pow(attempt.min(10) as u32)).min(retry_timeout)
}
