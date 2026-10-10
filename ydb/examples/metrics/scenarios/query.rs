//! Query-service scenarios: one-shot queries, failing queries, retried
//! transactions (commit and rollback), streaming, DDL, and scripts.
//!
//! Each tick picks one scenario at random. The comments map every operation to
//! the metric series it moves:
//! - `ydb_client_query_row_counter`, `ydb_client_transaction_*_counter`,
//!   `ydb_row_query_time_histogram` (seconds), `ydb_transaction_row_query_time_histogram`;
//! - query service: `ydb_query_operations_total{operation,result}`,
//!   `ydb_query_operation_duration_milliseconds{operation}` (ms),
//!   `ydb_query_result_rows` / `ydb_query_result_bytes{operation}` (u64),
//!   `ydb_query_errors_total{operation,status_code}`,
//!   `ydb_query_transactions_total{result}`, `ydb_query_transaction_duration_milliseconds`,
//!   `ydb_query_transaction_retries_total{status_code}`;
//! - session pool on the implicit-session path: `ydb_session_pool_acquire_*`,
//!   `ydb_session_pool_session_use_milliseconds`;
//! - gRPC: every RPC lands in `ydb_grpc_requests_total{endpoint,service,method,grpc_code}`
//!   (+ duration histogram), the query stream adds
//!   `ydb_grpc_stream_messages_total{direction="received"}`.

use std::time::Duration;

use rand::Rng;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use ydb::{Transaction, YdbError, YdbResult, closure};

use crate::SharedClient;
use crate::scenarios::sleep_jittered;

const TABLE: &str = "metrics_example";
const MIN_INTERVAL: Duration = Duration::from_secs(5);
const MAX_INTERVAL: Duration = Duration::from_secs(20);

/// Pick one scenario per tick and run it against the current client.
pub(crate) async fn run(client: SharedClient, cancel: CancellationToken) {
    // One-time DDL so the transactional scenarios have a table to work with.
    if let Err(err) = ensure_table(&client.read().await.clone()).await {
        warn!(
            ?err,
            "table setup failed; DDL/transaction scenarios will fail until it succeeds"
        );
    }

    loop {
        if !sleep_jittered(&cancel, MIN_INTERVAL, MAX_INTERVAL).await {
            return;
        }
        let client = client.read().await.clone();
        let tick = rand::thread_rng().gen_range(0..7);
        let result = match tick {
            0 => query_row_tick(&client).await,
            1 => failing_query_tick(&client).await,
            2 => retry_tx_commit_tick(&client).await,
            3 => retry_tx_rollback_tick(&client).await,
            4 => stream_tick(&client).await,
            5 => exec_ddl_tick(&client).await,
            _ => script_tick(&client).await,
        };
        if let Err(err) = result {
            warn!(?err, "query scenario tick failed");
        }
    }
}

async fn ensure_table(client: &ydb::Client) -> YdbResult<()> {
    let mut qc = client.query_client();
    qc.exec(format!(
        "CREATE TABLE IF NOT EXISTS {TABLE} (id Uint64 NOT NULL, val Utf8, PRIMARY KEY(id))"
    ))
    .await?;
    Ok(())
}

/// One-shot parameterized `query_row`.
///
/// Moves: `ydb_client_query_row_counter`,
/// `ydb_query_operations_total{operation="query_row",result="ok"}`, the
/// operation duration/rows/bytes histograms, `ydb_row_query_time_histogram`,
/// session pool acquire/use series.
async fn query_row_tick(client: &ydb::Client) -> YdbResult<()> {
    let mut qc = client.query_client();
    let mut row = qc
        .query_row("SELECT $value AS value")
        .param("$value", 42_u64)
        .await?;
    let _value: Option<u64> = row.remove_field_by_name("value")?.try_into()?;
    Ok(())
}

/// A syntactically valid query against a missing table.
///
/// Teaching point: the gRPC RPC itself succeeds (`grpc_code="ok"` in
/// `ydb_grpc_requests_total`) — the failure is application-level, so the
/// query-service series move instead:
/// `ydb_query_operations_total{operation="query_row",result="error"}` and
/// `ydb_query_errors_total{operation="query_row",status_code=...}`.
async fn failing_query_tick(client: &ydb::Client) -> YdbResult<()> {
    let mut qc = client.query_client();
    match qc
        .query_row("SELECT * FROM no_such_table_for_metrics_demo")
        .await
    {
        Ok(_) => {
            warn!("missing-table query unexpectedly succeeded");
            Ok(())
        }
        Err(err) => {
            tracing::debug!(?err, "expected query failure observed");
            Ok(())
        }
    }
}

/// `retry_tx` that upserts and reads back: Ok from the callback commits.
///
/// Moves: `ydb_client_transaction_exec_counter`,
/// `ydb_client_transaction_query_row_counter`,
/// `ydb_client_transaction_commit_counter`,
/// `ydb_query_transactions_total{result="commit"}`,
/// `ydb_query_transaction_duration_milliseconds`,
/// `ydb_transaction_row_query_time_histogram`.
async fn retry_tx_commit_tick(client: &ydb::Client) -> YdbResult<()> {
    let qc = client.query_client();
    let count: u64 = qc
        .retry_tx(closure!(async |tx: &mut Transaction| {
            tx.exec(format!(
                "UPSERT INTO {TABLE} (id, val) VALUES (1, \"committed\")"
            ))
            .await?;
            let mut row = tx
                .query_row(format!("SELECT COUNT(*) AS cnt FROM {TABLE}"))
                .await?;
            let cnt: Option<u64> = row.remove_field_by_name("cnt")?.try_into()?;
            Ok(cnt.unwrap_or(0))
        }))
        .await?;
    tracing::debug!(count, "retry_tx committed");
    Ok(())
}

/// `retry_tx` that rolls the transaction back explicitly.
///
/// Moves: `ydb_query_transactions_total{result="rollback"}` (or `"error"` if
/// the rollback itself fails). `ydb_query_transaction_retries_total{status_code}`
/// stays zero here — nothing retryable happens; to see retries, stop the local
/// YDB mid-run so the SDK observes ABORTED/UNAVAILABLE statuses.
async fn retry_tx_rollback_tick(client: &ydb::Client) -> YdbResult<()> {
    let qc = client.query_client();
    qc.retry_tx(closure!(async |tx: &mut Transaction| {
        tx.exec(format!("DELETE FROM {TABLE} WHERE id = 987654321"))
            .await?;
        // Explicit rollback: no commit, no retry, no error.
        tx.rollback().await?;
        Ok(())
    }))
    .await?;
    Ok(())
}

/// Materialized multi-row result set plus a multi-result-set stream.
///
/// Moves: `ydb_query_operations_total{operation="query_result_set"|"query"}`,
/// accumulated `ydb_query_result_rows` / `ydb_query_result_bytes`, and
/// `ydb_grpc_stream_messages_total{direction="received"}` on the query-stream
/// RPC (each received part is one stream message).
async fn stream_tick(client: &ydb::Client) -> YdbResult<()> {
    let mut qc = client.query_client();

    let result_set = qc
        .query_result_set(format!("SELECT id, val FROM {TABLE}"))
        .await?;
    let mut rows = 0_u64;
    for _row in result_set {
        rows += 1;
    }
    tracing::debug!(rows, "materialized result set drained");

    let mut stream = qc.query("SELECT 1 AS a; SELECT 2 AS b, 3 AS c;").await?;
    let mut sets = 0_u64;
    while let Some(result_set) = stream.next_result_set().await? {
        sets += 1;
        for _row in result_set {
            rows += 1;
        }
    }
    stream.close().await?;
    tracing::debug!(rows, sets, "stream drained");
    Ok(())
}

/// DDL round trip through `exec`.
///
/// Moves: `ydb_query_operations_total{operation="exec",result="ok"}` plus the
/// operation duration histogram.
async fn exec_ddl_tick(client: &ydb::Client) -> YdbResult<()> {
    let mut qc = client.query_client();
    qc.exec(format!(
        "CREATE TABLE IF NOT EXISTS {TABLE}_ddl (id Uint64 NOT NULL, PRIMARY KEY(id))"
    ))
    .await?;
    qc.exec(format!("DROP TABLE {TABLE}_ddl")).await?;
    Ok(())
}

/// Long-running script: start, poll until ready, paginate results.
///
/// Moves: `ydb_query_operations_total{operation="execute_script"}` and
/// `{operation="fetch_script_results"}` with their duration/rows/bytes
/// histograms.
async fn script_tick(client: &ydb::Client) -> YdbResult<()> {
    use std::time::Instant;

    use tokio::time::sleep;

    let mut qc = client.query_client();
    let op_client = client.operation_client();

    qc.exec(format!("DELETE FROM {TABLE}")).await?;
    qc.exec(format!(
        "UPSERT INTO {TABLE} (id, val) VALUES (7, \"scripted\")"
    ))
    .await?;

    let op = qc
        .execute_script(format!("SELECT id, val FROM {TABLE} WHERE id = $id"))
        .param("$id", 7_u64)
        .results_ttl(Duration::from_secs(3600))
        .await?;

    let poll_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if Instant::now() >= poll_deadline {
            return Err(YdbError::Custom(
                "script operation polling timed out".into(),
            ));
        }
        let status = op_client.get_operation(&op.id).await?;
        if status.ready {
            break;
        }
        sleep(Duration::from_millis(200)).await;
    }

    let mut next_token = String::new();
    loop {
        let page = qc
            .fetch_script_results(&op.id)
            .result_set_index(0)
            .rows_limit(100)
            .fetch_token(&next_token)
            .await?;
        next_token = page.next_fetch_token;
        for mut row in page.result_set {
            let id: Option<u64> = row.remove_field_by_name("id")?.try_into()?;
            tracing::debug!(?id, "script row");
        }
        if next_token.is_empty() {
            break;
        }
    }

    op_client.forget_operation(&op.id).await?;
    Ok(())
}
