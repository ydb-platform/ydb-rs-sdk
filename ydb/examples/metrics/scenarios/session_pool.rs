//! Session pool burst scenario: many parallel one-shot queries against a
//! deliberately small pool.
//!
//! A dedicated short-lived driver is built with `Client::with_session_pool`
//! (the shared driver keeps its default pool so other scenarios stay
//! unaffected). Ten concurrent `query_row` calls against a pool of five push
//! the acquire path and the pool lifecycle through their paces:
//! - `ydb_session_pool_sessions{state="idle"|"active"|"creating"}` gauges,
//!   `ydb_session_pool_size_limit`, `ydb_session_pool_pending_requests`;
//! - `ydb_session_pool_acquire_total{result}` +
//!   `ydb_session_pool_acquire_milliseconds{result}`;
//! - `ydb_session_pool_sessions_created_total`, the session-create duration
//!   histogram, `ydb_session_pool_session_use_milliseconds`;
//! - with `idle_ttl`/`item_usage_limit` configured, later ticks surface
//!   `ydb_session_pool_sessions_closed_total{reason="idle_ttl"|"usage_limit"}`;
//! - dropping the burst driver at the end of the tick emits
//!   `ydb_session_pool_sessions_closed_total{reason="shutdown"}`.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::warn;
use ydb::{SessionPoolSettings, YdbError, YdbResult};

use crate::build_client;
use crate::scenarios::sleep_jittered;

const BURST_SIZE: usize = 10;
const MIN_INTERVAL: Duration = Duration::from_secs(30);
const MAX_INTERVAL: Duration = Duration::from_secs(60);

pub(crate) async fn run(connection_string: String, cancel: CancellationToken) {
    loop {
        if !sleep_jittered(&cancel, MIN_INTERVAL, MAX_INTERVAL).await {
            return;
        }
        if let Err(err) = burst(&connection_string).await {
            warn!(?err, "session pool burst failed");
        }
    }
}

async fn burst(connection_string: &str) -> YdbResult<()> {
    let client = build_client(connection_string)
        .await?
        .with_session_pool(SessionPoolSettings {
            limit: 5,
            warm_up: 2,
            item_usage_limit: 10,
            idle_ttl: Duration::from_secs(30),
            ..Default::default()
        })
        .await?;
    let client = Arc::new(client);

    let mut tasks = Vec::with_capacity(BURST_SIZE);
    for _ in 0..BURST_SIZE {
        let client = Arc::clone(&client);
        tasks.push(tokio::spawn(async move {
            let mut qc = client.query_client();
            qc.query_row("SELECT 1 AS one").await
        }));
    }

    for task in tasks {
        task.await
            .map_err(|err| YdbError::Custom(format!("burst task join failed: {err}")))??;
    }

    // Dropping the driver shuts its pool down (reason="shutdown").
    drop(client);
    Ok(())
}
