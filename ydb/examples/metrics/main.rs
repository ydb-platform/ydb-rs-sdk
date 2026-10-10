//! Metrics demo: drives every SDK metric series with randomized scenarios and
//! exports them in Prometheus format on `http://0.0.0.0:9090/metrics` (port
//! overridden by `METRICS_PORT`).
//!
//! The example covers the metric series implemented on this branch (client
//! part1, gRPC, session pool, query service). See `README.md` for the series
//! list, the scenario-to-metric map, and known gaps:
//! - `ydb_grpc_stream_messages_total{direction="sent"}` is never emitted (all
//!   stream senders use `clone_sender()`, bypassing the counting wrapper);
//! - `ydb_grpc_connections{state="idle"}` is never emitted (channels are lazy,
//!   so an idle live-socket state cannot be observed).

mod exporter;
mod scenarios;

use std::sync::Arc;

use tokio::signal;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use ydb::{Client, ClientBuilder, YdbError, YdbResult};

/// The client shared by scenario tasks. `RwLock` allows the reconnect scenario
/// to swap in a freshly built client; scenarios clone the inner `Arc` and drop
/// the read guard immediately, so locks are never held across RPC awaits.
pub(crate) type SharedClient = Arc<RwLock<Arc<Client>>>;

/// Build a driver with static metric labels. The SDK binds its metric handles
/// at `build()` time; the global recorder must already be installed (see
/// `exporter::install` — called exactly once before the first `build()`).
pub(crate) async fn build_client(connection_string: &str) -> YdbResult<Client> {
    ClientBuilder::new_from_connection_string(connection_string)?
        .with_driver_name("metrics-example")
        .with_metrics_label("app", "metrics-example")
        .build()
        .await
}

#[tokio::main]
async fn main() -> YdbResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Must run before any `build_client()` call.
    exporter::install()
        .map_err(|err| YdbError::Custom(format!("failed to install metrics exporter: {err}")))?;

    let connection_string = std::env::var("YDB_CONNECTION_STRING")
        .unwrap_or_else(|_| "grpc://localhost:2136/local".to_string());

    let client: SharedClient = Arc::new(RwLock::new(Arc::new(
        build_client(&connection_string).await?,
    )));

    let cancel = CancellationToken::new();
    let mut tasks = vec![
        tokio::spawn(scenarios::query::run(Arc::clone(&client), cancel.clone())),
        tokio::spawn(scenarios::session_pool::run(
            connection_string.clone(),
            cancel.clone(),
        )),
        tokio::spawn(scenarios::topic::run(Arc::clone(&client), cancel.clone())),
        tokio::spawn(scenarios::reconnect::run(
            connection_string,
            Arc::clone(&client),
            cancel.clone(),
        )),
    ];

    tracing::info!("metrics endpoint: http://localhost:9090/metrics (METRICS_PORT overrides)");
    shutdown_signal().await;
    tracing::info!("shutdown signal received, stopping scenarios");

    cancel.cancel();
    while let Some(task) = tasks.pop() {
        if let Err(err) = task.await {
            tracing::warn!(?err, "scenario task failed to complete cleanly");
        }
    }

    // Dropping the last client handle shuts the session pool down:
    // `ydb_session_pool_sessions_closed_total{reason="shutdown"}` is emitted here.
    drop(client);
    Ok(())
}

/// Wait for Ctrl+C (SIGINT) or SIGTERM.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm = match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(sigterm) => sigterm,
            Err(err) => {
                tracing::warn!(?err, "failed to install SIGTERM handler; Ctrl+C only");
                let _ = signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            result = signal::ctrl_c() => {
                if let Err(err) = result {
                    tracing::warn!(?err, "failed to listen for Ctrl+C");
                }
            }
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = signal::ctrl_c().await;
    }
}
