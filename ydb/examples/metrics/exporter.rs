//! Prometheus exporter setup: global recorder, HTTP listener, histogram buckets.
//!
//! The SDK registers all metric handles at `Client::build()` time, so the global
//! recorder must be installed **before** the first client is built, or every
//! handle binds to the no-op recorder. `install()` panics if a global recorder
//! is already installed — call it exactly once at startup, outside any
//! reconnect loop (the recorder is global and survives client rebuilds).

use std::net::{Ipv4Addr, SocketAddr};

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};

/// Default port of the `/metrics` HTTP endpoint (`METRICS_PORT` overrides).
const DEFAULT_METRICS_PORT: u16 = 9090;

/// Buckets for SDK duration histograms named `*_milliseconds` (values are f64
/// milliseconds). The exporter default buckets (0.005–10 s) would leave every
/// duration observation in the +Inf bucket.
const MILLISECONDS_BUCKETS: &[f64] = &[
    0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
];

/// Buckets for the part1 row-query-time histograms, which record **seconds**
/// (`Duration::as_secs_f64`), unlike the `*_milliseconds` series.
const SECONDS_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Buckets for query result size histograms (`ydb_query_result_rows` counts
/// rows, `ydb_query_result_bytes` counts bytes; both are u64).
const RESULT_SIZE_BUCKETS: &[f64] = &[
    1.0,
    5.0,
    10.0,
    50.0,
    100.0,
    500.0,
    1000.0,
    5000.0,
    10000.0,
    100000.0,
    1_000_000.0,
];

/// Install the global Prometheus recorder with the built-in `/metrics` listener.
///
/// Binds `0.0.0.0` (not `127.0.0.1`) so the VictoriaMetrics container can
/// scrape the endpoint via `host.docker.internal`.
pub(crate) fn install() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::var("METRICS_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(DEFAULT_METRICS_PORT);

    PrometheusBuilder::new()
        .with_http_listener(SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), port))
        // All SDK duration series end in `_milliseconds`; rows/bytes keep
        // their exact names (histograms render as `<name>_bucket/_sum/_count`).
        .set_buckets_for_metric(
            Matcher::Suffix("_milliseconds".to_string()),
            MILLISECONDS_BUCKETS,
        )?
        .set_buckets_for_metric(
            Matcher::Full("ydb_row_query_time_histogram".to_string()),
            SECONDS_BUCKETS,
        )?
        .set_buckets_for_metric(
            Matcher::Full("ydb_transaction_row_query_time_histogram".to_string()),
            SECONDS_BUCKETS,
        )?
        .set_buckets_for_metric(
            Matcher::Full("ydb_query_result_rows".to_string()),
            RESULT_SIZE_BUCKETS,
        )?
        .set_buckets_for_metric(
            Matcher::Full("ydb_query_result_bytes".to_string()),
            RESULT_SIZE_BUCKETS,
        )?
        .install()?;

    Ok(())
}
