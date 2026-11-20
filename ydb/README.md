## Rust YDB SDK [![Latest Version](https://img.shields.io/crates/v/ydb.svg)](https://crates.io/crates/ydb)
[Documentation](https://docs.rs/ydb)

Rust SDK for YDB.
Supported rust: 1.88 and newer.


Integration tests, with dependency from real YDB database mark as ignored.
For run it:
1. Set YDB_CONNECTION_STRING env
2. run cargo test -- --include-ignored

### Cargo feature force-exhaustive-all

disable all non_exhaustive marks in public interface for force
check new variants at compile time instead of runtime.

### Exporting topic counters

Topic counters have dotted names in the SDK. For example, `metrics-exporter-prometheus` exports `ydb.topic.writer.sending.messages` as `ydb_topic_writer_sending_messages`, with the driver's `driver_name` label.

If your application calls `metrics_prometheus::try_install()` from `metrics-prometheus 0.11.2`, migrate before upgrading the SDK and using Topics. Its default recorder rejects dotted names when a writer or reader opens. In debug builds this panics. In release builds these counters are silently discarded.

To serve the counters from an existing metrics endpoint, add this application dependency:

```toml
[dependencies]
metrics-exporter-prometheus = { version = "0.18.3", default-features = false }
```

Install the recorder once at startup, before building the YDB client. Set the previous recorder's default histogram buckets to preserve the `_bucket` series used by existing Query dashboards:

```rust
let handle = metrics_exporter_prometheus::PrometheusBuilder::new()
    .set_buckets(&[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0])?
    .install_recorder()?;
```

If you configured custom histogram buckets, carry those boundaries over with `set_buckets_for_metric`.

Use `handle.render()` in the endpoint's scrape handler. This recorder has its own registry. If the endpoint also serves collectors registered directly with `prometheus`, keep serving those collectors. For manual recorder installation, follow the exporter's [upkeep instructions](https://docs.rs/metrics-exporter-prometheus/0.18.3/metrics_exporter_prometheus/#upkeep-and-maintenance).

Leave idle expiration disabled for counters (the default). The SDK retains counter handles for the lifetime of the driver. If the exporter expires a series, subsequent increments through its retained handle will no longer appear in scrapes.
