# Unreleased

Applications using the default recorder from `metrics-prometheus 0.11.2` must
update their recorder configuration before opening a topic reader or writer.
The five new topic counters use dotted names, which cause this recorder to
panic in debug builds and omit the counters in release builds. Applications
using `metrics-exporter-prometheus` already support these names.
See the [metrics guide](https://docs.rs/ydb/latest/ydb/#metrics) for migration
and the option to keep the old recorder with topic counters disabled.
