# SLO workloads (ydb-slo-action)

Docker-based SLO workloads for [ydb-slo-action](https://github.com/ydb-platform/ydb-slo-action) v2.

SLO tests exercise the SDK against a YDB cluster under chaos (node failures, network issues, rolling restarts). The workload must handle transient errors gracefully and export metrics for current vs baseline comparison.

## Layout

```
tests/slo/
  Dockerfile                 # builds slo-native-table binary
  slo-framework/             # shared framework (config, metrics, kv workload)
  native/table/              # TableClient key-value workload (#420)
```

## CI

1. Add the `SLO` label to a pull request targeting `master`.
2. Workflow `.github/workflows/slo.yml` builds current and baseline Docker images and runs `ydb-slo-action/init@v2`.
3. Workflow `.github/workflows/slo-report.yml` publishes the comparison report as a PR comment (same format as ydb-go-sdk).

## Local run

The workload runs **setup → run → teardown** in one process (create table, prefill, load, drop table).

```bash
cargo build --release -p slo-native-table

export YDB_CONNECTION_STRING=grpc://localhost:2136/local
export WORKLOAD_REF=local
export WORKLOAD_NAME=native-table
export WORKLOAD_DURATION=60
# Optional: omit OTEL_EXPORTER_OTLP_ENDPOINT for local runs without metrics export
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:9090/api/v1/otlp

./target/release/slo-native-table \
  --read-rps 100 \
  --write-rps 10 \
  --prefill-count 1000
```

Or via cargo:

```bash
YDB_CONNECTION_STRING=grpc://localhost:2136/local \
WORKLOAD_REF=local \
WORKLOAD_NAME=native-table \
WORKLOAD_DURATION=60 \
cargo run --release -p slo-native-table -- --read-rps 100 --write-rps 10
```

### CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--read-rps` | 1000 | Read requests per second |
| `--write-rps` | 100 | Write requests per second |
| `--read-timeout` | 10000 | Read timeout (ms) |
| `--write-timeout` | 10000 | Write timeout (ms) |
| `--prefill-count` | 1000 | Rows to insert during setup |
| `--partition-size` | 1 | Auto-partition size (MB) |
| `--min-partition-count` | 6 | Minimum partitions |
| `--max-partition-count` | 1000 | Maximum partitions |

Table path: `{database}/{WORKLOAD_NAME}/{WORKLOAD_REF}` (e.g. `/local/native-table/local`).

## Workload behavior

During **run**, two worker pools operate in parallel:

- **Read workers** — random reads from prefilled row IDs (`0 .. prefill-count`)
- **Write workers** — generate and upsert new rows

Workers continue on transient errors (required under chaos). Metrics are pushed via OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

## Table schema

| Column | Type |
|--------|------|
| `hash` | Uint64 |
| `id` | Uint64 |
| `payload_str` | Text? |
| `payload_double` | Double? |
| `payload_timestamp` | Timestamp? |
| `payload_hash` | Uint64? |

Primary key: `(hash, id)`
