# SLO workloads (ydb-slo-action)

Docker-based SLO workloads for [ydb-slo-action](https://github.com/ydb-platform/ydb-slo-action) v2.

## Layout

```
tests/slo/
  Dockerfile                 # builds slo-native-table binary
  slo-framework/               # shared framework (config, metrics, kv workload)
  native/table/                # TableClient key-value workload (#420)
```

## CI

1. Add the `SLO` label to a pull request targeting `master`.
2. Workflow `.github/workflows/slo.yml` builds current and baseline Docker images and runs `ydb-slo-action/init@v2`.
3. Workflow `.github/workflows/slo-report.yml` publishes the comparison report as a PR comment (same format as ydb-go-sdk).

## Local run

```bash
cargo build --release -p slo-native-table

export YDB_CONNECTION_STRING=grpc://localhost:2136/local
export WORKLOAD_REF=local
export WORKLOAD_NAME=native-table
export WORKLOAD_DURATION=60
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:9090/api/v1/otlp

./target/release/slo-native-table \
  --read-rps 100 \
  --write-rps 10
```

## Legacy example

The `ydb-slo-tests` crate still provides a CLI example (`cargo run --example native ...`) for manual testing. CI SLO uses the Docker workload above.
