# SLO workloads (ydb-slo-action)

Docker-based SLO workloads for [ydb-slo-action](https://github.com/ydb-platform/ydb-slo-action) v2.

SLO tests exercise the SDK against a YDB cluster under chaos (node failures, network issues, rolling restarts). The workload must handle transient errors gracefully and export metrics for current vs baseline comparison.

## Layout

```
tests/slo/
  Dockerfile                 # builds the native SLO workload binaries
  slo-framework/             # shared framework (config, metrics, kv workload)
  native/query/              # QueryClient key-value workload (#453)
  native/topic/              # Topic workload
  native/topic-tx/           # Topic + table transaction workload
```

## CI

Workflow `.github/workflows/slo.yml` builds current and baseline Docker images
and runs `ydb-slo-action/init@v2`. It can be started by adding the `SLO` label
to a pull request targeting `master`, or manually through `workflow_dispatch`.
The manual run does not require an issue or pull request number.

Workflow `.github/workflows/slo-report.yml` processes every non-cancelled SLO
run. Successful workload reports are uploaded as HTML artifacts. When an issue
or pull request number is available, the workflow also publishes the report as
a comment. The original SLO run retains raw metrics and logs when a workload
fails before an HTML report can be generated.

## Local run

The workload runs **setup → run → teardown** in one process (create table, prefill, load, drop table).

```bash
cargo build --release -p slo-native-query
# or
cargo build --release -p slo-native-topic
# or
cargo build --release -p slo-native-topic-tx

export YDB_CONNECTION_STRING=grpc://localhost:2136/local
export WORKLOAD_REF=local
export WORKLOAD_NAME=native-query
export WORKLOAD_DURATION=60
# Optional: omit OTEL_EXPORTER_OTLP_ENDPOINT for local runs without metrics export
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:9090/api/v1/otlp

./target/release/slo-native-query \
  --read-rps 100 \
  --write-rps 10 \
  --prefill-count 1000

# Topic workload (set WORKLOAD_NAME=native-topic):
./target/release/slo-native-topic \
  --write-rps 1000
```

Or via cargo:

```bash
YDB_CONNECTION_STRING=grpc://localhost:2136/local \
WORKLOAD_REF=local \
WORKLOAD_NAME=native-query \
WORKLOAD_DURATION=60 \
cargo run --release -p slo-native-query -- --read-rps 100 --write-rps 10

# Topic:
YDB_CONNECTION_STRING=grpc://localhost:2136/local \
WORKLOAD_REF=local \
WORKLOAD_NAME=native-topic \
WORKLOAD_DURATION=60 \
cargo run --release -p slo-native-topic -- --write-rps 1000

# Topic transaction chains:
YDB_CONNECTION_STRING=grpc://localhost:2136/local \
WORKLOAD_REF=local \
WORKLOAD_NAME=native-topic-tx \
WORKLOAD_DURATION=60 \
cargo run --release -p slo-native-topic-tx -- \
  --partition-count 16 \
  --session-pool-size 16
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

### Topic CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--write-rps` | 1000 | Total messages submitted per second |
| `--write-timeout` | 5000 | Message submission and acknowledgement deadline (ms) |
| `--delivery-timeout` | 5000 | Maximum wait for the next message batch (ms) |
| `--commit-timeout` | 5000 | Commit acknowledgement deadline (ms) |
| `--partition-count` | 10 | Topic partitions |
| `--reader-count` | 5 | Topic readers |
| `--writer-count` | 20 | Topic writers |

For the topic workload, `write` is one message submission completed by a server
acknowledgement. A successful `read` is one batch commit completed by a server
acknowledgement; delivery timeouts, SDK read errors, and validation failures are
also failed `read` operations. Successful read latency measures only commit
acknowledgement time. Consequently, write throughput counts messages while read
throughput counts committed batches.

### Topic transaction CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--partition-count` | 16 | Fixed topic partitions; one worker runs per partition |
| `--session-pool-size` | 16 | Shared query session pool limit and warm-up size |
| `--operation-timeout` | 120000 | YDB operation deadline (ms) |

The topic transaction workload creates the configured number of fixed topic
partitions and runs one worker per partition. Each partition starts with one
generation-0 event. A transaction reads one message, UPSERTs its immutable
transition, writes its successor to the same partition, and commits the consumer
offset. Workers share the bounded query session pool; the defaults provide one
query session per partition worker.

After shutdown, every partition must have exactly one live message:

```text
topic end offset - committed consumer offset = 1
```

The transition table must contain exactly one row for every committed input
offset, with contiguous generations. An ambiguous commit is recorded as a
failed transaction operation; the transactional reader reconnects in the
background and continues from the offset exposed by YDB without trying to
classify the previous outcome. The workload does not exercise automatic topic
partitioning or consumer-group rebalancing; the plain topic workload owns that
coverage.

Topic transaction metrics measure one logical chain advance, including any
internal `retry_tx` attempts. Retry overhead is the number of extra attempts per
logical transaction, expressed as a percentage. A confirmed commit is
successful; an operational error or ambiguous commit result is failed. The
report separates ambiguous commits, operational failures, and invalid chain
state.
Atomicity and worker progress remain final pass/fail invariants.

Table path: `{database}/{WORKLOAD_NAME}/{WORKLOAD_REF}` (e.g. `/local/native-query/local`).

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
