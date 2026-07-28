# YDB SDK comparison benchmark

One scenario in. One result out. The same workload, on the same YDB, through
different SDKs.

This benchmark looks for large SDK differences and regressions. It is not a
YDB server benchmark and it does not replace the correctness checks in the SLO
workloads. Topic and Query workloads share one JSON-in/JSON-out contract;
future transaction workloads should keep the same shape.

## Executable contract

Each native benchmark executable:

1. Accepts exactly one scenario JSON path.
2. Reads credentials and the target from `YDB_CONNECTION_STRING`.
3. Writes exactly one result JSON to standard output.
4. Writes diagnostics to standard error.
5. Returns non-zero without a result when setup, workload, drain, or shutdown
   fails.

JSON is the interchange format because Rust and C++ support it directly and a
checked-in scenario records the complete experiment.

## Run the Rust benchmark

From the repository root:

```bash
docker compose up -d
export YDB_CONNECTION_STRING='grpc://localhost:2136/local'

cargo bench --quiet -p ydb --bench sdk_compare -- \
  "$PWD/benchmarks/sdk-compare/scenarios/topic-smoke.json" \
  > target/topic-smoke-rust.json

cargo bench --quiet -p ydb --bench sdk_compare -- \
  "$PWD/benchmarks/sdk-compare/scenarios/query-smoke.json" \
  > target/query-smoke-rust.json
```

Use the corresponding `single-thread` or `multi-thread` scenario for
measurements. The absolute path is intentional: Cargo starts the executable
from the `ydb` package directory.

## Scenario

One file describes one run. Every field is required; missing, unknown, or
invalid fields are rejected.

### Topic workload

```json
{
  "name": "topic-single-thread",
  "execution": {
    "worker_threads": 1,
    "warmup_seconds": 15,
    "measurement_seconds": 60,
    "drain_timeout_seconds": 30
  },
  "workload": {
    "kind": "topic",
    "topic_name": "sdk-compare-topic-single-thread",
    "consumer_name": "sdk-compare-consumer",
    "partition_count": 4,
    "writers_per_partition": 1,
    "reader_count": 4,
    "message_size_bytes": 1024,
    "max_in_flight_per_writer": 100,
    "write_batch_max_messages": 1,
    "write_batch_max_delay_ms": 1,
    "partition_write_speed_bytes_per_second": 52428800
  }
}
```

`worker_threads` controls benchmark-owned executor threads, not threads created
inside an SDK. Single-thread therefore means one application executor thread,
not one process-wide CPU.

The Topic is created directly below the database from the connection string.
It has `partition_count` fixed partitions, one important consumer, RAW payloads,
and the configured per-partition write quota. Fixed partitions hold server
topology constant while the clients are compared. Setup fails if the Topic
already exists.

Each partition has `writers_per_partition` writers pinned to it. Writer `i` for
partition `p` uses producer ID `sdk-compare-writer-{p}-{i}` and normal SDK
sequence numbering. `max_in_flight_per_writer` is enforced independently for
every writer. `write_batch_max_messages` and `write_batch_max_delay_ms`
configure SDK write-request batching. Other transport settings keep their SDK
defaults. Readers subscribe to the entire topic, use normal SDK/server
partition assignment, and commit every delivered SDK batch. Commit
acknowledgements are recorded asynchronously and do not backpressure reading.

## Topic timeline

All SDK sessions open before the benchmark clock starts. One monotonic schedule
then governs every worker:

```text
warm up continuously
        │ warmup_seconds
        ▼
measure continuously
        │ measurement_seconds
        ▼
stop new writes and drain work already started
```

Reader and writer tasks are created once and stay alive across the boundary.
Task startup belongs to warm-up. The pipeline is not emptied or restarted
before measurement.

The payload timestamp determines whether a message began during warm-up or
measurement. Warm-up work that finishes late is ignored. At the measurement
boundary writers stop submitting and readers stop requesting batches. Drain
waits only for write and commit acknowledgements already started; it does not
try to consume every written message. `drain_timeout_seconds` bounds this work
and shutdown. Topic-drop failure is only a warning because measurement has
already completed.

## Topic payload

The payload has an eight-byte header followed by `0xA5` bytes. Integers are
little-endian.

| Bytes | Value |
|---|---|
| `0..8` | nanoseconds from the process monotonic clock (`u64`) |
| `8..message_size_bytes` | `0xA5` |

The buffer is allocated before the header is timestamped immediately before
submission. Readers decode only the timestamp. This benchmark trusts the SLO
workloads for deeper payload verification.

## Topic measurements

| Result key | Boundary |
|---|---|
| `topic.write_ack` | message submission to server write acknowledgement |
| `topic.end_to_end` | message submission to reader application delivery |
| `topic.commit_ack` | measured-batch commit submission to server acknowledgement |

A write enters `topic.write_ack` when its submission starts during measurement,
even if its acknowledgement arrives during drain. Reader delivery occurs when
the SDK returns a batch to application code; `topic.commit_ack` includes a
commit when that batch contains at least one measured message.

Write throughput is `topic.write_ack.count / measurement_seconds`; read
throughput is `topic.end_to_end.count / measurement_seconds`. Byte rates are
message rates multiplied by `message_size_bytes`.

## Query workload

The Query workload executes a table-free query that returns generated rows.
Use it to compare complete Query SDK stacks without depending on table state.

```json
{
  "name": "query-single-thread",
  "execution": {
    "worker_threads": 1,
    "warmup_seconds": 15,
    "measurement_seconds": 60,
    "drain_timeout_seconds": 30
  },
  "workload": {
    "kind": "query",
    "concurrent_requests": 4,
    "row_count": 2500,
    "payload_size_bytes": 1024
  }
}
```

Three checked-in scenarios cover smoke testing and the standard comparison:

| Scenario | Executor threads | Query workers | Pool sessions | Warm-up | Measurement | Drain |
|---|---:|---:|---:|---:|---:|---:|
| `query-smoke` | 1 | 1 | 1 | 1 s | 2 s | 10 s |
| `query-single-thread` | 1 | 4 | 4 | 15 s | 60 s | 30 s |
| `query-multi-thread` | 4 | 4 | 4 | 15 s | 60 s | 30 s |

`worker_threads` controls benchmark-owned executor threads.
`concurrent_requests` controls the number of closed-loop Query workers and the
maximum number of queries in flight. Session-pool capacity and warm-up are
derived from `concurrent_requests`; they are not separate scenario settings.

Workers run continuously across warm-up and measurement. Queries are classified
by when they start: warm-up queries are ignored, while queries started during
measurement are included even if they finish during drain.

The benchmark reports actual decoded rows and payload bytes. It does not
validate returned values, counts, ordering, or payload contents; correctness
belongs to tests, integration tests, and SLO workloads.

Query results contain:

| Result key | Definition |
|---|---|
| `query.execute` | Parameter construction through stream acquisition, complete consumption, typed extraction, and close |
| `queries_per_second` | Completed measured queries divided by `measurement_seconds` |
| `rows_per_second` | Actual decoded measured rows divided by `measurement_seconds` |
| `payload_bytes_per_second` | Actual decoded measured payload bytes divided by `measurement_seconds` |

`query.execute` uses the common latency shape, including `p99_9`. Query p99.9
is informational at low sample counts and must always be interpreted together
with `count`.

Other implementations must consume the same checked-in scenarios and emit the
same result shape. For workload details, use
[query.rs](../../ydb/benches/sdk_compare/query.rs) as the executable reference.

## Result

The result has three top-level fields:

| Field | Content |
|---|---|
| `scenario` | the complete input scenario object |
| `implementation` | `language`, `sdk_version`, and descriptive `build_profile` |
| `metrics` | the workload-specific latency and throughput metrics |

Latency uses microseconds and an HDR Histogram covering 1 microsecond through
300 seconds with three significant digits. Each latency metric exports `count`,
then `min`, `max`, `mean`, `p50`, `p95`, `p99`, and `p99_9` under `latency_us`.
`latency_us` is `null` when the count is zero.

Every latency metric has the same shape:

```json
{
  "count": 120000,
  "latency_us": {
    "min": 410,
    "max": 18200,
    "mean": 930.4,
    "p50": 810,
    "p95": 1410,
    "p99": 2300,
    "p99_9": 7900
  }
}
```

Topic metrics retain the three latency metrics and four throughput rates
described above. Query metrics contain `query.execute`,
`queries_per_second`, `rows_per_second`, and
`payload_bytes_per_second`. The metrics object has no workload or enum tag.

Compare only results with identical scenarios and equivalent test environments.
Build profiles are descriptive metadata: Rust and C++ names need not be equal.
