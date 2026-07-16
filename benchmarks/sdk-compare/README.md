# YDB SDK comparison benchmark

One scenario in. One result out. The same workload, on the same YDB, through
different SDKs.

This benchmark looks for large SDK differences and regressions. It is not a
YDB server benchmark and it does not replace the correctness checks in the SLO
workloads. Topic is implemented first; Query and Topic transactions should keep
the same JSON-in/JSON-out shape.

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
```

Use `topic-single-thread.json` or `topic-multi-thread.json` for measurements.
The absolute path is intentional: Cargo starts the executable from the `ydb`
package directory.

## Scenario

One file describes one run. Every field is required; missing, unknown, or
invalid fields are rejected.

```json
{
  "schema_version": 1,
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
    "writer_count": 4,
    "reader_count": 4,
    "message_size_bytes": 1024,
    "max_in_flight_per_writer": 100,
    "partition_write_speed_bytes_per_second": 52428800
  }
}
```

`schema_version` versions the complete scenario and result protocol.
`worker_threads` controls benchmark-owned executor threads, not threads created
inside an SDK. Single-thread therefore means one application executor thread,
not one process-wide CPU.

The Topic is created directly below the database from the connection string.
It has `partition_count` fixed partitions, one important consumer, RAW payloads,
and the configured per-partition write quota. Fixed partitions hold server
topology constant while the clients are compared. Setup fails if the Topic
already exists.

Writer `i` uses producer ID `sdk-compare-writer-{i}` and normal SDK sequence
numbering and producer-ID routing. `max_in_flight_per_writer` is enforced by the
benchmark; SDK batching and transport defaults remain untouched. Readers use
normal SDK/server partition assignment and commit every delivered SDK batch.

## Timeline

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

## Payload

The payload has an eight-byte header followed by `0xA5` bytes. Integers are
little-endian.

| Bytes | Value |
|---|---|
| `0..8` | nanoseconds from the process monotonic clock (`u64`) |
| `8..message_size_bytes` | `0xA5` |

The buffer is allocated before the header is timestamped immediately before
submission. Readers decode only the timestamp. This benchmark trusts the SLO
workloads for deeper payload verification.

## Measurements

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

Latency uses microseconds and an HDR Histogram covering 1 microsecond through
300 seconds with three significant digits. Each latency metric exports `count`,
then `min`, `max`, `mean`, `p50`, `p95`, `p99`, and `p99_9` under `latency_us`.
`latency_us` is `null` when the count is zero.

## Result

The result has three top-level fields:

| Field | Content |
|---|---|
| `scenario` | the complete input scenario object |
| `implementation` | `language`, `sdk_version`, and descriptive `build_profile` |
| `metrics` | the three latency metrics and four throughput rates |

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

The throughput keys are `write_messages_per_second`,
`write_bytes_per_second`, `read_messages_per_second`, and
`read_bytes_per_second`.

Compare only results with identical scenarios and equivalent test environments.
Build profiles are descriptive metadata: Rust and C++ names need not be equal.
