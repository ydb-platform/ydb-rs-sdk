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
2. Reads the target from `YDB_CONNECTION_STRING`.
   The C++ executable uses the YDB SDK's standard credential environment
   variables.
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

## Build the C++ benchmark natively

The supported and tested build path uses Nix for the compiler and third-party
libraries. Docker is used only to run local YDB:

```bash
cd benchmarks/sdk-compare
nix develop .#cpp
just cpp-build
```

Building without Nix is possible, but this benchmark does not maintain or
validate a separate set of system dependency versions. Install `just` and
prepare a working build environment for the YDB C++ SDK by following the
[official C++ SDK installation instructions](https://ydb.tech/docs/en/reference/ydb-sdk/install#cpp),
then run `just cpp-build` from this directory. Compatibility of manually
installed compilers, gRPC, Protobuf, and other C++ dependencies is outside the
benchmark's build contract.

In both environments, the `just` recipe clones an exact public YDB C++ SDK
revision into the ignored `target/` directory, initializes the required
submodules, and applies the checked-in SDK patch before building. The patch is
required for this benchmark; an arbitrary or unpatched SDK checkout is not
supported. The generated checkout is not committed; the pinned revision, patch,
and build recipe are.

`SDK_COMPARE_YDB_SDK_SOURCE` is the low-level CMake input for that prepared SDK
checkout. The `just` recipe sets it automatically. Set it manually only when
invoking CMake directly, and point it at the same pinned and patched checkout.

With direnv enabled, this directory's `.envrc` enters the same shell
automatically.

The build also creates the ignored `benchmarks/sdk-compare/cpp/compile_commands.json`
symlink for clangd. Run a scenario against local YDB with:

```bash
just cpp-run scenarios/topic-smoke.json
```

The result is printed as JSON on standard output. Build and runtime diagnostics
use standard error, so a result can be saved directly:

```bash
just cpp-run scenarios/topic-smoke.json \
  > ../../target/topic-smoke-cpp.json
```

Use `topic-single-thread.json` or `topic-multi-thread.json` for measurements.

## Validate the C++ benchmark

The validation commands build the required preset before running it:

```bash
just cpp-run scenarios/topic-smoke.json
just cpp-asan scenarios/topic-smoke.json
just cpp-asan scenarios/topic-rebalance-smoke.json
```

On a native AMD64 host, also run:

```bash
just cpp-tsan scenarios/topic-smoke.json
```

Run commands accept JSON files from the `scenarios/` directory. Fish completes
these paths normally. If omitted, every run command uses
`scenarios/topic-smoke.json`.

When `YDB_CONNECTION_STRING` is unset, C++ run commands start local YDB and use
anonymous credentials. When it is set, they preserve the connection string and
the SDK credential environment unchanged.

## Scenario

One file describes one run. Every field is required and validated. Rust is the
canonical schema gate for paired runs and rejects unknown fields.

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

`worker_threads` configures the Rust benchmark's Tokio runtime and the C++ SDK's
client thread pool. The C++ benchmark uses the SDK's native `TFuture` coroutine
behavior instead of a separate application executor. SDK network threads remain
SDK-defined in both implementations.

The Topic is created directly below the database from the connection string.
It has `partition_count` fixed partitions, one important consumer, RAW payloads,
and the configured per-partition write quota. Fixed partitions hold server
topology constant while the clients are compared. Setup fails if the Topic
already exists.

The C++ benchmark runs one coroutine task per stable `IWriteSession` and
`IReadSession`. Each task waits asynchronously with `WaitEvent()`, drains ready
events with `GetEvents(false)`, and resumes inline on the thread that fulfills
the awaited SDK future. One shared deadline future stops new work in all tasks
at the measurement boundary. Each task drains its pending acknowledgements and
returns its latency recorders through `NThreading::TFuture`; the reader and
writer managers merge them after every task completes. Session destructors then
perform SDK resource cleanup.
The benchmark does not use the experimental `IProducer`, direct-read or
direct-write paths, a blocking session, custom RPCs, or benchmark-owned worker
threads per session. Reader tasks handle partition lifecycle events explicitly,
preserving normal server partition assignment while exposing commit
acknowledgements for latency measurement.

Each partition has `writers_per_partition` writers pinned to it. Writer `i` for
partition `p` uses producer ID `sdk-compare-writer-{p}-{i}` and normal SDK
sequence numbering. Both implementations enforce `max_in_flight_per_writer`
independently for every writer. C++ withholds the SDK continuation token while
the cap is reached and resumes after an acknowledgement frees a slot.
`write_batch_max_messages` and `write_batch_max_delay_ms` define the shared
writer flush policy. Paired comparisons currently require one message per SDK
batch. Rust can group several ordinary messages into one write request, while
the pinned C++ SDK instead packs them into a server-dependent batch block, so
values above one are not equivalent. With the message limit set to one, the
delay is non-binding. Both implementations use the ordinary topic-service
stream, with direct read and direct write disabled. The benchmark leaves other
SDK memory, retry, routing, and transport settings at their defaults. Readers
subscribe to the entire topic, use normal SDK/server partition assignment, and
commit every delivered SDK batch. Commit acknowledgements are recorded
asynchronously and do not backpressure reading.

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

Reader and writer workers are created once and stay alive across the boundary.
Worker startup belongs to warm-up. The pipeline is not emptied or restarted
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
