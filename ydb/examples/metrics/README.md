# Metrics example

A long-running emulator of an active YDB client that drives **every metric
series implemented in the SDK** (client counters, gRPC transport, session
pool, query service) and exports them in Prometheus format on
`http://localhost:9090/metrics` (override the port with `METRICS_PORT`).

The stack ships with a Docker Compose file that starts a local YDB,
VictoriaMetrics (scraping the example every 5 s), and Grafana with a
provisioned dashboard — no manual setup needed.

## Quick start

```bash
# from the repository root
docker compose -f ydb/examples/metrics/docker-compose.yaml up -d
cargo run --example metrics

# open the provisioned dashboard (anonymous Viewer access)
open http://localhost:3000/d/ydb-sdk-metrics
```

The compose file includes the repo-root YDB service, so it is self-contained.
If you already have the root `docker compose` stack running (its YDB owns
ports 2135/2136), start only the observability services:

```bash
docker compose -f ydb/examples/metrics/docker-compose.yaml up -d victoriametrics grafana
```

Or bring your own YDB (and compose stack) via `YDB_CONNECTION_STRING`:

```bash
YDB_CONNECTION_STRING=grpc://localhost:2136/local cargo run --example metrics
curl localhost:9090/metrics
```

Shutdown: Ctrl+C or `kill -TERM`. Scenarios stop first, then the client is
dropped, which emits `ydb_session_pool_sessions_closed_total{reason="shutdown"}`.

## How it works

`main.rs` installs the Prometheus recorder (`metrics-exporter-prometheus`)
**before** building the client: the SDK binds all metric handles at
`Client::build()` time, so a recorder installed later would leave every handle
on the no-op recorder. The client is built without
`with_metrics_recorder` (the shortest demo — it binds to the ambient global
recorder) and carries two static labels: `driver_name="metrics-example"`
(via `with_driver_name`) and `app="metrics-example"` (via
`with_metrics_label`).

Duration histograms are exported with custom buckets
(`PrometheusBuilder::set_buckets_for_metric`): the SDK records durations in
**milliseconds** (`*_milliseconds` series) and the part1 row-query-time
histograms in **seconds**, so the exporter defaults (0.005–10 s) would put
every observation in the +Inf bucket. Result sizes (rows/bytes) are u64
counts with their own buckets. Histograms render as
`<name>_bucket/_sum/_count`; the Grafana queries use `histogram_quantile`.

Four independent scenario loops run with randomized 5 s – 3 min intervals:

| Scenario | What it does | Metric series it moves |
|----------|--------------|------------------------|
| `scenarios/query.rs` | Each tick picks one of: parameterized `query_row`, a failing query against a missing table, `retry_tx` commit (upsert + count), `retry_tx` explicit rollback, materialized result set + multi-result-set stream, DDL create/drop, `execute_script` + poll + `fetch_script_results` | Client counters + `ydb_row_query_time_histogram`; `ydb_query_operations_total{operation,result}`, `ydb_query_operation_duration_milliseconds`, `ydb_query_result_rows/bytes`, `ydb_query_errors_total`, `ydb_query_transactions_total`, `ydb_query_transaction_duration_milliseconds`; session pool acquire/use; every RPC in `ydb_grpc_requests_total` |
| `scenarios/session_pool.rs` | Builds a dedicated driver with `with_session_pool(limit=5, warm_up=2, item_usage_limit=10, idle_ttl=30s)` and fires 10 parallel `query_row`s | `ydb_session_pool_sessions{state}`, `size_limit`, `pending_requests`, `acquire_total/milliseconds{result}`, `sessions_created_total`, `sessions_closed_total{reason}` (idle_ttl/usage_limit over time, shutdown at drop), `session_create_milliseconds`, `session_use_milliseconds` |
| `scenarios/topic.rs` | Creates a directory + topic with a consumer, produces 5 messages, consumes them with commit (deadline-bounded), drops everything | `ydb_new_topic_client_counter`, `ydb_grpc_stream_messages_total{direction="received"}` on `StreamRead`, topic/scheme RPCs in `ydb_grpc_requests_total` |
| `scenarios/reconnect.rs` | Builds a fresh driver and swaps it into the shared slot, dropping the old one (no public `stop()` on `Client`) | `ydb_new_client_counter`, `ydb_grpc_connections{state="connecting"\|"active"}`, `ydb_grpc_connection_establish_milliseconds{result="ok"}`, discovery RPCs, `sessions_closed_total{reason="shutdown"}` on the old driver |

The failing-query tick is a useful teaching point: the gRPC RPC succeeds
(`grpc_code="ok"`), so only the query-service series move —
`operations_total{result="error"}` and `errors_total{status_code=...}`.

`ydb_query_transaction_retries_total{status_code}` stays at zero under normal
conditions — nothing retryable happens against a healthy local YDB. Stop the
YDB container mid-run (`docker stop ydb-rs-sdk-ydb-1`) to watch ABORTED /
UNAVAILABLE retries, `errors_total`, and `grpc_errors_total` light up, then
start it again to watch the SDK recover.

## Metric series covered

All series carry the static labels `driver_name="metrics-example"` and
`app="metrics-example"`. Label value sets are closed enums — dashboards must
use these exact strings:

- **Client:** `ydb_new_client_counter`, `ydb_new_table_client_counter`,
  `ydb_new_query_client_counter`, `ydb_new_scheme_client_counter`,
  `ydb_new_topic_client_counter`, `ydb_client_query_row_counter`,
  `ydb_client_transaction_query_row_counter`,
  `ydb_client_transaction_exec_counter`,
  `ydb_client_transaction_commit_counter`,
  `ydb_client_transaction_rollback_counter`,
  `ydb_row_query_time_histogram` (seconds),
  `ydb_transaction_row_query_time_histogram` (seconds).
- **gRPC:** `ydb_grpc_requests_total{endpoint,service,method,grpc_code}`,
  `ydb_grpc_request_duration_milliseconds{endpoint,service,method}`,
  `ydb_grpc_errors_total{endpoint,grpc_code}`,
  `ydb_grpc_stream_messages_total{endpoint,service,method,direction}`,
  `ydb_grpc_connections{endpoint,state}`, `ydb_grpc_connection_establish_milliseconds{endpoint,result=ok|error}`.
- **Session pool:** `ydb_session_pool_sessions{state=idle|active|creating}`,
  `ydb_session_pool_size_limit`, `ydb_session_pool_pending_requests`,
  `ydb_session_pool_acquire_total{result=ok|timeout|error}`,
  `ydb_session_pool_acquire_milliseconds{result}`,
  `ydb_session_pool_session_create_milliseconds`,
  `ydb_session_pool_sessions_created_total`,
  `ydb_session_pool_sessions_closed_total{reason=idle_ttl|usage_limit|bad_session|shutdown|keepalive_failed}`,
  `ydb_session_pool_session_use_milliseconds`,
  `ydb_session_pool_keepalive_total{result=ok|error}`.
- **Query service:** `ydb_query_operations_total{operation=exec|query_row|query_result_set|query|execute_script|fetch_script_results,result=ok|error}`,
  `ydb_query_operation_duration_milliseconds{operation}`,
  `ydb_query_result_rows{operation}`, `ydb_query_result_bytes{operation}`,
  `ydb_query_errors_total{operation,status_code}`,
  `ydb_query_transactions_total{result=commit|rollback|error}`,
  `ydb_query_transaction_duration_milliseconds`,
  `ydb_query_transaction_retries_total{status_code}`.

## Known gaps (by design, fixed in later SDK cycles)

- `ydb_grpc_stream_messages_total{direction="sent"}` is **never emitted**: all
  stream senders (topic reader/writer, coordination) use `clone_sender()`,
  bypassing the counting wrapper. Only `direction="received"` shows data.
- `ydb_grpc_connections{state="idle"}` is **never emitted**: pool channels are
  created with `connect_lazy`, so an idle live-socket state cannot be observed.
  The gauge tracks pool entries, not live sockets, and is not decremented when
  a driver is dropped — the reconnect scenario grows it by design.
- `ydb_session_pool_acquire_total{result="timeout|error"}` needs an
  overloaded or unavailable YDB; see the retry experiment above.

## Files

- `main.rs` — exporter install, client build, scenario scheduler, Ctrl+C/SIGTERM.
- `exporter.rs` — `PrometheusBuilder`: listener, histogram buckets, `install()`.
- `scenarios/` — the four scenario loops.
- `docker-compose.yaml` — includes the repo-root YDB service; adds
  VictoriaMetrics and Grafana (requires Docker Compose >= 2.20 for `include`).
- `victoria/scrape.yml` — scrape job for `host.docker.internal:9090`.
- `grafana/provisioning/` — datasource (`uid: victoria`) and dashboard provider.
- `grafana/dashboards/ydb-sdk-metrics.json` — provisioned dashboard, 4 sections
  (Client, Query Service, Session Pool, gRPC), ~25 panels.
