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
cargo run --features metrics --example metrics

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
YDB_CONNECTION_STRING=grpc://localhost:2136/local cargo run --features metrics --example metrics
curl localhost:9090/metrics
```

Shutdown: Ctrl+C or `kill -TERM`. Scenarios stop first, then the client is
dropped, which emits `ydb_session_pool_sessions_closed_total{reason="shutdown"}`.

## How it works

`main.rs` installs the Prometheus recorder (`metrics-exporter-prometheus`)
**before** building the client: the SDK binds all metric handles at
`Client::build()` time, so a recorder installed later would leave every handle
on the no-op recorder. The client is built without
`with_metrics_recorder` (the shortest demo — with the SDK's `metrics` feature
it binds to the ambient global recorder). Shared and reconnect drivers carry
`driver_name="metrics-example"` (via `with_driver_name`) and
`app="metrics-example"` (via `with_metrics_label`). The dedicated session-pool
burst driver uses `driver_name="metrics-example-burst"` and the same `app`
label. The example opts into `MetricsDetail::Endpoints` to demonstrate
per-endpoint gRPC series. SDK clients use aggregated `MetricsDetail::Basic`
series without an `endpoint` label by default. Endpoint detail retains at most
64 distinct addresses per client by default (`with_metrics_max_endpoints`
overrides this); further addresses share `endpoint="__other__"` without being
retained in SDK caches or the interner. The limit does not remove series
already retained by an external backend.

Duration histograms are exported with custom buckets
(`PrometheusBuilder::set_buckets_for_metric`): all SDK durations are recorded
in **seconds**. New series use the `_seconds` suffix; the two existing
row-query-time histograms keep their names. The shared buckets span
0.0005–60 s. Result rows are exact counts; result bytes are approximate u64
counts. Both use their own buckets. Histograms render as
`<name>_bucket/_sum/_count`; the Grafana queries use `histogram_quantile`.

Four independent scenario loops run with randomized 5 s – 3 min intervals:

| Scenario | What it does | Metric series it moves |
|----------|--------------|------------------------|
| `scenarios/query.rs` | Each tick picks one of: parameterized `query_row`, a failing query against a missing table, `retry_tx` commit (upsert + count), `retry_tx` explicit rollback, materialized result set + multi-result-set stream, DDL create/drop, `execute_script` + poll + `fetch_script_results`, `explain`, a named `query_row` (`.with_name(...)`), a named failing query | Client counters + `ydb_row_query_time_histogram`; `ydb_query_operations_total{operation,result}`, `ydb_query_operation_duration_seconds`, `ydb_query_result_rows/bytes`, `ydb_query_errors_total`, `ydb_query_transactions_total`, `ydb_query_transaction_duration_seconds`; named variants `ydb_query_named_*{query_name}`; session pool acquire/use; every RPC in `ydb_grpc_requests_total` |
| `scenarios/session_pool.rs` | Builds a dedicated `metrics-example-burst` driver with `with_session_pool(limit=5, warm_up=2, item_usage_limit=10, idle_ttl=30s)` and fires 10 parallel `query_row`s | `ydb_session_pool_sessions{state}`, `size_limit`, `pending_requests`, `acquire_total/seconds{result}`, `sessions_created_total`, `sessions_closed_total{reason}` (idle_ttl/usage_limit over time, shutdown at drop), `session_create_seconds`, `session_use_seconds` |
| `scenarios/topic.rs` | Creates a directory + topic with a consumer, produces 5 messages, consumes them with commit (deadline-bounded), drops everything | `ydb_new_topic_client_counter`, `ydb_grpc_stream_messages_total{direction="sent"\|"received"}` on `StreamRead`/`StreamWrite`, topic/scheme RPCs in `ydb_grpc_requests_total` |
| `scenarios/reconnect.rs` | Builds a fresh driver and swaps it into the shared slot, dropping the old one (no public `stop()` on `Client`) | `ydb_new_client_counter`, `ydb_grpc_connections{state="connecting"\|"active"}`, `ydb_grpc_connection_establish_seconds{result="ok"}`, discovery RPCs, `sessions_closed_total{reason="shutdown"}` on the old driver |

The failing-query tick is a useful teaching point: the gRPC RPC succeeds
(`grpc_code="ok"`), so only the query-service series move —
`operations_total{result="error"}` and `errors_total{status_code=...}`.

`ydb_query_transaction_retries_total{status_code}` stays at zero under normal
conditions — nothing retryable happens against a healthy local YDB. Stop the
YDB container mid-run (`docker compose -f ydb/examples/metrics/docker-compose.yaml stop ydb`)
to watch ABORTED / UNAVAILABLE retries, `errors_total`, and `grpc_errors_total`
light up, then start it again to watch the SDK recover.

## Metric series covered

All series carry the static label `app="metrics-example"`. Their `driver_name`
is `metrics-example` or `metrics-example-burst`, as described above. Label value
sets are otherwise closed enums — dashboards must use these exact strings:

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
  `ydb_grpc_request_duration_seconds{endpoint,service,method}`,
  `ydb_grpc_errors_total{endpoint,grpc_code}`,
  `ydb_grpc_stream_messages_total{endpoint,service,method,direction}`,
  `ydb_grpc_connections{endpoint,state}`, `ydb_grpc_connection_establish_seconds{endpoint,result=ok|error}`.
- **Session pool:** `ydb_session_pool_sessions{state=idle|active|creating}`,
  `ydb_session_pool_size_limit`, `ydb_session_pool_pending_requests`,
  `ydb_session_pool_acquire_total{result=ok|timeout|error|cancelled}`,
  `ydb_session_pool_acquire_seconds{result}`,
  `ydb_session_pool_session_create_seconds`,
  `ydb_session_pool_sessions_created_total`,
  `ydb_session_pool_sessions_closed_total{reason=idle_ttl|usage_limit|max_age|pool_full|bad_session|shutdown|keepalive_failed}`,
  `ydb_session_pool_session_use_seconds`,
  `ydb_session_pool_attach_stream_closed_total{reason=session_shutdown|node_shutdown|stream_closed|error}`.
  Pool gauges are additive across pools sharing a recorder; dropping a pool
  subtracts only its own contribution. `cancelled` means the acquire future was
  dropped before completion. Aborting an attach listener during ordinary session
  cleanup does not emit an attach-stream closure.
- **Query service:** `ydb_query_operations_total{operation=exec|query_row|query_result_set|query|execute_script|fetch_script_results|explain,result=ok|error}`,
  `ydb_query_operation_duration_seconds{operation}`,
  `ydb_query_result_rows{operation}`, `ydb_query_result_bytes{operation}` (not emitted for explain),
  `ydb_query_errors_total{operation,status_code}` (`CLIENT_ERROR` for client-side shape/conversion failures),
  `ydb_query_transactions_total{result=commit|rollback|error}`,
  `ydb_query_transaction_duration_seconds`,
  `ydb_query_transaction_retries_total{status_code}` (only when the next attempt starts).
  Stream outcomes, durations, and sizes are recorded once at close, failure, or
  drop; dropping an unclosed stream produces `CANCELLED`.
- **Named queries** (only for calls carrying `.with_name("...")`, mirrored from
  the common series): `ydb_query_named_operations_total{query_name,result}`,
  `ydb_query_named_operation_duration_seconds{query_name}`,
  `ydb_query_named_errors_total{query_name,status_code}`,
  `ydb_query_named_result_rows{query_name}`,
  `ydb_query_named_result_bytes{query_name}`. Use constants or a small, bounded
  set of generated names (for example, one per request handler), matching
  `[a-z0-9_]{1,64}`. Static strings and owned `String` values are both accepted.
  Never use query text, parameters, identifiers, or other unbounded runtime
  data. The default metrics adapter logs one warning per malformed name and
  records it under `query_name="__invalid__"` without interrupting the query.

## Known limitations

- `ydb_grpc_errors_total` includes `grpc_code="cancelled"` when long-lived streams
  are dropped during expected reconnects or shutdown. Exclude that code from
  alerts if such reconnects are normal for your workload.
- `ydb_grpc_connections{state="idle"}` is **never emitted**: pool channels are
  created with `connect_lazy`, so an idle live-socket state cannot be observed.
  The gauge tracks pool entries, not live sockets. Each initialized entry adds
  one to `state="active"`; dropping its pool subtracts that contribution.
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
  (Client, Query Service, Session Pool, gRPC), ~29 panels.
