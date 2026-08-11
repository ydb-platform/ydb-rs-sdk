# YDB SDK Examples

Run any example with:

```bash
cargo run --example <name>
```

## Common dependencies for examples
All examples (but listed below) need local ydb started with docker-compose up with docker-compose.yaml from repository root dir.

Examples read `YDB_CONNECTION_STRING` where noted; the rest connect to `grpc://localhost:2136/local`.

## Table Service API layers

| Example | Layer | RPC |
|---------|-------|-----|
| `basic-read-rows`, `basic-bulk-upsert` | `TableClient` + `QueryClient` | DDL via `QueryClient::exec`; data via sessionless `ReadRows` / `BulkUpsert` |
| `basic-select-upsert`, `basic-upsert-many-rows`, `container-types` | `QueryClient` | YQL queries and transactions |
| `query-service-*` | `QueryClient` | Query Service (implicit sessions, streaming, tx modes) |

YQL (including DDL) and multi-statement transactions use [`QueryClient`](https://docs.rs/ydb/latest/ydb/struct.QueryClient.html) with default [`TxMode::Implicit`](https://docs.rs/ydb/latest/ydb/enum.TxMode.html). `TableClient` covers typed DDL RPCs (`create_table`, …), describe, copy/rename, and sessionless `ReadRows` / `BulkUpsert`.

## Query Service

| Example | What it shows |
|---------|---------------|
| `query-service-basic` | One-shot `exec` / `query_row` / `query_result_set`, parameters, `.optional()`, `.typed::<T>()` |
| `query-service-transaction` | `retry_tx` with `AsyncFnMut(&mut Transaction)`; staying generic over `QueryExecutor` |
| `query-service-stream` | Multi-result-set streaming inside `retry_tx` |
| `query-service-tx-modes` | Transaction isolation modes (`TxMode::Implicit` and explicit `BeginTx`) |
| `query-service-script` | Long-running script: start operation, poll until ready, paginate with `FetchScriptResults` |
| `vector-search` | Vector search with YQL Knn UDFs |
| `container-types` | Container values — `List`, `Struct`, `Tuple`, `Optional` |
| `basic-select-upsert` | Select and upsert inside `retry_tx` |
| `basic-upsert-many-rows` | Writing many rows in one request |
| `basic` | Series/seasons/episodes sample schema, bulk load via `AS_TABLE`, snapshot streaming reads — see [basic/README.md](basic/README.md) |

## Table Service

| Example | What it shows |
|---------|---------------|
| `basic-read-rows` | Sessionless `ReadRows` point lookups |
| `basic-bulk-upsert` | Sessionless `BulkUpsert` batch writes |

## Topics

| Example | What it shows |
|---------|---------------|
| `topic-writer` | Producing messages to a topic |
| `topic-reader-retry` | Consuming messages with reconnect/retry handling |
| `topic-read-in-transaction-example` | Reading topic messages inside a transaction |

## Coordination

| Example | What it shows |
|---------|---------------|
| `mutex` | Distributed mutex built on a coordination semaphore |

## Operations

| Example | What it shows |
|---------|---------------|
| `operation-service-example` | Listing, polling, forgetting and cancelling long-running server operations |

## Authentication

| Example | What it shows | Extra requirements |
|---------|---------------|--------------------|
| `auth-token` | Static access token via `AccessTokenCredentials` | — |
| `auth-static-credentials` | User/password via `StaticCredentials` | — |
| `auth-env-connection-string` | Connection string taken from the environment | `YDB_CONNECTION_STRING` |
| `auth-yc-cmdline` | Token produced by an external command | see below |
| `auth-ycloud-metadata` | Token from the VM metadata service | see below |
| `auth-ycloud-serviceaccount` | Service account key file | `YDB_CONNECTION_STRING`, `YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS` |

## Observability

| Example | What it shows |
|---------|---------------|
| `basic-logs` | Enabling verbose `tracing` output |
| `tracing-select-upsert` | Instrumenting queries with `tracing` spans |

## Additional dependencies for some examples
### auth-yc-cmdline
The auth-yc-cmdline.rs example need installed [yc cli](https://cloud.yandex.com/en/docs/cli/operations/install-cli) and active authentication to yandex cloud account.

### auth-ycloud-metadata
The auth-ycloud-metadata.rs example need to be run from Compute Engine in Yandex Cloud with service account - for receive auth token.
