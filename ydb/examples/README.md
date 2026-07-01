# YDB SDK Examples

## Common dependencies for examples
All examples (but listed below) need local ydb started with docker-compose up with docker-compose.yaml from repository root dir.

## Table Service API layers

| Example | Layer | RPC |
|---------|-------|-----|
| `basic-read-rows`, `basic-bulk-upsert` | `TableClient` + `QueryClient` | DDL via `QueryClient::exec`; data via sessionless `ReadRows` / `BulkUpsert` |
| `basic-select-upsert`, `basic-upsert-many-rows`, `container-types` | `QueryClient` | YQL queries and transactions |
| `query-service-*` | `QueryClient` | Query Service (implicit sessions, streaming, tx modes) |

YQL (including DDL) and multi-statement transactions use [`QueryClient`](https://docs.rs/ydb/latest/ydb/struct.QueryClient.html) with default [`TxMode::Implicit`](https://docs.rs/ydb/latest/ydb/enum.TxMode.html). `TableClient` covers typed DDL RPCs (`create_table`, …), describe, copy/rename, and sessionless `ReadRows` / `BulkUpsert`.

## Additional dependencies for some examples
### auth-yc-cmdline
The auth-yc-cmdline.rs example need installed [yc cli](https://cloud.yandex.com/en/docs/cli/operations/install-cli) and active authentication to yandex cloud account.

### auth-ycloud-metadata
The auth-ycloud-metadata.rs example need to be run from Compute Engine in Yandex Cloud with service account - for receive auth token.
