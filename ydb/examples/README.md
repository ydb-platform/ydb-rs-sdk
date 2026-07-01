# YDB SDK Examples

## Common dependencies for examples
All examples (but listed below) need local ydb started with docker-compose up with docker-compose.yaml from repository root dir.

## Table Service API layers

| Example | Layer | RPC |
|---------|-------|-----|
| `basic-read-rows`, `basic-bulk-upsert` | `TableClient` | sessionless `ReadRows` / `BulkUpsert` + DDL |
| `basic-select-upsert`, `basic-upsert-many-rows`, `container-types` | `QueryClient` | YQL queries and transactions |
| `query-service-*` | `QueryClient` | Query Service (implicit sessions, streaming, tx modes) |

YQL queries and multi-statement transactions use [`QueryClient`](https://docs.rs/ydb/latest/ydb/struct.QueryClient.html). `TableClient` covers DDL, describe, copy, and sessionless `ReadRows` / `BulkUpsert`.

## Additional dependencies for some examples
### auth-yc-cmdline
The auth-yc-cmdline.rs example need installed [yc cli](https://cloud.yandex.com/en/docs/cli/operations/install-cli) and active authentication to yandex cloud account.

### auth-ycloud-metadata
The auth-ycloud-metadata.rs example need to be run from Compute Engine in Yandex Cloud with service account - for receive auth token.
