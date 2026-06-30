# YDB SDK Examples

## Common dependencies for examples
All examples (but listed below) need local ydb started with docker-compose up with docker-compose.yaml from repository root dir.

## Table Service API layers

| Example | Layer | RPC |
|---------|-------|-----|
| `basic-read-rows`, `basic-bulk-upsert` | `TableClient` | sessionless `ReadRows` / `BulkUpsert` |
| `explain-query-example` | `TableClient` | `ExplainDataQuery`, scheme queries |
| `table-session-stream-read` | `Session` via [`TableClient::retry`] | `StreamReadTable` |
| `table-session-scan-query` | `Session` via [`TableClient::retry`] | `StreamExecuteScanQuery` |
| `table-tx-modes` | `TableClient` + `Transaction` | autocommit `ExecuteDataQuery` per `Mode` |

Session-only RPCs use [`TableClient::retry`](TableClient::retry) (go-sdk: `table.Client.Do`): a session is leased from the pool for the callback and returned (or discarded) when the future completes.

## Additional dependencies for some examples
### auth-yc-cmdline
The auth-yc-cmdline.rs example need installed [yc cli](https://cloud.yandex.com/en/docs/cli/operations/install-cli) and active authentication to yandex cloud account.

### auth-ycloud-metadata
The auth-ycloud-metadata.rs example need to be run from Compute Engine in Yandex Cloud with service account - for receive auth token.
