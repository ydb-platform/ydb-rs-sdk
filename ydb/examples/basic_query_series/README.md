# Basic query example (series)

Query Service counterpart of `ydb-go-sdk/examples/basic/native/query`:
creates `native/query/{series,seasons,episodes}` tables, bulk-loads TV-show sample
data via `AS_TABLE`, and reads series rows with snapshot read-only streaming.

```bash
export YDB_CONNECTION_STRING=grpc://localhost:2136/local
cargo run --example basic_query_series
```

Requires a local YDB (see repository `docker-compose.yaml`) and Rust 1.85+.
