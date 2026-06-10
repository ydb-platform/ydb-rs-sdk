# System Patterns

## Workspace structure

```
ydb-rs-sdk/
├── ydb/                            # Public SDK (main crate)
│   └── examples/
│       ├── *.rs                    # cargo example targets
│       └── ydb-example-urlshortener/  # full app example
├── ydb-grpc/                       # Generated protobuf + tonic stubs
├── ydb-slo-tests/
└── .agents/                        # Agent workspace (context/, rules/)
```

## Layered architecture (`ydb` crate)

```
ClientBuilder
    └── Client                    # top-level handle, discovery, wait()
            ├── table_client()    # YQL, sessions, transactions
            ├── scheme_client()   # directory listing, describe path
            ├── topic_client()    # readers/writers
            └── coordination_client()

Client
    └── ConnectionPool            # gRPC channels per endpoint
            └── grpc_wrapper/     # raw tonic clients + interceptors
                    └── ydb-grpc  # prost message types
```

## Key modules

| Module | Responsibility |
|--------|----------------|
| `client_builder.rs` | Parse connection string, configure pool, credentials, balancers |
| `connection_pool.rs` | Channel lifecycle, endpoint selection |
| `load_balancer/` | `RandomBalancer`, `StaticBalancer`, `NearestDcBalancer` |
| `session_pool.rs` | YDB session acquisition for table API |
| `client_table.rs` | High-level table client, `retry_transaction` |
| `grpc_wrapper/` | Thin wrappers around tonic services; auth interceptors |
| `errors.rs` | `YdbError`, status code mapping, retry classification |
| `types.rs` | YDB value types, conversions |

## Recurring patterns

### Retry wrapper

Table operations use a retry helper (see `trait_operation.rs`, `client_table.rs`). Retriable gRPC statuses trigger re-execution; idempotent operations are safe to retry.

### `grpc_wrapper` naming

Raw service clients live under `grpc_wrapper/raw_*` (e.g. `raw_table_service`, `raw_scheme_client`). Public clients compose these with pool + interceptors.

### Integration tests

Files like `client_table_test_integration.rs` use `#[ignore]` and `test_integration_helper` to gate on `YDB_CONNECTION_STRING`.

### Builder pattern

`ClientBuilder`, topic reader/writer options, and several config types use `derive_builder`.

## Adding a new API

1. Confirm protobuf support exists in `ydb-grpc` (regenerate if needed).
2. Add `grpc_wrapper/raw_*` client methods.
3. Expose through a `client_*` module with retries and error mapping.
4. Re-export stable types from `lib.rs`.
5. Add unit tests; add `#[ignore]` integration test if server interaction is required.

## Anti-patterns

- Leaking `ydb-grpc` types in the public `ydb` API without a stable wrapper.
- Bypassing the connection pool for production RPC paths.
- Adding dependencies without workspace-level version alignment.
