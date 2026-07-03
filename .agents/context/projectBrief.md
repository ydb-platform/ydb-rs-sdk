# Project Brief

## What this is

**ydb-rs-sdk** is the official Rust SDK for [YDB](https://ydb.tech/) — a distributed SQL database. The repository is a Cargo workspace publishing the `ydb` crate (and supporting crates) to [crates.io](https://crates.io/crates/ydb).

## Goals

- Provide an idiomatic, async Rust client for YDB table, query, scheme, topic, coordination, and discovery APIs.
- Handle production concerns: connection pooling, load balancing, retries, credentials, TLS.
- Stay compatible with YDB server protobuf/gRPC contracts via the `ydb-grpc` crate.
- Maintain semver for published crates; MSRV **1.85**.

## Non-goals

- `ydb-grpc` is an internal building block — not a public-facing API for application developers.
- **SQL/YQL execution via the table client** — use `QueryClient` instead (see `productContext.md`).
- This repo does not host the YDB server, documentation site, or non-Rust SDKs.

## Key constraints

- **Async runtime**: Tokio-based; public APIs are `async`.
- **Generated code**: protobuf types live in `ydb-grpc`; regenerating protos is a separate maintenance task.
- **Integration tests**: require a running YDB instance; marked `#[ignore]` in unit test runs.
- **CI parity**: changes must pass `cargo fmt --check` and `cargo clippy` (see `techContext.md`).

## Success criteria for agent work

- Public API changes are intentional, documented, and semver-aware.
- New code follows existing module boundaries and retry/pool patterns.
- `.agents/context/` reflects the current state after each significant change.
