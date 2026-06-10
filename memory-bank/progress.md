# Progress

> **Volatile file** — append/update as work completes.

## What works (baseline)

- **Table API**: queries, transactions with automatic retry, session pool, bulk upsert.
- **Scheme API**: directory listing, path operations (evolving — see open PRs).
- **Topics**: reader/writer with partitioning and offset management.
- **Coordination**: distributed semaphores (integration-tested).
- **Discovery**: endpoint discovery with load balancing strategies.
- **Auth**: static tokens, access tokens, JWT/metadata credentials.
- **TLS**: custom CA support, rustls via tonic.

## CI status

- Lint: `cargo fmt --check` + `cargo clippy` on Rust 1.91.0.
- Tests: full workspace tests with `--include-ignored` against `ydbplatform/local-ydb:nightly` on Rust 1.82 and 1.91.0.

## Known issues / gaps

- Check GitHub Issues for active bugs and feature requests.
- Cross-SDK parity with Go/Java SDKs is tracked issue-by-issue.
- `ydb-grpc-helpers` is commented out of the workspace — status unclear for new contributors.

## Milestones

| Date | Milestone |
|------|-----------|
| 2026-06 | Memory bank for AI agents ([#428](https://github.com/ydb-platform/ydb-rs-sdk/issues/428)) |
| ongoing | Default gRPC message limits ([#417](https://github.com/ydb-platform/ydb-rs-sdk/pull/417)) merged |

## Changelog for agents

When making user-visible API or behavior changes, note them here briefly until a formal changelog process is adopted (unlike `ydb-go-sdk`, this repo does not yet require `CHANGELOG.md` entries).
