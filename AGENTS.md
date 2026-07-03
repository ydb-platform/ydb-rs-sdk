# Agent Guidelines — ydb-rs-sdk

Canonical agent entry point. Tool configs (`CLAUDE.md`, Cursor rules) start here and route to [`.agents/`](.agents/).

Keep this file **lean** (~60 lines) — it routes to detailed sources. Loading a large AGENTS.md on every session wastes context tokens; use a thin navigation file plus on-demand docs (see [agentsmd/agents.md](https://github.com/agentsmd/agents.md)).

## Project context

Project knowledge lives in [`.agents/context/`](.agents/context/). Coding rules live in [`.agents/rules/`](.agents/rules/) (below). See [`.agents/README.md`](.agents/README.md) for the full layout.

**Before coding** — read selectively:

1. One stable file as needed:
   - architecture / module layout → [`systemPatterns.md`](.agents/context/systemPatterns.md)
   - toolchain / CI / local dev → [`techContext.md`](.agents/context/techContext.md)
   - API surface / users → [`productContext.md`](.agents/context/productContext.md)
   - scope / goals → [`projectBrief.md`](.agents/context/projectBrief.md)
3. Quick lookup: [`README.md`](README.md), [`CONTRIBUTING.md`](CONTRIBUTING.md), [docs.rs/ydb](https://docs.rs/ydb)

**After significant work** — update stable context files (`systemPatterns.md`, `productContext.md`, `techContext.md`, `projectBrief.md`) when the work merges. Do **not** merge changes to `activeContext.md` or `progress.md` (placeholders — see file headers).

On **"update memory bank"** — review all core files in [`.agents/context/README.md`](.agents/context/README.md).

## Coding rules (load on demand)

| Topic | File |
|-------|------|
| Style, API boundaries, dependencies | [`.agents/rules/coding-standards.md`](.agents/rules/coding-standards.md) |
| Unit vs integration tests, local YDB | [`.agents/rules/testing.md`](.agents/rules/testing.md) |
| Issue-first workflow, user boundaries | [`.agents/rules/workflow.md`](.agents/rules/workflow.md) |
| Local dev, docker-compose, CI commands | [`.agents/rules/environment.md`](.agents/rules/environment.md) |

## Non-obvious rules (always on)

- Priorities: correctness → readability → public-API ergonomics → simple internal code → performance only where it matters.
- Visibility: private by default, `pub(crate)` to share inside the crate, `pub` only for SDK consumers.
- No `unwrap` / `expect` / `panic!` / `unsafe` in production code; `unreachable!` only with explicit human approval. Tests may use `unwrap`/`expect`.
- No silent error swallowing on a broken invariant (`.ok()`, `unwrap_or_default()`, empty fallbacks).
- Deadline-based retries via `tokio::time::timeout`; do not introduce `max_retries`.
- No commented-out code or debug prints in committed code (including `examples/`).
- Comments, doc comments, error messages, logs: **English**.
- Match style in the touched module; do not reformat unrelated code.
- Do **not** change `Cargo.toml` / `Cargo.lock` unless the task requires it.
- MSRV is **1.85** (`rust-version` in workspace `Cargo.toml`); CI runs on Rust 1.85 and 1.91.
- Integration tests are `#[ignore]`; need `YDB_CONNECTION_STRING` and `--include-ignored`.
- `ydb-grpc` is generated; clippy excludes it. Do not bump crate versions unless asked.
- Non-trivial changes: discuss in a GitHub issue first ([`CONTRIBUTING.md`](CONTRIBUTING.md)).
- **Every task must end with the linter gate** (see [Done when](#done-when)) — do not hand off or open a PR until `cargo fmt --check` and CI clippy pass on touched crates.

## Done when

From repo root — **run before every handoff / PR**, even for small or doc-only changes in Rust code:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --no-deps --exclude=ydb-grpc -- -D warnings
cargo test --workspace
```

Fix all clippy warnings (`-D warnings`); do not rely on `cargo test` alone. `ydb-grpc` is excluded from clippy (generated code).

Ask the user before dependency upgrades, MSRV changes, or public API design choices.
