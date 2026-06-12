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

**After significant work** — update `progress.md` and stable context files when the work itself merges. Do **not** merge changes to `activeContext.md` (branch-only scratch pad — see file header).

On **"update memory bank"** — review all core files in [`.agents/context/README.md`](.agents/context/README.md).

## Coding rules (load on demand)

| Topic | File |
|-------|------|
| Style, API boundaries, dependencies | [`.agents/rules/coding-standards.md`](.agents/rules/coding-standards.md) |
| Unit vs integration tests, local YDB | [`.agents/rules/testing.md`](.agents/rules/testing.md) |
| Issue-first workflow, user boundaries | [`.agents/rules/workflow.md`](.agents/rules/workflow.md) |
| Local dev, docker-compose, CI commands | [`.agents/rules/environment.md`](.agents/rules/environment.md) |

## Non-obvious rules (always on)

- Comments, doc comments, error messages, logs: **English**.
- Match style in the touched module; do not reformat unrelated code.
- Do **not** change `Cargo.toml` / `Cargo.lock` unless the task requires it.
- Integration tests are `#[ignore]`; need `YDB_CONNECTION_STRING` and `--include-ignored`.
- `ydb-grpc` is generated; clippy excludes it. Do not bump crate versions unless asked.
- Non-trivial changes: discuss in a GitHub issue first ([`CONTRIBUTING.md`](CONTRIBUTING.md)).

## Done when

From repo root:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --no-deps --exclude=ydb-grpc -- -D warnings
cargo test --workspace
```

Also: update `progress.md` only when the delivered work merges.

Ask the user before dependency upgrades, MSRV changes, or public API design choices.
