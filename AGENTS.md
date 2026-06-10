# Agent Guidelines — ydb-rs-sdk

Canonical agent instructions. Tool entry points (`CLAUDE.md`, Cursor rules) reference this file only.

## Memory bank

Long-term context lives in [`memory-bank/`](memory-bank/). Details: architecture, CI, product scope.

**Before coding** — read selectively (do not load everything):

1. [`memory-bank/activeContext.md`](memory-bank/activeContext.md) — always
2. One stable file if the task needs it:
   - architecture / module layout → [`systemPatterns.md`](memory-bank/systemPatterns.md)
   - toolchain / CI / local dev → [`techContext.md`](memory-bank/techContext.md)
   - API surface / users → [`productContext.md`](memory-bank/productContext.md)
   - scope / goals → [`projectBrief.md`](memory-bank/projectBrief.md)

**After significant work** — update `activeContext.md` and `progress.md`. Update stable files only when architecture, tooling, or scope changed.

On **"update memory bank"** — review all core files.

## Rules (non-obvious)

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

Ask the user before dependency upgrades, MSRV changes, or public API design choices.
