# Agent Guidelines — ydb-rs-sdk

This file is the canonical source of agent instructions for this repository.
Tool-specific entry points (`CLAUDE.md`, Cursor rules, etc.) should reference this file, not duplicate it.

## Memory Bank (required workflow)

This project keeps long-term context in [`memory-bank/`](memory-bank/). Agent memory resets between sessions; the memory bank is the persistent project brain.

### Before starting any task

1. Read **all** core memory bank files (in this order):
   - [`memory-bank/projectbrief.md`](memory-bank/projectbrief.md)
   - [`memory-bank/productContext.md`](memory-bank/productContext.md)
   - [`memory-bank/systemPatterns.md`](memory-bank/systemPatterns.md)
   - [`memory-bank/techContext.md`](memory-bank/techContext.md)
   - [`memory-bank/activeContext.md`](memory-bank/activeContext.md)
   - [`memory-bank/progress.md`](memory-bank/progress.md)
2. Skim [`memory-bank/README.md`](memory-bank/README.md) for structure and update rules.
3. If the task touches a specific crate, also read the nearest crate `README.md` (`ydb/`, `ydb-grpc/`, etc.).

Do not write or change code until the memory bank has been loaded.

### After completing significant work

Update the memory bank before finishing the task:

1. **`activeContext.md`** — current focus, recent decisions, open questions, next steps.
2. **`progress.md`** — what was done, what remains, known issues.
3. **`systemPatterns.md`** or **`techContext.md`** — only when architecture, conventions, or tooling changed.
4. **`projectbrief.md`** / **`productContext.md`** — only when scope or product goals changed.

When the user says **"update memory bank"**, review **every** core file even if some need no edits.

Commit memory bank updates in the same PR as the code changes they describe.

## Coding

### Language and style

- All code comments, doc comments, error messages, and log messages must be in **English**.
- Follow existing Rust style in the touched module; do not reformat unrelated code.
- Run `cargo fmt` on changed files before submitting.

### Workspace layout

| Crate | Role |
|-------|------|
| `ydb` | Public SDK — main development surface |
| `ydb-grpc` | Generated protobuf/gRPC types (low-level, not for end users) |
| `ydb-example-urlshortener` | Example application |
| `ydb-slo-tests` | SLO/load tests |

`ydb-grpc-helpers` exists but is not a workspace member.

### Architecture conventions

- **Client entry point**: `ClientBuilder` → `Client` → service clients (`table_client()`, `scheme_client()`, `topic_client()`, etc.).
- **Retries**: table operations use `retry_transaction`; gRPC calls go through connection pool + interceptors.
- **Layering**: public API in `ydb/src/client_*` and `ydb/src/lib.rs` re-exports; gRPC details stay in `grpc_wrapper/`.
- **Integration tests**: marked `#[ignore]`; require `YDB_CONNECTION_STRING` and `--include-ignored`.

Match patterns in neighboring code before introducing new abstractions.

## Verification (definition of done)

Run from the repository root:

```bash
# Format check (CI)
cargo fmt --check

# Linter (CI) — ydb-grpc is excluded (generated code)
cargo clippy --workspace --all-targets --no-deps --exclude=ydb-grpc -- -D warnings

# Unit tests (no YDB required)
cargo test --workspace -- --skip-ignored
```

With a local YDB instance (`grpc://localhost:2136?database=/local`):

```bash
YDB_CONNECTION_STRING='grpc://localhost:2136?database=/local' \
  cargo test --workspace -- --include-ignored
```

A task is done when the relevant checks pass and the memory bank reflects the changes.

## Dependencies

- Do **not** change `Cargo.toml` / `Cargo.lock` unless the task explicitly requires it.
- Workspace dependency versions for `prost`, `tonic`, `pbjson` are pinned centrally in the root `Cargo.toml` — keep them in sync across crates.
- Minimum supported Rust: **1.82** (`rust-version` in workspace). CI also tests **1.91.0**.

## Versioning and releases

- Crates follow [SemVer](https://semver.org/). For `0.X.Y`: `X` = breaking, `Y` = compatible changes.
- Publishing is manual via `.github/workflows/publish-crate.yml` — do not bump versions unless asked.
- Breaking changes require a GitHub release describing incompatibilities.

## Pull requests

- Discuss non-trivial changes in a GitHub issue first (see [`CONTRIBUTING.md`](CONTRIBUTING.md)).
- Keep PRs focused; describe user-visible behavior changes in the PR body.
- Include memory bank updates when the change affects architecture, conventions, or ongoing work.

## Escalation

Stop and ask the user when:

- Multiple valid architectural approaches exist and the choice affects public API.
- The task requires dependency upgrades or MSRV changes.
- CI cannot be run locally and the change is high-risk (public API, retry semantics, connection lifecycle).
