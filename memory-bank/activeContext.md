# Active Context

> **Volatile file** — update after every significant work session.

## Current focus

Initial memory bank setup ([#428](https://github.com/ydb-platform/ydb-rs-sdk/issues/428)): establishing `memory-bank/`, `AGENTS.md`, and `CLAUDE.md` so AI agents have persistent project context.

## Recent changes

- Added memory bank with six core context files and agent workflow in `AGENTS.md`.
- `CLAUDE.md` points to `AGENTS.md` as the single source of truth.

## Open questions

- Whether to add nested `AGENTS.md` per crate (`ydb/`, `ydb-grpc/`) as the workspace grows.
- Whether to integrate with `ai-dev-kit` skill installation for YDB-wide agent tooling.

## Next steps

- Keep `activeContext.md` and `progress.md` updated as features land (e.g. scheme `describe_path`, connection pool improvements).
- Align cross-SDK APIs with `ydb-go-sdk` where parity gaps are reported in issues.

## Working conventions (reminder)

- Read all memory bank files before coding.
- Update this file and `progress.md` before closing a PR.
- Run `cargo fmt --check` and `cargo clippy` before requesting review.
