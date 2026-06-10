# Active Context

> **Volatile file** — update after every significant work session.

## Current focus

Memory bank for AI agents ([#428](https://github.com/ydb-platform/ydb-rs-sdk/issues/428)). `AGENTS.md` slimmed to operational rules only; detailed context stays here.

## Recent changes

- `AGENTS.md` reduced to ~40 lines: selective memory-bank reads, non-obvious rules, CI commands.
- Removed duplication between `AGENTS.md` and stable memory-bank files.

## Open questions

- Whether to add nested `AGENTS.md` per crate as the workspace grows.
- Whether to integrate with `ai-dev-kit` for YDB-wide agent tooling.

## Next steps

- Keep this file and `progress.md` updated as features land.
- Add rules to `AGENTS.md` only after repeated agent mistakes (incremental, not upfront).

## Working conventions (reminder)

- Read `activeContext.md` every session; other memory-bank files only when relevant.
- Update this file and `progress.md` before closing a PR.
- Run `cargo fmt --check` and `cargo clippy` before requesting review.
