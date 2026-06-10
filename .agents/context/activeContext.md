# Active Context

> **Volatile file** — update after every significant work session.

## Current focus

Agent workspace under `.agents/` ([#428](https://github.com/ydb-platform/ydb-rs-sdk/issues/428)): `context/` for project knowledge, `rules/` for coding standards.

## Recent changes

- Consolidated agent assets: `memory-bank/` → `.agents/context/`, detailed rules → `.agents/rules/`.
- `AGENTS.md` slimmed to operational router; detailed context stays in `.agents/context/`.

## Open questions

- Whether to add nested `AGENTS.md` per crate as the workspace grows.
- Whether to add `.agents/skills/` and integrate with `ai-dev-kit` for YDB-wide agent tooling.

## Next steps

- Keep this file and `progress.md` updated as features land.
- Add rules to `AGENTS.md` only after repeated agent mistakes (incremental, not upfront).

## Working conventions (reminder)

- Read `activeContext.md` every session; other context files only when relevant.
- Coding rules: `AGENTS.md` → `.agents/rules/` (on demand).
- Update this file and `progress.md` before closing a PR.
- Run `cargo fmt --check` and `cargo clippy` before requesting review.
