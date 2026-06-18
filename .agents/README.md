# Agent workspace — ydb-rs-sdk

Canonical home for AI coding agent assets. Human and agent entry point remains [`AGENTS.md`](../AGENTS.md) in the repo root.

## Layout

| Directory | Purpose | Load when |
|-----------|---------|-----------|
| [`context/`](context/) | Project knowledge — architecture, tooling, progress | Stable files on demand; `activeContext.md` is branch-only (see its header) |
| [`rules/`](rules/) | Coding standards and workflow | On demand via `AGENTS.md` router |

`skills/` and `prompts/` may be added later when project-specific agent workflows are needed (see [ydb-pg-extension `.agents/`](https://github.com/ydb-platform/ydb-pg-extension/tree/main/.agents) for reference).

## Related

- [#428](https://github.com/ydb-platform/ydb-rs-sdk/issues/428) — agent context for this project
