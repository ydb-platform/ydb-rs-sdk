# Active Context

> **Volatile file** — update at the end of a work session or before closing a PR.
>
> **Merge conflicts:** with many parallel PRs this file conflicts often. On conflict, keep the union of recent decisions or reset to a short generic focus — do not block the feature PR on agent housekeeping.

## Current focus

_No active task recorded._ Check open PRs and GitHub Issues for ongoing work.

## Recent changes

- Agent workspace under `.agents/` (`context/`, `rules/`).

## Open questions

- Whether to add nested `AGENTS.md` per crate as the workspace grows.
- Whether to add `.agents/skills/` and integrate with `ai-dev-kit`.

## Next steps

- Update this file when starting or finishing significant work.

## Working conventions (reminder)

- Read this file every session; other context files only when relevant.
- Coding rules: `AGENTS.md` → `.agents/rules/` (on demand).
- Update this file and `progress.md` before closing a PR.
- Run `cargo fmt --check` and `cargo clippy` before requesting review.
