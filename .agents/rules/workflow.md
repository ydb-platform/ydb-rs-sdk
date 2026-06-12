# Workflow

## Issue-first (required for non-trivial work)

Per [`CONTRIBUTING.md`](../../CONTRIBUTING.md): discuss new features and bug fixes in a GitHub issue before implementing. If no issue exists, open one.

## User-request boundaries

Stop and ask when blocked — do not ship an unsanctioned alternative approach.

✅ **Correct**

```
User: "Implement feature X using approach A"
Agent: "Attempting approach A..."
Agent: "Approach A hit error P: <details>. Next step?"
```

❌ **Wrong**

```
User: "Fix the build using method M"
Agent: "Method M failed, so I implemented alternative N instead."
```

## Code reuse

1. Search the repo for similar helpers (`rg`, IDE search) before adding new utilities.
2. Follow existing retry/pool/error-mapping patterns in `client_table.rs`, `trait_operation.rs`.
3. Extend shared helpers rather than duplicating logic across `grpc_wrapper` and `client_*` layers.

## Context updates

- **`activeContext.md`** — branch-only scratch pad. Revert to the placeholder before merge; never land session notes on `master`.
- **`progress.md`** — update in the PR that delivers completed work.
- Stable files (`systemPatterns.md`, …) — only when architecture, tooling, or scope actually changed.

Add rules to `AGENTS.md` only after repeated agent mistakes — incremental, not upfront.
