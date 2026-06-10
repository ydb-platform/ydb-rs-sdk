# Memory Bank — ydb-rs-sdk

Structured, version-controlled context for AI coding agents working on this repository.

Based on the [Memory Bank](https://docs.cline.bot/best-practices/memory-bank) pattern and the [AGENTS.md](https://agents.md/) open standard. See [`AGENTS.md`](../AGENTS.md) for the mandatory read/update workflow.

## Core files

Read all of these at the start of every session:

| File | Stability | Purpose |
|------|-----------|---------|
| [`projectbrief.md`](projectbrief.md) | Stable | Scope, goals, constraints |
| [`productContext.md`](productContext.md) | Stable | Users, problems solved, API surface |
| [`systemPatterns.md`](systemPatterns.md) | Evolving | Architecture, modules, design patterns |
| [`techContext.md`](techContext.md) | Evolving | Toolchain, CI, local dev setup |
| [`activeContext.md`](activeContext.md) | **Volatile** | Current task, decisions, next steps |
| [`progress.md`](progress.md) | **Volatile** | Status log, milestones, known issues |

## File hierarchy

```
projectbrief.md ──┬── productContext.md ──┐
                  ├── systemPatterns.md  ──┼── activeContext.md ── progress.md
                  └── techContext.md ──────┘
```

`activeContext.md` and `progress.md` change most often. Update them after every meaningful session.

## Update triggers

Update the memory bank when:

1. A feature, fix, or refactor is merged or ready for PR.
2. Architecture or public API conventions change.
3. CI commands, MSRV, or dev setup change.
4. The user requests **"update memory bank"** (full review of all core files).

## What not to store here

- Secrets, tokens, or personal credentials.
- Large generated artifacts or full API dumps (link to `docs.rs` instead).
- Chat transcripts.
