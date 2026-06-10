# Memory Bank — ydb-rs-sdk

Structured, version-controlled context for AI coding agents.

`AGENTS.md` stays minimal (operational rules only). This directory holds detailed context — read **selectively**, not all at once.

## Core files

| File | Stability | Read when |
|------|-----------|-----------|
| [`activeContext.md`](activeContext.md) | **Volatile** | **Every session** — current focus, decisions, next steps |
| [`progress.md`](progress.md) | **Volatile** | Resuming work, closing a PR, status checks |
| [`systemPatterns.md`](systemPatterns.md) | Evolving | Architecture, new modules, API layering |
| [`techContext.md`](techContext.md) | Evolving | CI, MSRV, local YDB, build commands |
| [`productContext.md`](productContext.md) | Stable | Public API, users, feature parity |
| [`projectbrief.md`](projectbrief.md) | Stable | Scope, goals, constraints |

## Reading strategy

```
Every session:  activeContext.md
If needed:      one stable file matching the task
Full review:    all files (on "update memory bank" or major onboarding)
```

Avoid loading all six core files at session start — it wastes context tokens without improving outcomes ([research](https://arxiv.org/abs/2602.11988)).

## Update triggers

1. Feature/fix ready for PR → `activeContext.md` + `progress.md`
2. Architecture or CI changed → `systemPatterns.md` or `techContext.md`
3. Scope changed → `projectBrief.md` or `productContext.md`
4. User says **"update memory bank"** → review every core file

## What not to store

- Secrets, tokens, credentials
- Large generated artifacts (link to `docs.rs` instead)
- Chat transcripts
