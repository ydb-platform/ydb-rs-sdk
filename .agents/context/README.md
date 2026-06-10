# Project context — ydb-rs-sdk

Structured, version-controlled context for AI coding agents.

`AGENTS.md` stays minimal (operational router). This directory holds detailed context — read **selectively**, not all at once.

> Paths in context files may lag behind refactors — verify with Glob/Grep before acting on specific filenames.

## Knowledge tree

```
ydb-rs-sdk/
├── .agents/                        ← agent workspace (you are in context/)
│   ├── README.md                   layout of context / rules
│   ├── context/                    ← you are here
│   │   ├── README.md               entry point, reading/update strategy
│   │   ├── activeContext.md        branch-only scratch pad (policy placeholder on master)
│   │   ├── progress.md             evolving: completed milestones
│   │   ├── projectBrief.md         stable: scope, goals, constraints
│   │   ├── productContext.md       stable: users, API surface, feature parity
│   │   ├── systemPatterns.md       evolving: workspace layout, module patterns
│   │   └── techContext.md          evolving: CI, MSRV, local YDB, build commands
│   └── rules/                      coding standards (on demand via AGENTS.md)
│
├── AGENTS.md                       lean agent router (read first)
├── CLAUDE.md                       tool entry point → AGENTS.md
├── ydb/                            public SDK crate
│   └── examples/                   cargo example snippets (*.rs)
├── ydb-grpc/                       generated protobuf + tonic stubs
└── ydb-slo-tests/
```

## Core files

| File | Stability | Read when |
|------|-----------|-----------|
| [`activeContext.md`](activeContext.md) | **Branch-only** | Policy placeholder on `master`; optional scratch pad on a feature branch — **never merge edits** |
| [`progress.md`](progress.md) | Evolving | Completed milestones — update in the PR that delivers the work |
| [`systemPatterns.md`](systemPatterns.md) | Evolving | Architecture, new modules, API layering |
| [`techContext.md`](techContext.md) | Evolving | CI, MSRV, local YDB, build commands |
| [`productContext.md`](productContext.md) | Stable | Public API, users, feature parity |
| [`projectBrief.md`](projectBrief.md) | Stable | Scope, goals, constraints |

## Reading strategy

```
If needed:      one stable file matching the task (see table)
Code patterns:  .agents/rules/ via AGENTS.md router (on demand)
Full review:    all files (on "update memory bank" or major onboarding)
```

Avoid loading all six core files at session start — it wastes context tokens without improving outcomes.

## Update triggers

1. Feature/fix ready for PR → `progress.md` (if milestone); revert `activeContext.md` to placeholder
2. Architecture or CI changed → `systemPatterns.md` or `techContext.md`
3. Scope changed → `projectBrief.md` or `productContext.md`
4. User says **"update memory bank"** → review every core file

## What not to store

- Secrets, tokens, credentials
- Large generated artifacts (link to `docs.rs` instead)
- Chat transcripts
- Duplication of `AGENTS.md` rules — link instead

## Related issues

- [#428](https://github.com/ydb-platform/ydb-rs-sdk/issues/428) — agent context for this project
