# Progress

> **Placeholder on `main`/`master`** — no durable project knowledge here.

## Policy

- **Green master:** `main`/`master` holds only completed, merged work.
- **This file must have no diff at PR merge time.** Durable updates belong in stable context files (`systemPatterns.md`, `productContext.md`, `techContext.md`, `projectBrief.md`).
- On `main`/`master` this file stays exactly this placeholder.

## Where to put durable knowledge

| What | Where |
|------|-------|
| Architecture, retries, module layout | `systemPatterns.md` |
| API surface, coverage, gaps, releases | `productContext.md` |
| CI, MSRV, local dev, SLO | `techContext.md` |
| Scope, goals, constraints | `projectBrief.md` |
| Branch/session notes | `activeContext.md` (branch-only; also no merge) |
| Coding rules | `AGENTS.md` → `.agents/rules/` |
