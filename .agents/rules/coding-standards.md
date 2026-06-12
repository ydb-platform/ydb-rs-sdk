# Coding Standards

Read when touching public API, module layout, dependencies, or error handling.

## Style

- Comments, doc comments, error messages, and logs: **English**.
- Match naming and formatting in the touched module; do not reformat unrelated code.

## Lint before handoff (required)

After editing Rust code, **always** run from repo root before marking work complete:

```bash
cargo fmt
cargo fmt --check
cargo clippy --workspace --all-targets --no-deps --exclude=ydb-grpc -- -D warnings
```

Fix all reported issues in touched files. CI runs the same checks in [`.github/workflows/linter.yaml`](../../.github/workflows/linter.yaml).

## Dependencies and versions

- Do **not** change `Cargo.toml` / `Cargo.lock` unless the task explicitly requires it.
- Do not run `cargo update` or bump workspace dependency versions unless asked.
- Shared versions live in root `[workspace.dependencies]`; member crates use `workspace = true`.
- Do not bump published crate versions unless the user requests a release.

## Public API (`ydb` crate)

- `ydb-grpc` types are internal — do not leak them in the public API without a stable wrapper.
- New APIs follow the layered pattern in [`.agents/context/systemPatterns.md`](../context/systemPatterns.md).
- Public API changes must be semver-aware and intentional.
- Many enums are `#[non_exhaustive]`; respect this unless using `force-exhaustive-all` for downstream checks.

## Architecture anti-patterns

See `systemPatterns.md` for layout. In short:

- Bypassing the connection pool for production RPC paths.
- Adding dependencies without workspace-level version alignment.
- Exposing raw tonic/prost types as stable public types.
