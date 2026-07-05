# Coding Standards

Read when touching public API, module layout, dependencies, error handling, or anything non-trivial.

## Priorities

In order, highest first:

1. **Correctness.**
2. **Readability and maintainability for humans.** Simple constructs, consistent style, simple contracts, simple dependencies.
3. **Ergonomics of the public API** — external users live with it; calling it must be pleasant.
4. **Simplicity of internal code** — boring, straight-line, easy to follow.
5. **Performance** — where it actually matters. Don't pessimize for no reason, but don't add complexity for speculative wins either.

When two pull in opposite directions, the higher one wins.

## Style

- Comments, doc comments, error messages, and logs: **English**.
- Match naming and formatting in the touched module; do not reformat unrelated code.
- Run `cargo fmt` on changed files before handoff.
- **Before handoff:** run `cargo fmt --check` and CI clippy (`cargo clippy --workspace --all-targets --no-deps --exclude=ydb-grpc -- -D warnings`); fix all warnings.
- No commented-out code and no debug `println!`/`dbg!` in committed code (including `examples/`).

## Rust edition / version

- MSRV is not pinned in policy: feel free to use recent language and `std` features when they meaningfully simplify the code.
- CI compiles against Rust 1.85 and 1.91; if you use a newer feature, make sure it compiles on the latest of those — otherwise raise the question.

## Visibility

- Default is **private**.
- Use `pub(crate)` when something must be shared inside the crate.
- `pub` is reserved for items intended for SDK consumers.

## Types

- `Option<T>` only when "absent" is a real domain state. If a sensible default exists (e.g. `Codec::Raw`), make the field required and use the default.
- Small `Copy` enums and scalars are passed by value, not by reference.
- **In the public API**, do not introduce `type Alias = SomeStdType` just to attach one convenient method or shorten a name. Wrap it in a real `struct` with its own API so the implementation can change without breaking consumers. Inside the crate, type aliases are fine.

## Public API (`ydb` crate)

- `ydb-grpc` types are internal — do not leak `prost`/`tonic` types (`prost::Bytes`, tonic streams, raw gRPC futures) into public signatures. Wrap, or use `Vec<u8>` / `bytes::Bytes`.
- New APIs follow the layered pattern in [`.agents/context/systemPatterns.md`](../context/systemPatterns.md).
- Public API changes must be semver-aware and intentional.
- Many enums are `#[non_exhaustive]`; respect this unless using `force-exhaustive-all` for downstream checks.
- Mark experimental / non-production APIs in rustdoc.
- Prefer ergonomic argument types where the caller has several natural forms: `impl Into<String>` / `impl AsRef<str>` for stringy inputs (see `retry_explain_data_query`).
- Evolve additively: new behavior gets a new method (e.g. `commit_with_ack` alongside `commit`), not a changed signature on an existing one.
- Raw-client (`grpc_wrapper/raw_*`) methods return the decoded response struct as-is, so newly added server fields propagate without code changes.

## Errors

- **No `unwrap` / `expect` / `panic!` / `unsafe`** in production code. If an invariant truly must hold, return `YdbError` with a diagnostic message. In tests, `unwrap`/`expect` are fine.
- `unreachable!` is allowed **only with explicit human approval** when there is no other way to express the invariant. The comment next to it must explain what makes the branch impossible.
- An internal invariant breaking is an error, not a silent fallback. No `.ok()` / `unwrap_or_default()` / empty-default returns to mask broken state.
- Error messages must embed the offending identifier (column, table, topic, path, codec id, …) so a single log line is diagnosable.
- Workarounds and hacks live behind a typed flag or named constant, with a comment linking to the tracking issue and explaining the removal condition.
- When reacting to a failed RPC, classify by **gRPC error / status code** first. Matching on error text or YDB issue codes is a last resort, used only when nothing else works, and explicitly called out in a comment near the match.

## Naming and readability

- Names reflect semantics: `execute` means synchronous run; methods that only enqueue are `schedule`/`spawn`/`submit`. `get_*` does not mutate. Paired serialize/deserialize share a name root.
- Prefer positive booleans (`fallback_enabled`) over negated ones (`!disabled_fallback`).
- Magic numbers become named `const`s — and the same constant is reused at the producer and consumer of the value.
- Flatten deeply nested `match`-on-`await` chains (especially in reader/writer paths) — extract or use early returns.
- Return named `struct`s, not `(T, bool)` or anonymous tuples, in non-trivial code.
- Examples in `ydb/examples/` must compile and run; strip debug noise before merge.
- Manual `drop(self.field)` to "force" cleanup is an antipattern — `Drop` runs anyway.

## Dependencies and versions

- Do **not** change `Cargo.toml` / `Cargo.lock` unless the task explicitly requires it.
- Do not run `cargo update` or bump workspace dependency versions unless asked.
- Shared versions live in root `[workspace.dependencies]`; member crates use `workspace = true`.
- Do not bump published crate versions unless the user requests a release.
- **Test-only and dev-only code must not leak into production surface.** Helpers used only by tests live behind `#[cfg(test)]` or in dedicated test modules; test-only crates go in `[dev-dependencies]`. Public API must not depend on them, even transitively.
- Before writing a new helper, search the workspace, `ydb-grpc`, and `tokio` APIs for an existing one — extend it rather than introducing a parallel implementation.

## Concurrency and performance

Apply when the code path actually matters (hot loops, reader/writer pipelines, connection pool). Don't preemptively complicate cold paths.

- Retries and timeouts are **deadline-based**: use `tokio::time::timeout` and per-call `.timeout()` on builders; do not introduce `max_retries` knobs.
- **Driver retry budget** (`retry_budget.rs`): second+ retry attempts call `RetryBudget::acquire(deadline)`; when exhausted, wait for quota or until the call deadline. Topic reconnectors use their own `Retry` — not the driver budget.
- CPU-heavy work (compression, hashing of large payloads) runs on a worker pool / `spawn_blocking` — never on the async task that owns the request.
- Cap parallel-work chunks by message count, not only by byte size, so one large batch can't pin a worker.
- In hot paths: no avoidable `String` allocations where `&str` / `bytes::Bytes` works; reserve `Vec`/`HashMap` capacity when the size is known; pre-resolve name → index in setup so the inner loop indexes.
- Gate expensive `log!` argument construction behind `log_enabled!`.

## Architecture anti-patterns

See `systemPatterns.md` for layout. In short:

- Bypassing the connection pool for production RPC paths.
- Adding dependencies without workspace-level version alignment.
- Exposing raw tonic/prost types as stable public types.
- Reinventing a helper that already exists in the workspace.
