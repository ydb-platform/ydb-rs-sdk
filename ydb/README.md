## Rust YDB SDK [![Latest Version](https://img.shields.io/crates/v/ydb.svg)](https://crates.io/crates/ydb)
[Documentation](https://docs.rs/ydb)

Rust SDK for YDB.
Supported rust: 1.68.0 and newer.


Integration tests, with dependency from real YDB database mark as ignored.
For run it:
1. Set YDB_CONNECTION_STRING env
2. run cargo test -- --ignored

### Cargo feature force-exhaustive-all

disable all non_exhaustive marks in public interface for force
check new variants at compile time instead of runtime.
