# Rust YDB SDK
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ydb-platform/ydb/blob/main/LICENSE)
[![Latest Version](https://img.shields.io/crates/v/ydb.svg)](https://crates.io/crates/ydb)
[![Released API docs](https://docs.rs/ydb/badge.svg)](https://docs.rs/ydb)
[![Linter](https://github.com/ydb-platform/ydb-rs-sdk/workflows/Linter/badge.svg)](https://github.com/ydb-platform/ydb-rs-sdk/actions/workflows/linter.yml)
[![YDB tests](https://github.com/ydb-platform/ydb-rs-sdk/actions/workflows/rust-tests.yml/badge.svg?branch=master&event=schedule)](https://github.com/ydb-platform/ydb-rs-sdk/actions/workflows/rust-tests.yml)
[![codecov](https://codecov.io/gh/ydb-platform/ydb-rs-sdk/badge.svg?precision=2)](https://app.codecov.io/gh/ydb-platform/ydb-rs-sdk)
[![View examples](https://img.shields.io/badge/learn-examples-brightgreen.svg)](https://github.com/ydb-platform/ydb-rs-sdk/tree/master/ydb/examples)
[![Telegram](https://img.shields.io/badge/chat-on%20Telegram-2ba2d9.svg)](https://t.me/ydb_en)
[![WebSite](https://img.shields.io/badge/website-ydb.tech-blue.svg)](https://ydb.tech)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/CONTRIBUTING.md)

Rust SDK for YDB.

### Prerequisites
Rust 1.85.0 or newer

### Installation
Add the YDB dependency to your project using `cargo add ydb` or add this your Cargo.toml:
```toml
[dependencies]
ydb = "0.2.2"
```

### Example
Create a new Rust file (e.g., main.rs) and add the following code:

```rust
use ydb::{ClientBuilder, Query, AccessTokenCredentials, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {

 // create the driver
 let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
    .with_credentials(AccessTokenCredentials::from("asd"))
    .client()?;

 // wait until the background initialization of the driver finishes
 // In this example, it will never be resolved because YDB methods have
 // infinite retries by default. You can manage it using a standard
 // Tokio timeout.
 client.wait().await?;

 // read the query result
 let sum: i32 = client
    .table_client() // create table client
    .retry_transaction(|mut t| async move {
        // the code in transaction can retry a few times if there was a retriable error

        // send the query to the database
        let res = t.query(Query::from("SELECT 1 + 1 AS sum")).await?;

        // read exactly one result from the db
        let field_val: i32 = res.into_only_row()?.remove_field_by_name("sum")?.try_into()?;

        // return result
        return Ok(field_val);
    })
    .await?;

 // this will print "sum: 2"
 println!("sum: {}", sum);
    return Ok(());
}
```

For more examples, see [ydb/examples](https://github.com/ydb-platform/ydb-rs-sdk/tree/master/ydb/examples).

## Tests

Integration tests, with dependency from real YDB database marked as ignored.
To run it:
1. Set YDB_CONNECTION_STRING env
2. run cargo test -- --include-ignored

# Version policy

Crates follow to semver 2.0 https://semver.org/spec/v2.0.0.html.
For version 0.X.Y: X increments for expected backwards incompatible changes, Y increments for any compatible changes (fixes, extend api without broke compatible).
For incompatible changes creates github release with describe incompatibles.
