# Topic examples for ydb.tech

This executable supplies the Rust snippets in the topic reference on ydb.tech.
It runs against the SDK in this checkout and checks topic management, writing,
acknowledgments, selectors, committing and transactional reading.

Run from the repository root with a local YDB instance:

```sh
cargo run -p ydb --example ydb-tech-topic
```

`YDB_CONNECTION_STRING` defaults to `grpc://localhost:2136/local`.
The application uses unique topic names and a 90-second deadline.
