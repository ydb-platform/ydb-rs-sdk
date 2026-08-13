#![recursion_limit = "256"]
//! Query Service `EXPLAIN` — inspect how YDB would run a query, without running it.
//!
//! The server compiles the query (resolving types and schema) and returns its execution plan as
//! JSON plus the compiled MiniKQL AST. Nothing is executed, which makes this safe for expensive
//! or side-effecting statements.

use std::time::Duration;

use ydb::{ClientBuilder, YdbResult};

const QUERY: &str = "SELECT * FROM `.sys/nodes`";
const EXAMPLE_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?
        .build()
        .await?;
    let qc = client.query_client();

    // 1. A plain SELECT: the plan shows how the table is read, the AST shows the compiled form.
    let explained = qc.explain(QUERY).timeout(EXAMPLE_TIMEOUT).await?;
    println!("query plan:\n{}\n", explained.query_plan);
    println!("query AST:\n{}\n", explained.query_ast);

    // 2. Compilation resolves schema, so a missing table is reported here — no execution needed.
    match qc
        .explain("SELECT * FROM `no_such_table`")
        .timeout(EXAMPLE_TIMEOUT)
        .await
    {
        Ok(_) => println!("missing table: unexpectedly accepted"),
        Err(err) => println!("missing table: {err}"),
    }

    // 3. Statements with nothing to plan (DDL) come back without a plan.
    match qc
        .explain("CREATE TABLE explain_example (id Int64, PRIMARY KEY(id))")
        .timeout(EXAMPLE_TIMEOUT)
        .await
    {
        Ok(_) => println!("DDL: unexpectedly returned a plan"),
        Err(err) => println!("DDL: {err}"),
    }

    Ok(())
}
