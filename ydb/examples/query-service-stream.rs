//! Multi-result-set streaming inside `retry_transaction` (lazy tx on implicit session).

use ydb::{ClientBuilder, Transaction};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client =
        ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?.client()?;
    client.wait().await?;

    let qc = client.query_client();

    let sets = qc
        // Annotate the parameter type (`tx: &mut Transaction`) so the
        // IDE can complete methods on `tx`: rust-analyzer does not yet
        // reliably infer `async ||` closure parameter types from the
        // `AsyncFnMut` bound. The compiler infers it fine without this.
        .retry_transaction(async |tx: &mut Transaction| {
            let mut stream = tx.query("SELECT 42 AS a; SELECT 1 AS b, 2 AS c;").await?;

            // While `stream` is alive, `tx` stays mutably borrowed — a second
            // concurrent query in the same transaction does not compile:
            //
            //     tx.exec("SELECT 1").await?;
            //     // error[E0499]: cannot borrow `*tx` as mutable more than once
            //
            // The single-stream-per-transaction invariant comes for free.

            let mut set_count = 0;
            while let Some(result_set) = stream.next_result_set().await? {
                for mut row in result_set {
                    let _ = row.remove_field_by_name("a");
                }
                set_count += 1;
            }
            stream.close().await?;

            Ok(set_count)
        })
        .await?;

    println!("result sets: {sets}");
    Ok(())
}
