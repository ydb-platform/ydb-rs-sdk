//! Multi-result-set streaming inside `retry_tx` (lazy tx on implicit session).

use futures_util::TryStreamExt;
use ydb::{ClientBuilder, Transaction, closure};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?
        .build()
        .await?;

    let qc = client.query_client();

    let sets = qc
        // Annotate the parameter type (`tx: &mut Transaction`) so the
        // IDE can complete methods on `tx`: rust-analyzer does not yet
        // reliably infer `async ||` closure parameter types from the
        // `AsyncFnMut` bound. The compiler infers it fine without this.
        .retry_tx(closure!(async |tx: &mut Transaction| {
            let mut stream = tx.query("SELECT 42 AS a; SELECT 1 AS b, 2 AS c;").await?;

            // While `stream` is alive, `tx` stays mutably borrowed — a second
            // concurrent query in the same transaction does not compile:
            //
            //     tx.exec("SELECT 1").await?;
            //     // error[E0499]: cannot borrow `*tx` as mutable more than once
            //
            // The single-stream-per-transaction invariant comes for free.

            let mut result_set_count = 0;
            while let Some(mut result_set) = stream.next_result_set().await? {
                while let Some(part) = result_set.try_next().await? {
                    for mut row in part {
                        let _ = row.remove_field_by_name("a");
                    }
                }
                result_set_count += 1;
            }

            Ok(result_set_count)
        }))
        .await?;

    println!("result sets: {sets}");
    Ok(())
}
