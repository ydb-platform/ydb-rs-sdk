//! Query Service API PROTOTYPE (issue #207): multi-result-set streaming.
//! `ExecuteQueryResponsePart` is hidden, the public unit of iteration is a
//! logical `ResultSet`.

use ydb::{ClientBuilder, QueryExecutor};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
        .client()?;
    client.wait().await?;

    let qc = client.query_client();

    let sets = qc
        .retry_transaction(async |tx| {
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
            stream.close().await?; // consumes the stream, releases the borrow

            // `tx` is usable again after the stream is closed:
            tx.exec("DELETE FROM test").await?;

            Ok(set_count)
        })
        .await?;

    println!("result sets: {sets}");
    Ok(())
}
