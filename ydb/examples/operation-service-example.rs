use std::time::Duration;

use tokio::time::sleep;
use ydb::{ClientBuilder, ListOperationsRequest, OperationKind, YdbError, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    let connection_string = std::env::var("YDB_CONNECTION_STRING")
        .unwrap_or_else(|_| "grpc://localhost:2136/local".to_string());

    let client = ClientBuilder::new_from_connection_string(connection_string)?.client()?;

    if let Ok(res) = tokio::time::timeout(Duration::from_secs(10), client.wait()).await {
        res?
    } else {
        return Err(YdbError::from("connection timeout"));
    }

    let op_client = client.operation_client();

    let listed = op_client
        .list_operations(ListOperationsRequest::new(OperationKind::EXECUTE_QUERY))
        .await?;

    println!(
        "script operations listed: {} (next_page_token len={})",
        listed.operations.len(),
        listed.next_page_token.len()
    );

    for op in &listed.operations {
        println!(
            "operation id={} ready={} status={}",
            op.id, op.ready, op.status
        );
        if !op.ready {
            op_client.cancel_operation(&op.id).await?;
            println!("cancel requested for {}", op.id);

            for _ in 0..30 {
                let status = op_client.get_operation(&op.id).await?;
                if status.ready {
                    println!("operation {} finished with status {}", op.id, status.status);
                    op_client.forget_operation(&op.id).await?;
                    println!("operation {} forgotten", op.id);
                    break;
                }
                sleep(Duration::from_millis(500)).await;
            }
        }
    }

    Ok(())
}
