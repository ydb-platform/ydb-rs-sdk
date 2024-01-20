use std::time::Duration;

use tokio::task::JoinHandle;

use ydb::{
    ClientBuilder, CoordinationSession, NodeConfigBuilder, SessionOptionsBuilder, YdbResult,
};

async fn mutex_work(session: CoordinationSession) {
    let lease = session
        .acquire_semaphore("my-resource".to_string(), 1)
        .await
        .unwrap();

    let lease_alive = lease.alive();
    println!("acquired semaphore");
    tokio::select! {
        _ = lease_alive.cancelled() => {},
        _ = tokio::time::sleep(Duration::from_millis(20)) => {
            println!("finished work");
        },
    }
}

#[tokio::main]
async fn main() -> YdbResult<()> {
    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
        .client()?;
    client.wait().await?;

    let mut coordination_client = client.coordination_client();

    let _ = coordination_client
        .drop_node("local/test".to_string())
        .await;

    coordination_client
        .create_node(
            "local/test".to_string(),
            NodeConfigBuilder::default().build()?,
        )
        .await?;

    let session = coordination_client
        .create_session(
            "local/test".to_string(),
            SessionOptionsBuilder::default().build()?,
        )
        .await?;

    session.create_semaphore("my-resource", 1, vec![]).await?;

    let mut handles: Vec<JoinHandle<()>> = vec![];
    for _ in 0..10 {
        let mut client = client.coordination_client();
        handles.push(tokio::spawn(async move {
            let session = client
                .create_session(
                    "local/test".to_string(),
                    SessionOptionsBuilder::default().build().unwrap(),
                )
                .await
                .unwrap();

            let session_alive_token = session.alive();
            tokio::select! {
                _ = session_alive_token.cancelled() => {},
                _ = mutex_work(session) => {},
            }
        }));
    }

    for result in futures_util::future::join_all(handles).await {
        result?;
    }

    Ok(())
}
