use std::{sync::Arc, time::Duration};

use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use ydb::{
    AcquireOptionsBuilder, ClientBuilder, CoordinationClient, CoordinationSession,
    DescribeOptionsBuilder, NodeConfigBuilder, SessionOptionsBuilder, WatchMode,
    WatchOptionsBuilder, YdbResult,
};

#[allow(dead_code)]
struct ServiceWorker {
    endpoint: String,
    coordination: CoordinationClient,
    leader_endpoint: Arc<Mutex<Option<String>>>,
    explode_token: CancellationToken,
}

#[allow(dead_code)]
impl ServiceWorker {
    fn new(endpoint: String, coordination: CoordinationClient) -> Self {
        Self {
            endpoint,
            coordination,
            leader_endpoint: Arc::new(Mutex::new(None)),
            explode_token: CancellationToken::new(),
        }
    }

    async fn get_leader(&self) -> Option<String> {
        let leader_handle = self.leader_endpoint.lock().await;
        leader_handle.clone()
    }

    fn explode(&self) {
        self.explode_token.cancel();
    }

    async fn become_leader(&self) {
        let mut leader_handle = self.leader_endpoint.lock().await;
        *leader_handle = Some(self.endpoint.clone());

        tokio::time::sleep(tokio::time::Duration::from_secs(100)).await;
    }

    async fn become_secondary(&self, session: &mut CoordinationSession) {
        let mut subscription = session
            .watch_semaphore(
                "my-service-leader".to_string(),
                WatchOptionsBuilder::default()
                    .watch_mode(WatchMode::Owners)
                    .describe_options(
                        DescribeOptionsBuilder::default()
                            .with_owners(true)
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();

        loop {
            let leader_description = subscription.recv().await;
            if let None = leader_description {
                break;
            }

            match leader_description.unwrap().owners.first() {
                Some(owner) => {
                    let mut leader_handle = self.leader_endpoint.lock().await;
                    *leader_handle = Some(String::from_utf8(owner.data.clone()).unwrap());
                }
                None => {
                    // try reacquire
                    break;
                }
            }
        }
    }

    async fn do_work(&self, mut session: CoordinationSession) {
        loop {
            let lease = session
                .acquire_semaphore(
                    "my-service-leader".to_string(),
                    ydb::AcquireCount::Single,
                    AcquireOptionsBuilder::default()
                        .data(self.endpoint.as_bytes().to_vec())
                        // try acquire
                        .timeout(Duration::ZERO)
                        .build()
                        .unwrap(),
                )
                .await;

            match lease {
                Ok(lease) => {
                    let lease_alive = lease.alive();
                    tokio::select! {
                        _ = lease_alive.cancelled() => {},
                        _ = self.become_leader() => {},
                    }
                }
                Err(_) => {
                    self.become_secondary(&mut session).await;
                }
            }
        }
    }

    async fn run(&self, session: CoordinationSession) {
        let session_alive_token = session.alive();
        let explode_token = self.explode_token.clone();
        tokio::select! {
            _ = session_alive_token.cancelled() => {},
            _ = explode_token.cancelled() => {},
            _ = self.do_work(session) => {},
        }
    }
}

async fn explode_leader(workers: &Vec<Arc<ServiceWorker>>) {
    let leader_1 = workers[0].get_leader().await;
    let leader_2 = workers[1].get_leader().await;
    let leader_3 = workers[2].get_leader().await;

    assert_eq!(leader_1, leader_2);
    assert_eq!(leader_2, leader_3);

    match leader_1.unwrap().as_str() {
        "endpoint-1" => workers[0].explode(),
        "endpoint-2" => workers[1].explode(),
        "endpoint-3" => workers[2].explode(),
        _ => unreachable!("bad leader"),
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
        .await?;

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

    session
        .create_semaphore(
            "my-service-leader".to_string(),
            ydb::SemaphoreLimit::Mutex,
            None,
        )
        .await?;

    let workers = vec![
        Arc::new(ServiceWorker::new(
            "endpoint-1".to_string(),
            client.coordination_client(),
        )),
        Arc::new(ServiceWorker::new(
            "endpoint-2".to_string(),
            client.coordination_client(),
        )),
        Arc::new(ServiceWorker::new(
            "endpoint-3".to_string(),
            client.coordination_client(),
        )),
    ];

    let mut handles: Vec<JoinHandle<()>> = vec![];
    for worker in workers.iter() {
        let worker_ref = worker.clone();
        let worker_session = coordination_client
            .create_session(
                "local/test".to_string(),
                SessionOptionsBuilder::default().build()?,
            )
            .await?;

        handles.push(tokio::spawn(async move {
            worker_ref.run(worker_session).await;
        }))
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    explode_leader(&workers).await;

    futures_util::future::join_all(handles).await;

    Ok(())
}
