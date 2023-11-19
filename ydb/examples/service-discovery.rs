use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};
use ydb::{
    AcquireOptionsBuilder, ClientBuilder, CoordinationClient, DescribeOptionsBuilder, Lease,
    NodeConfigBuilder, SemaphoreDescription, Session, SessionEvent, SessionOptionsBuilder,
    WatchMode, YdbResult,
};

#[allow(dead_code)]
struct ServiceWorker {
    endpoint: String,
    worker_endpoints: Arc<Mutex<Vec<String>>>,
    session: Arc<Mutex<Session>>,
}

#[allow(dead_code)]
impl ServiceWorker {
    async fn new(endpoint: String, mut coordination_client: CoordinationClient) -> Self {
        let (sender, receiver) = mpsc::channel(1_usize);
        let session = coordination_client
            .create_session(
                // FIXME: to places where session constructed
                "local/test".to_string(),
                SessionOptionsBuilder::default()
                    .on_state_changed(sender.clone())
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();

        let session = Arc::new(Mutex::new(session));
        let session_handle = session.clone();

        // session renewer
        // TODO: move to sdk?
        let _ = tokio::spawn(async move {
            let sender = sender.clone();
            let mut receiver = receiver;
            loop {
                loop {
                    let event = receiver.recv().await.unwrap();
                    if let SessionEvent::Expired = event {
                        break;
                    }
                }

                let new_session = coordination_client
                    .create_session(
                        // FIXME: to places where session constructed
                        "local/test".to_string(),
                        SessionOptionsBuilder::default()
                            .on_state_changed(sender.clone())
                            .build()
                            .unwrap(),
                    )
                    .await
                    .unwrap();

                {
                    let mut session_lock = session_handle.lock().await;
                    *session_lock = new_session;
                }
            }
        });

        Self {
            endpoint,
            worker_endpoints: Arc::new(Mutex::new(vec![])),
            session: session.clone(),
        }
    }

    async fn get_workers(&mut self) -> Vec<String> {
        let workers_handle = self.worker_endpoints.lock().await;
        workers_handle.clone()
    }

    async fn run(&mut self) {
        let _lease: Lease;
        {
            let mut session = self.session.lock().await;
            _lease = session
                .acquire_semaphore(
                    "my-service-workers".to_string(),
                    ydb::AcquireCount::Single,
                    AcquireOptionsBuilder::default()
                        .data(self.endpoint.as_bytes().to_vec())
                        .build()
                        .unwrap(),
                )
                .await
                .unwrap()
                .unwrap();
        }

        loop {
            let (sender, mut receiver) = mpsc::channel(1_usize);

            let workers_description: SemaphoreDescription;
            {
                let mut session = self.session.lock().await;
                workers_description = session
                    .describe_semaphore(
                        "my-service-workers".to_string(),
                        DescribeOptionsBuilder::default()
                            .watch_mode(WatchMode::Owners)
                            .with_owners(true)
                            .on_changed(sender.clone())
                            .build()
                            .unwrap(),
                    )
                    .await
                    .unwrap();
            }

            {
                let mut workers_handle = self.worker_endpoints.lock().await;
                *workers_handle = workers_description
                    .owners
                    .iter()
                    .map(|owner| String::from_utf8(owner.data.clone()).unwrap())
                    .collect();
            }

            receiver.recv().await.unwrap();
        }
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

    let mut session = coordination_client
        .create_session(
            "local/test".to_string(),
            SessionOptionsBuilder::default().build()?,
        )
        .await?;

    session
        .create_semaphore(
            "my-service-workers".to_string(),
            ydb::SemaphoreLimit::Unbounded,
            None,
        )
        .await?;

    let client_1 = client.coordination_client();
    let client_2 = client.coordination_client();
    let client_3 = client.coordination_client();

    let workers = vec![
        tokio::spawn(async move {
            let mut worker = ServiceWorker::new("endpoint-1".to_string(), client_1).await;
            worker.run().await;
        }),
        tokio::spawn(async move {
            let mut worker = ServiceWorker::new("endpoint-2".to_string(), client_2).await;
            worker.run().await;
        }),
        tokio::spawn(async move {
            let mut worker = ServiceWorker::new("endpoint-3".to_string(), client_3).await;
            worker.run().await;
        }),
    ];

    for worker in workers {
        worker.await.unwrap();
    }

    Ok(())
}
