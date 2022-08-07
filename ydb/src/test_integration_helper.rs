use crate::client::Client;
use crate::client_builder::ClientBuilder;
use crate::errors::YdbResult;
use async_once::AsyncOnce;
use lazy_static::lazy_static;
use std::sync::Arc;
use tracing::trace;

lazy_static! {
    static ref TEST_CLIENT: AsyncOnce<Arc<Client>> = AsyncOnce::new(async {
        let client_builder: ClientBuilder =
            std::env::var("YDB_CONNECTION_STRING").unwrap_or_else(|_|
            "grpc://localhost:2136?database=/local".to_string()).parse().unwrap();

        trace!("create client");
        let client: Client = client_builder
            .client()
            .unwrap();

        trace!("start wait");
        client.wait().await.unwrap();
        Arc::new(client)
    });

    pub static ref TEST_TIMEOUT: i32 = {
        const DEFAULT_TIMEOUT_MS: i32 = 3600 * 1000; // a hour - for manual tests
        match std::env::var("TEST_TIMEOUT"){
            Ok(timeout)=>{
                if let Ok(timeout) = timeout.parse() {
                    timeout
                } else {
                    DEFAULT_TIMEOUT_MS
                }
            },
            Err(_)=>{
                DEFAULT_TIMEOUT_MS
            }
        }
    };
}

#[tracing::instrument]
pub(crate) async fn create_client() -> YdbResult<Arc<Client>> {
    trace!("create client");
    Ok(TEST_CLIENT.get().await.clone())
}
