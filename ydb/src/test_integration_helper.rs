use crate::client::Client;
use crate::client::TimeoutSettings;
use crate::errors::YdbResult;
use crate::test_helpers::test_client_builder;
use async_once::AsyncOnce;
use lazy_static::lazy_static;
use std::sync::Arc;
use tracing::trace;

lazy_static! {
    static ref TEST_CLIENT: AsyncOnce<Arc<Client>> = AsyncOnce::new(async {
        trace!("create client");
        let client: Client =
            test_client_builder()
                .client()
                .unwrap()
                .with_timeouts(TimeoutSettings {
                    operation_timeout: std::time::Duration::from_secs(60),
                });

        trace!("start wait");
        client.wait().await.unwrap();
        Arc::new(client)
    });
}

#[tracing::instrument]
pub(crate) async fn create_client() -> YdbResult<Arc<Client>> {
    trace!("create client");
    Ok(TEST_CLIENT.get().await.clone())
}
