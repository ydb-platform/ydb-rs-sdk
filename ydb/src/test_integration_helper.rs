use crate::client::Client;
use crate::client::TimeoutSettings;
use crate::errors::YdbResult;
use crate::test_helpers::test_custom_ca_client_builder;
use crate::test_helpers::{test_client_builder, test_with_password_builder};
use async_once::AsyncOnce;
use lazy_static::lazy_static;
use std::sync::Arc;

use tracing::trace;

lazy_static! {
    static ref TEST_CLIENT: AsyncOnce<Arc<Client>> = AsyncOnce::new(async {
        trace!("create client");
        connect().await.unwrap()
    });
}

#[tracing::instrument]
pub(crate) async fn create_client() -> YdbResult<Arc<Client>> {
    trace!("get client");
    // https://github.com/ydb-platform/ydb-rs-sdk/issues/92
    // return Ok(TEST_CLIENT.get().await.clone());
    connect().await
}

async fn connect() -> YdbResult<Arc<Client>> {
    let client = test_client_builder()
        .client()
        .unwrap()
        .with_timeouts(TimeoutSettings {
            operation_timeout: std::time::Duration::from_secs(60),
        });

    trace!("start wait");
    client.wait().await.unwrap();
    Ok(Arc::new(client))
}

#[tracing::instrument]
pub(crate) async fn create_password_client() -> YdbResult<Arc<Client>> {
    let client = test_with_password_builder().client().unwrap();
    trace!("start wait");
    client.wait().await.unwrap();
    Ok(Arc::new(client))
}

#[tracing::instrument]
pub(crate) async fn create_custom_ca_client() -> YdbResult<Arc<Client>> {
    let client = test_custom_ca_client_builder().client().unwrap().with_timeouts(TimeoutSettings {
        operation_timeout: std::time::Duration::from_secs(60),
    });
    trace!("start wait");
    client.wait().await.unwrap();
    Ok(Arc::new(client))
}
