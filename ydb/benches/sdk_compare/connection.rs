use std::env;

use anyhow::{Context, Result};
use ydb::{Client, ClientBuilder, SessionPoolSettings};

const CONNECTION_STRING_ENV: &str = "YDB_CONNECTION_STRING";

#[bon::builder]
pub(crate) async fn connect(session_pool_size: Option<usize>) -> Result<Client> {
    let connection_string = env::var(CONNECTION_STRING_ENV)
        .with_context(|| format!("{CONNECTION_STRING_ENV} is not set"))?;

    let client = ClientBuilder::new_from_connection_string(&connection_string)
        .context("failed to parse YDB connection string")?
        .client()
        .context("failed to create YDB client")?;

    client
        .wait()
        .await
        .context("failed to initialize YDB client")?;

    let Some(session_pool_size) = session_pool_size else {
        return Ok(client);
    };

    client
        .with_session_pool(
            SessionPoolSettings::new()
                .with_limit(session_pool_size)
                .with_warm_up(session_pool_size),
        )
        .await
        .context("failed to configure YDB session pool")
}
