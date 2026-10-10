//! Full reconnect scenario: build a fresh driver and swap it into the shared
//! slot, dropping the old one.
//!
//! There is no public `stop()`/`restart()` on `ydb::Client`, so a full
//! reconnect is drop + `ClientBuilder::build()`. The new driver is built
//! first and swapped in only on success, so a failed reconnect never leaves
//! the example without a client.
//!
//! Moves:
//! - `ydb_new_client_counter` on every rebuild;
//! - `ydb_grpc_connections{state="connecting"|"active"}` and
//!   `ydb_grpc_connection_establish_milliseconds{result="ok"}` for the new
//!   driver's pool entries;
//! - discovery RPCs in `ydb_grpc_requests_total{service="...DiscoveryService"}`;
//! - dropping the old driver shuts its session pool down:
//!   `ydb_session_pool_sessions_closed_total{reason="shutdown"}`.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::SharedClient;
use crate::build_client;
use crate::scenarios::sleep_jittered;

const MIN_INTERVAL: Duration = Duration::from_secs(60);
const MAX_INTERVAL: Duration = Duration::from_secs(120);

pub(crate) async fn run(
    connection_string: String,
    client: SharedClient,
    cancel: CancellationToken,
) {
    loop {
        if !sleep_jittered(&cancel, MIN_INTERVAL, MAX_INTERVAL).await {
            return;
        }

        match build_client(&connection_string).await {
            Ok(new_client) => {
                let mut guard = client.write().await;
                let old = std::mem::replace(&mut *guard, Arc::new(new_client));
                drop(guard);
                // Dropping the old driver shuts its session pool down.
                drop(old);
                tracing::debug!("client reconnected");
            }
            Err(err) => {
                warn!(?err, "reconnect failed; keeping the current client");
            }
        }
    }
}
