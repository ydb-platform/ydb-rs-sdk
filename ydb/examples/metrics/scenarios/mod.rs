//! Independent scenario loops, each driving a subset of the SDK metric series.

pub(crate) mod query;
pub(crate) mod reconnect;
pub(crate) mod session_pool;
pub(crate) mod topic;

use std::time::Duration;

use rand::Rng;
use tokio_util::sync::CancellationToken;

/// Sleep for a randomized interval in `[min, max]`. Returns `false` if the
/// cancellation token fired while sleeping — the caller should exit its loop.
pub(crate) async fn sleep_jittered(
    cancel: &CancellationToken,
    min: Duration,
    max: Duration,
) -> bool {
    let secs = rand::thread_rng().gen_range(min.as_secs()..=max.as_secs());
    tokio::select! {
        _ = cancel.cancelled() => false,
        _ = tokio::time::sleep(Duration::from_secs(secs)) => true,
    }
}
