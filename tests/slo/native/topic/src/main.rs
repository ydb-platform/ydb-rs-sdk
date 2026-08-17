#![recursion_limit = "256"]
mod storage;

use slo_framework::{install_ring_crypto_provider, run};

#[tokio::main]
async fn main() -> Result<(), String> {
    install_ring_crypto_provider();
    run(|fw| Box::pin(storage::new_workload(fw.clone()))).await
}
