#![recursion_limit = "256"]

mod storage;
mod workload;

use slo_framework::run;

#[tokio::main]
async fn main() -> Result<(), String> {
    run(|fw| Box::pin(workload::new_workload(fw.clone()))).await
}
