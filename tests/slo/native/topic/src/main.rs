mod storage;

use slo_framework::run;

#[tokio::main]
async fn main() -> Result<(), String> {
    run(|fw| Box::pin(storage::new_workload(fw.clone()))).await
}
