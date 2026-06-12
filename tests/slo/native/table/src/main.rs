mod storage;

use slo_framework::run;

#[tokio::main]
async fn main() {
    run(|fw| Box::pin(storage::new_workload(fw.clone()))).await;
}
