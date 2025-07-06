use std::process::exit;
use std::time::Duration;
use tokio::time::timeout;
use tracing::log;

mod db;
mod ui;

#[tokio::main]
async fn main() {
    // very verbose logs
    tracing_subscriber::fmt()
        // enable everything
        .with_max_level(tracing::Level::DEBUG)
        // sets this to be the default, global collector for this application.
        .init();

    let db = match timeout(Duration::from_secs(3), db::init_db()).await {
        Ok(Ok(db)) => db,
        Ok(Err(err)) => {
            log::error!("Can't connect to ydb: {}", err);
            exit(1)
        }
        Err(err) => {
            log::error!("Can't connect to ydb by timeout: {}", err);
            exit(1)
        }
    };

    if let Err(err) = ui::run(db.table_client()).await {
        println!("Failed to start http server: {}", &err);
        exit(1)
    }
}
