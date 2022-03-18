use std::process::exit;

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

    if let Err(err) = db::check().await {
        println!("Failed ydb init: {}", &err);
        exit(1)
    };

    if let Err(err) = ui::run().await {
        println!("Failed to start http server: {}", &err);
        exit(1)
    }

    return;
}
