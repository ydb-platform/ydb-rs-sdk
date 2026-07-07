use std::time::Duration;
use tokio::time::timeout;
use ydb::{ClientBuilder, HasGrpcOptions, YdbError, YdbResult};

#[tokio::main]
async fn main() -> YdbResult<()> {
    // very verbose logs
    tracing_subscriber::fmt()
        // enable everything
        .with_max_level(tracing::Level::TRACE)
        // sets this to be the default, global collector for this application.
        .init();

    let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?
        .try_with_grpc_opts(|opts| {
            opts.keepalive_interval(Duration::from_secs(3))
                .max_message_size(100500)
                .load_certificate("./cert.pem")
        })?
        .with_grpc_opts(|opts| {
            opts.keepalive_interval(Duration::from_secs(3))
                .max_message_size(100500)
        })
        .client()?;

    if let Ok(res) = timeout(Duration::from_secs(3), client.wait()).await {
        res?
    } else {
        return Err(YdbError::from("Connection timeout"));
    };

    println!("done");
    Ok(())
}
