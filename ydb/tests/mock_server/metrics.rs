use super::server::MockServer;
use ydb::{Client, ClientBuilder, YdbResult};

#[path = "../support/topic_prometheus.rs"]
mod topic_prometheus;
pub use topic_prometheus::TopicPrometheus as TestMetrics;

impl TestMetrics {
    pub async fn client(server: &MockServer) -> YdbResult<(Client, Self)> {
        const DRIVER: &str = "topic-test";
        let client = ClientBuilder::new_from_connection_string(format!(
            "{}/local?use_discovery=false",
            server.endpoint()
        ))?
        .with_driver_name(DRIVER)
        .build()
        .await?;
        Ok((client, Self::new(DRIVER)))
    }

    pub fn counter(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        self.sample(name, labels)
            .unwrap_or_else(|| panic!("missing counter {name}"))
    }
}
