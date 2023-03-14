use tracing_test::traced_test;

use crate::test_integration_helper::create_client;
use crate::{client_topic::client::TopicOptionsBuilder, YdbResult};

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn create_delete_topic_test() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let topic_name = "test_topic".to_string();
    let topic_path = format!("{}/{}", database_path, topic_name);

    let mut topic_client = client.topic_client();
    let mut scheme_client = client.scheme_client();

    let _ = topic_client.drop_topic(topic_path.clone()).await; // ignoring error

    topic_client
        .create_topic(topic_path.clone(), TopicOptionsBuilder::default().build()?)
        .await?;
    let directories_after_topic_creation =
        scheme_client.list_directory(database_path.clone()).await?;
    assert!(directories_after_topic_creation
        .iter()
        .any(|d| d.name == topic_name));

    topic_client.drop_topic(topic_path).await?;
    let directories_after_topic_droppage = scheme_client.list_directory(database_path).await?;
    assert!(!directories_after_topic_droppage
        .iter()
        .any(|d| d.name == topic_name));

    Ok(())
}
