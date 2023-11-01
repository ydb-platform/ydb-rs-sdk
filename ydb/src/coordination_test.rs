use tracing_test::traced_test;

use crate::{
    client_coordination::list_types::{
        ConsistencyMode, NodeConfigBuilder, RateLimiterCountersMode,
    },
    test_integration_helper::create_client,
    YdbResult,
};

#[tokio::test]
#[traced_test]
#[ignore] // need YDB access
async fn create_delete_node_test() -> YdbResult<()> {
    let client = create_client().await?;
    let database_path = client.database();
    let node_name = "test_node".to_string();
    let node_path = format!("{}/{}", database_path, node_name);

    let mut coordination_client = client.coordination_client();

    let _ = coordination_client.drop_node(node_path.clone()).await; // ignore error

    coordination_client
        .create_node(node_path.clone(), NodeConfigBuilder::default().build()?)
        .await?;

    let node_desc = coordination_client.describe_node(node_path.clone()).await?;
    assert_eq!(node_desc.config.self_check_period_millis, 0);
    assert_eq!(node_desc.config.session_grace_period_millis, 0);
    assert!(matches!(node_desc.config.read_consistency_mode, None));
    assert!(matches!(node_desc.config.attach_consistency_mode, None));
    assert!(matches!(node_desc.config.rate_limiter_counters_mode, None));

    coordination_client
        .alter_node(
            node_path.clone(),
            NodeConfigBuilder::default()
                .self_check_period_millis(1)
                .session_grace_period_millis(2)
                .read_consistency_mode(Some(ConsistencyMode::Strict))
                .rate_limiter_counters_mode(Some(RateLimiterCountersMode::Aggregated))
                .build()?,
        )
        .await?;

    let node_desc = coordination_client.describe_node(node_path.clone()).await?;
    assert_eq!(node_desc.config.self_check_period_millis, 1);
    assert_eq!(node_desc.config.session_grace_period_millis, 2);
    assert!(matches!(
        node_desc.config.read_consistency_mode,
        Some(ConsistencyMode::Strict)
    ));
    assert!(matches!(node_desc.config.attach_consistency_mode, None));
    assert!(matches!(
        node_desc.config.rate_limiter_counters_mode,
        Some(RateLimiterCountersMode::Aggregated)
    ));

    coordination_client.drop_node(node_path.clone()).await?;

    Ok(())
}
