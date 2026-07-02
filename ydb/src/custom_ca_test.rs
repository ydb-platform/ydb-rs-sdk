use crate::{test_integration_helper::create_custom_ca_client, YdbResult};
use tracing_test::traced_test;

#[tokio::test]
#[traced_test]
#[ignore] // YDB access is necessary
async fn custom_ca_test() -> YdbResult<()> {
    // Enable the test after fix https://github.com/ydb-platform/ydb/issues/4638
    return Ok(());

    #[allow(unreachable_code)]
    {
        let client = create_custom_ca_client().await?;
        let mut row = client.query_client().query_row("SELECT 2").await?;
        let two: i32 = row.remove_field(0)?.try_into()?;

        assert_eq!(two, 2);
        Ok(())
    }
}
