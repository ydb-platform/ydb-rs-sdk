use tracing_test::traced_test;

use crate::YdbResult;

#[tokio::test]
#[traced_test]
async fn auth_test() -> YdbResult<()> {
    Ok(())
}
