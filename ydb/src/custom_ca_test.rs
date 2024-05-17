use tracing_test::traced_test;
use crate::{
    test_integration_helper::create_custom_ca_client,
    Query,
    Transaction,
    YdbResult,
};

#[tokio::test]
#[traced_test]
#[ignore] // YDB access is necessary
async fn custom_ca_test() -> YdbResult<()> {
    // Enable the test after fix https://github.com/ydb-platform/ydb/issues/4638
    return Ok(());

    #[allow(unreachable_code)]
    {
        let client = create_custom_ca_client().await?;
        let two: i32 = client
            .table_client() // create table client
            .retry_transaction(|mut t: Box<dyn Transaction>| async move {
                let res = t.query(Query::from("SELECT 2")).await?;
                let field_val: i32 = res.into_only_row()?.remove_field(0)?.try_into()?;
                Ok(field_val)
            })
            .await?;

        assert_eq!(two, 2);
        Ok(())
    }
}
