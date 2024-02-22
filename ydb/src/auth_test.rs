use tracing::trace;
use tracing_test::traced_test;

use crate::{
    test_helpers::CONNECTION_STRING,
    test_integration_helper::create_password_client,
    Query,
    YdbResult,
    Transaction,
    credentials::UserPasswordAuth
};

#[tokio::test]
#[traced_test]
#[ignore] // YDB access is necessary
async fn auth_success_test() -> YdbResult<()> {
    let uri = http::uri::Uri::from_static(&(CONNECTION_STRING));

    let database = uri.path().to_string();
    let up_auth = UserPasswordAuth::new(
        "root".to_string(),
        "1234".to_string(),
        uri, database);

    let token_str = up_auth.acquire_token().await?;

    trace!("got token: `{}'", token_str);
    if token_str.is_empty() {
        panic!("got the empty token on the presumably successful auth request");
    }

    Ok(())
}

#[tokio::test]
#[traced_test]
#[should_panic]
#[ignore] // YDB access is necessary
async fn wrong_username_test() {
    let uri = http::uri::Uri::from_static(&(CONNECTION_STRING));
    let database = uri.path().to_string();
    let up_auth = UserPasswordAuth::new(
        "wr0n9_u$ern@me".to_string(),
        "1234".to_string(),
        uri, database);

    up_auth.acquire_token().await.unwrap();
}

#[tokio::test]
#[traced_test]
#[should_panic]
#[ignore] // YDB access is necessary
async fn wrong_password_test() {
    let uri = http::uri::Uri::from_static(&(CONNECTION_STRING));
    let database = uri.path().to_string();
    let up_auth = UserPasswordAuth::new(
        "root".to_string(),
        "wr0n9_p@$$w0rd".to_string(),
        uri, database);

    up_auth.acquire_token().await.unwrap();
}

#[tokio::test]
#[traced_test]
#[ignore] // YDB access is necessary
async fn password_client_test() -> YdbResult<()> {
    let client = create_password_client().await?;

    let sum: i32 = client
    .table_client() // create table client
    .retry_transaction(|mut t: Box<dyn Transaction>| async move {
        // send the query to the database
        let res = t.query(Query::from("SELECT 17 + 25 AS sum")).await?;

        // read exactly one result from the db
        let field_val: i32 = res.into_only_row()?.remove_field_by_name("sum")?.try_into()?;

        // return result
        return Ok(field_val);
    })
    .await?;

    assert_eq!(sum, 42);

    Ok(())
}
