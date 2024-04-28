use secrecy::ExposeSecret;
use tracing::trace;
use tracing_test::traced_test;

use crate::{
    credentials::StaticCredentials, pub_traits::Credentials, test_helpers::CONNECTION_STRING,
    test_integration_helper::{create_custom_ca_client, create_password_client}, Query, Transaction, YdbResult,
};

#[test]
#[traced_test]
#[ignore] // YDB access is necessary
fn auth_success_test() -> YdbResult<()> {
    let uri = http::uri::Uri::from_static(&(CONNECTION_STRING));

    let database = uri.path().to_string();
    let up_auth = StaticCredentials::new("root".to_string(), "1234".to_string(), uri, database, None);

    let token_sec = up_auth.create_token()?.token;
    let raw_token = token_sec.expose_secret();

    trace!("got token: `{}'", raw_token);
    if raw_token.is_empty() {
        panic!("got the empty token on the presumably successful auth request");
    }

    Ok(())
}

#[tokio::test]
#[traced_test]
#[ignore] // YDB access is necessary
async fn auth_async_success_test() -> YdbResult<()> {
    let uri = http::uri::Uri::from_static(&(CONNECTION_STRING));

    let database = uri.path().to_string();
    let up_auth = StaticCredentials::new("root".to_string(), "1234".to_string(), uri, database, None);

    let token_sec = std::thread::spawn(move || up_auth.create_token())
        .join()
        .unwrap()
        .unwrap()
        .token;
    let raw_token = token_sec.expose_secret();

    trace!("got token: `{}'", raw_token);
    if raw_token.is_empty() {
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
    let up_auth = StaticCredentials::new(
        "wr0n9_u$ern@me".to_string(),
        "1234".to_string(),
        uri,
        database,
        None,
    );

    up_auth.acquire_token().await.unwrap();
}

#[tokio::test]
#[traced_test]
#[should_panic]
#[ignore] // YDB access is necessary
async fn wrong_password_test() {
    let uri = http::uri::Uri::from_static(&(CONNECTION_STRING));
    let database = uri.path().to_string();
    let up_auth = StaticCredentials::new(
        "root".to_string(),
        "wr0n9_p@$$w0rd".to_string(),
        uri,
        database,
        None,
    );

    up_auth.acquire_token().await.unwrap();
}

#[tokio::test]
#[traced_test]
#[ignore] // YDB access is necessary
async fn password_client_test() -> YdbResult<()> {
    let client = create_password_client().await?;
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

#[tokio::test]
#[traced_test]
#[ignore] // YDB access is necessary
async fn custom_ca_test() -> YdbResult<()> {
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
