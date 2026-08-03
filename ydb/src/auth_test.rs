use secrecy::ExposeSecret;
use tracing::trace;
use tracing_test::traced_test;

use crate::{
    YdbResult,
    credentials::{CommandLineCredentials, StaticCredentials},
    pub_traits::Credentials,
    test_helpers::CONNECTION_STRING,
    test_integration_helper::create_password_client,
};

#[test]
#[traced_test]
#[ignore] // YDB access is necessary
fn auth_success_test() -> YdbResult<()> {
    let uri = http::uri::Uri::from_static(&(CONNECTION_STRING));

    let database = uri.path().to_string();
    let up_auth = StaticCredentials::new("root".to_string(), "1234".to_string(), uri, database);

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
    let up_auth = StaticCredentials::new("root".to_string(), "1234".to_string(), uri, database);

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
    );

    up_auth.acquire_token().await.unwrap();
}

#[tokio::test]
#[traced_test]
#[ignore] // YDB access is necessary
async fn password_client_test() -> YdbResult<()> {
    let client = create_password_client().await?;
    let mut row = client.query_client().query_row("SELECT 2").await?;
    let two: i32 = row.remove_field(0)?.try_into()?;

    assert_eq!(two, 2);
    Ok(())
}

/// `ExitStatus::code()` returns `None` when the child is terminated by a
/// signal. `CommandLineCredentials::create_token` used to unwrap it, so a
/// signal-killed helper panicked instead of returning an error.
#[test]
#[cfg(unix)]
fn command_line_credentials_signal_killed_command() -> YdbResult<()> {
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;

    let script_path =
        std::env::temp_dir().join(format!("ydb-selfkill-{}.sh", uuid::Uuid::new_v4()));

    let mut script = std::fs::File::create(&script_path)?;
    script.write_all(b"#!/bin/sh\nkill -9 $$\n")?;
    drop(script);
    std::fs::set_permissions(&script_path, std::fs::Permissions::from_mode(0o755))?;

    let cred = CommandLineCredentials::from_cmd(script_path.to_string_lossy().as_ref())?;
    let result = cred.create_token();

    std::fs::remove_file(&script_path)?;

    let err = result.expect_err("a signal-killed command must not produce a token");
    assert!(
        err.to_string().contains("can't execute"),
        "unexpected error: {err}"
    );

    Ok(())
}
