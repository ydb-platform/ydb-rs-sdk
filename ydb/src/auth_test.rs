use secrecy::ExposeSecret;
use tracing::trace;
use tracing_test::traced_test;

use crate::{
    YdbResult,
    credentials::{
        AnonymousCredentials, CommandLineCredentials, GCEMetadata, MetadataUrlCredentials,
        ServiceAccountCredentials, StaticCredentials, unix_timestamp,
    },
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

/// Write an executable shell script into a temp dir and wrap it into
/// `CommandLineCredentials`.
///
/// `from_cmd` splits the command on whitespace, so the helper has to be a
/// single path with no arguments - hence a script file rather than `sh -c`.
#[cfg(unix)]
struct ScriptCommand {
    path: std::path::PathBuf,
}

#[cfg(unix)]
impl ScriptCommand {
    fn new(body: &str) -> YdbResult<Self> {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;

        let path = std::env::temp_dir().join(format!("ydb-cred-{}.sh", uuid::Uuid::new_v4()));
        let mut file = std::fs::File::create(&path)?;
        write!(file, "#!/bin/sh\n{body}\n")?;
        drop(file);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))?;

        Ok(Self { path })
    }

    fn credentials(&self) -> YdbResult<CommandLineCredentials> {
        CommandLineCredentials::from_cmd(self.path.to_string_lossy().as_ref())
    }
}

#[cfg(unix)]
impl Drop for ScriptCommand {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// `ExitStatus::code()` returns `None` when the child is terminated by a
/// signal. `CommandLineCredentials::create_token` used to unwrap it, so a
/// signal-killed helper panicked instead of returning an error.
#[test]
#[cfg(unix)]
fn command_line_credentials_signal_killed_command() -> YdbResult<()> {
    let script = ScriptCommand::new("kill -9 $$")?;

    let err = script
        .credentials()?
        .create_token()
        .expect_err("a signal-killed command must not produce a token");

    assert!(
        err.to_string().contains("can't execute"),
        "unexpected error: {err}"
    );

    Ok(())
}

/// The same error path for an ordinary non-zero exit: the message must carry
/// the status and whatever the helper wrote to stderr.
#[test]
#[cfg(unix)]
fn command_line_credentials_failed_command_reports_stderr() -> YdbResult<()> {
    let script = ScriptCommand::new("echo 'no active profile' >&2\nexit 3")?;

    let err = script
        .credentials()?
        .create_token()
        .expect_err("a failing command must not produce a token");
    let message = err.to_string();

    assert!(message.contains("can't execute"), "unexpected: {message}");
    assert!(message.contains('3'), "status missing from: {message}");
    assert!(
        message.contains("no active profile"),
        "stderr missing from: {message}"
    );

    Ok(())
}

#[test]
#[cfg(unix)]
fn command_line_credentials_returns_trimmed_token() -> YdbResult<()> {
    let script = ScriptCommand::new("echo '  t0ken  '")?;

    let token = script.credentials()?.create_token()?;

    assert_eq!(token.token.expose_secret(), "t0ken");

    Ok(())
}

/// `debug_string` must describe the token, never print it.
#[test]
#[cfg(unix)]
fn command_line_credentials_debug_string_hides_token() -> YdbResult<()> {
    let long = "0123456789abcdefghijklmnopqrstuvwxyz";
    let script = ScriptCommand::new(&format!("echo '{long}'"))?;

    let description = script.credentials()?.debug_string();

    assert!(
        !description.contains(long),
        "token leaked into: {description}"
    );
    assert_eq!(description, "012..xyz");

    Ok(())
}

#[test]
fn command_line_credentials_rejects_empty_command() {
    let err = CommandLineCredentials::from_cmd("   ")
        .expect_err("an empty command line must be rejected");

    assert!(
        err.to_string().contains("can't split get token command"),
        "unexpected error: {err}"
    );
}

/// `GCEMetadata::new` and `MetadataUrlCredentials::new` build from a
/// compile-time constant. They used to `unwrap` an infallible parse; make sure
/// they still produce the documented endpoints and cannot panic.
#[test]
fn gce_metadata_new_uses_google_metadata_url() {
    let description = GCEMetadata::new().debug_string();

    assert!(
        description.contains(
            "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
        ),
        "unexpected endpoint: {description}"
    );
}

#[test]
fn gce_metadata_from_url_keeps_custom_url() -> YdbResult<()> {
    let description = GCEMetadata::from_url("http://127.0.0.1:8080/token")?.debug_string();

    assert!(
        description.contains("http://127.0.0.1:8080/token"),
        "unexpected endpoint: {description}"
    );

    Ok(())
}

#[test]
fn metadata_url_credentials_construct_without_panic() {
    // Both constructors go through the same infallible path; neither performs
    // any I/O, so this only pins that building them never panics.
    let _ = MetadataUrlCredentials::new();
    let _ = MetadataUrlCredentials::default();
}

/// The JWT timestamp used to be an `expect("Time went backwards")`. Both
/// branches are now reachable, so both are checked.
#[test]
fn unix_timestamp_counts_seconds_from_the_epoch() -> YdbResult<()> {
    use std::time::{Duration, UNIX_EPOCH};

    assert_eq!(unix_timestamp(UNIX_EPOCH)?, 0);
    assert_eq!(
        unix_timestamp(UNIX_EPOCH + Duration::from_secs(1_700_000_000))?,
        1_700_000_000
    );

    Ok(())
}

#[test]
fn unix_timestamp_rejects_clock_before_the_epoch() {
    use std::time::{Duration, UNIX_EPOCH};

    let err = unix_timestamp(UNIX_EPOCH - Duration::from_secs(1))
        .expect_err("a pre-epoch clock must be reported as an error");

    assert!(
        err.to_string()
            .contains("system clock is set before the UNIX epoch"),
        "unexpected error: {err}"
    );
}

/// `build_jwt` computes `iat` before touching the key, so an unusable key
/// still exercises that path and then fails on the key itself.
#[test]
fn service_account_credentials_reject_unusable_private_key() {
    let cred = ServiceAccountCredentials::new("service-account-id", "key-id", "not-a-pem-key");

    let err = cred
        .create_token()
        .expect_err("an unusable private key must not produce a token");

    assert!(
        !err.to_string().is_empty(),
        "error message must explain the failure"
    );
}

#[test]
fn anonymous_credentials_produce_empty_token() -> YdbResult<()> {
    let token = AnonymousCredentials::new().create_token()?;

    assert_eq!(token.token.expose_secret(), "");

    Ok(())
}
/// `acquire_token` used to `unwrap` the `get_auth_service` result. Point it at
/// a closed loopback port: the failure must surface as an error, not a panic.
/// Loopback refuses immediately, so this needs no network and no YDB.
#[test]
fn static_credentials_unreachable_endpoint_reports_error() {
    let cred = StaticCredentials::new(
        "root".to_string(),
        "1234".to_string(),
        http::uri::Uri::from_static("grpc://127.0.0.1:1/local"),
        "/local".to_string(),
    );

    let err = cred
        .create_token()
        .expect_err("an unreachable endpoint must not produce a token");

    assert!(
        matches!(
            err,
            crate::YdbError::TransportGRPCStatus(_)
                | crate::YdbError::Transport(_)
                | crate::YdbError::TransportDial(_)
        ),
        "expected a transport error, got: {err:?}"
    );
}
