use std::process::Output;

use secrecy::ExposeSecret;
use tracing::trace;
use tracing_test::traced_test;

use crate::{
    YdbResult,
    credentials::{
        AccessTokenCredentials, AnonymousCredentials, CommandLineCredentials, GCEMetadata,
        MetadataUrlCredentials, StaticCredentials, unix_timestamp,
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

/// `create_token` spawns the helper and hands its result to
/// `token_from_output`. The tests below drive `token_from_output` directly, so
/// every exit status - including the ones a real command cannot be asked for
/// reliably - is covered without an external script.
fn command_output(status: std::process::ExitStatus, stdout: &str, stderr: &str) -> Output {
    Output {
        status,
        stdout: stdout.as_bytes().to_vec(),
        stderr: stderr.as_bytes().to_vec(),
    }
}

/// Raw wait status of a process killed by SIGKILL: no exit code, only the
/// signal number in the low bits.
#[cfg(unix)]
const SIGKILL_WAIT_STATUS: i32 = 9;

/// Raw wait status of a process that exited with code 3: unix keeps the exit
/// code in the high byte.
#[cfg(unix)]
const EXIT_CODE_3_WAIT_STATUS: i32 = 3 << 8;

/// `ExitStatus::code()` returns `None` when the child is terminated by a
/// signal. `CommandLineCredentials::create_token` used to unwrap it, so a
/// signal-killed helper panicked instead of returning an error.
#[test]
#[cfg(unix)] // raw wait statuses are encoded per platform
fn command_line_credentials_signal_killed_command() {
    use std::os::unix::process::ExitStatusExt;

    let killed = std::process::ExitStatus::from_raw(SIGKILL_WAIT_STATUS);
    assert!(
        killed.code().is_none(),
        "a signal-killed status must carry no exit code"
    );

    let err = CommandLineCredentials::token_from_output(command_output(killed, "", ""))
        .expect_err("a signal-killed command must not produce a token");

    assert!(
        err.to_string().contains("can't execute"),
        "unexpected error: {err}"
    );
}

/// The same error path for an ordinary non-zero exit: the message must carry
/// the status and whatever the helper wrote to stderr.
#[test]
#[cfg(unix)] // raw wait statuses are encoded per platform
fn command_line_credentials_failed_command_reports_stderr() {
    use std::os::unix::process::ExitStatusExt;

    let failed = std::process::ExitStatus::from_raw(EXIT_CODE_3_WAIT_STATUS);
    let err = CommandLineCredentials::token_from_output(command_output(
        failed,
        "",
        "no active profile\n",
    ))
    .expect_err("a failing command must not produce a token");
    let message = err.to_string();

    assert!(message.contains("can't execute"), "unexpected: {message}");
    assert!(message.contains('3'), "status missing from: {message}");
    assert!(
        message.contains("no active profile"),
        "stderr missing from: {message}"
    );
}

#[test]
fn command_line_credentials_returns_trimmed_token() -> YdbResult<()> {
    let succeeded = std::process::ExitStatus::default();

    let token =
        CommandLineCredentials::token_from_output(command_output(succeeded, "  t0ken  \n", ""))?;

    assert_eq!(token.token.expose_secret(), "t0ken");

    Ok(())
}

/// `debug_string` must describe the token, never print it.
#[test]
fn command_line_credentials_description_hides_token() {
    let long = "0123456789abcdefghijklmnopqrstuvwxyz";

    let description = CommandLineCredentials::describe_token(long);

    assert!(
        !description.contains(long),
        "token leaked into: {description}"
    );
    assert_eq!(description, "012..xyz");
}

/// A token short enough to be recognizable is not described at all.
#[test]
fn command_line_credentials_description_hides_short_token() {
    let short = "t0ken";

    let description = CommandLineCredentials::describe_token(short);

    assert!(
        !description.contains(short),
        "token leaked into: {description}"
    );
    assert_eq!(description, "short_token");
}

/// The command is arbitrary, so its output may contain multi-byte characters.
/// Describing such a token must not panic on a byte index that falls inside a
/// character.
#[test]
fn command_line_credentials_description_handles_multibyte_token() {
    // 24 characters, 48 bytes: long enough to be truncated, and every cut
    // point of the byte-based implementation lands inside a character.
    let token = "абвгдежзийклмнопрстуфхцч";

    let description = CommandLineCredentials::describe_token(token);

    assert!(
        !description.contains(token),
        "token leaked into: {description}"
    );
    assert_eq!(description, "абв..хцч");
}

/// A token that is short in characters but long in bytes must still be treated
/// as short.
#[test]
fn command_line_credentials_description_counts_characters_not_bytes() {
    let token = "абвгдежзийк"; // 11 characters, 22 bytes

    let description = CommandLineCredentials::describe_token(token);

    assert_eq!(description, "short_token");
}

/// `AccessTokenCredentials` describes its token through the same helper, so
/// the multi-byte token must not panic there either.
#[test]
fn access_token_credentials_description_hides_multibyte_token() {
    let token = "абвгдежзийклмнопрстуфхцч"; // 24 characters, 48 bytes

    let description = AccessTokenCredentials::from(token).debug_string();

    assert!(
        !description.contains(token),
        "token leaked into: {description}"
    );
    assert_eq!(description, "static token: абв...хцч");
}

#[test]
fn access_token_credentials_description_hides_short_token() {
    let description = AccessTokenCredentials::from("абвгдежзийк").debug_string();

    assert_eq!(description, "static token: xxx...xxx");
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

/// A throwaway RSA key, generated for this test and used nowhere else, so the
/// signing path can run to completion instead of failing on the key.
#[cfg(feature = "service-account-credentials")]
const TEST_PRIVATE_KEY: &str = "\
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCso2/kc+4L/Ss5
mQQV9F2p9131jjCHNd3bB3xHkOGt7052BECwEsxcB9qfsY5zB8oSxI3yRQ96jZKo
rU1oVeBu43wtVOD85flslUaGRNqaIt2D6jXgCurlaQj7X7qtnKCf9B+BELaMRHj6
F0SDe7FRiPXaqx3wkd+ObAcK2PBB87b2WyYFOTDtO62/DFuO+AyLwlRjOtaUSe2z
9zKxxV306d5eIB0nOsZilt87DRW9bRuv3JCSOpUUW/rYmjASVkceG3SygxuXTlgr
sDCu8pgc9zVZtLynnSRnw9H7WIx3yINpH6I9LLr4uvknG47qVd4wf4ik4AK+Kqzp
GZZZbDX9AgMBAAECggEAAtoyCRMMeQPWNcqcN98K64bIwjvdLv622BDH/uHn69j3
8bJt4/rYVLtUfPr3Olm2/v38XzP0Geqo5HYojabAx7qk3JYTn/On97rnqJd1CfYb
6G5zZ9MGWoMuJzXhS09t+ZAtzRLnngQSAGyaymLxMdwSo9BCW5OT0mVg7TVaxg9a
+dAArk4/JAg9uCIRL7Iy2VhY/C3+I6Z6G6KlSY5DtZ4pcsnP/rdr5bfcoHl1mSxH
D4aR/CaVYRf8p5pvwlTeYQ4HwEVYU2rrJWOB12mMIagiN2LvTcPD8iH9Gjs0oisa
b0mZv48iJ9WJnidvmbe0Wuw21j+8fyTVR4jIoohAwQKBgQDuOQBP/5cxO9EGD94E
1piwCiCslRDgHaF92h+0QxCY4+eOTDs6yigUpFGvdPSwpBCuD3MaqXTzdZhi7HYq
xjHQOt9G1mqdHQxlCkHXlffnJmf4TwPJYidOiTjkLpPRkIwaLuoAG/KRXs/b5obE
r1jtqXCQyzIWIbxZSkv+nqCWCQKBgQC5hYQ8RgEsEyicDu+cYSK1OdqaWKqYvB2S
P2unzmwHxA9M6eBxgvG7yCmV1FrcM65FMoPFpnBYmBrYnEFMhBLlRLvw+vMv5ias
yu89RA7xRP1Y9Wyya5pQLAK9hHHG95BveJ37gfPewc4FlpRgCipQ1x5wmc3OnQ/w
zYFslBh9VQKBgQCXwE/mphaWEliw1+LCikFxwsHVro+1dKqvFQglOKOWAXg8OWtx
o92saJBwiLxm2dyWRNk6HsCVZk/QlHkh/mc1EouxAF2iACMpKkOPJcVpxhltIEh1
PMV1TgyjrtgTI1VcSI5fy23WFOOsXUl0eY7YtpsyYX5igtMMvjty03HQoQKBgGjp
dTc0i1MmXm+qDpxLXfpIBjQEciy/LKXYkYOquvLFT7W6CH+IgjUj40hkdyFRZHqj
L1tlXODs2zmlW1Pu2FSowvNqpn1u1G4gsJvf0edkkTRuTZK0QOaXjNEphcHzm809
03E4MvXEeUI1viB1fyqKDB+3CmiN70bIoCkLsrGlAoGBAKDatV/ZFE/F4LWfZ2rF
8V1GMIV26gHdxbk7QPURsgtWF6LkUqWu4aMeUW3qtfEBSldS4XQ2TcQNS/+VE9YN
TQHygyP7e85dvWBSvC9yMlPv5bXYqGaccKmCzpV/HUam7wElbxpzUmzR9PeoP0Vh
ymXXV/34RqqRRzkruEw7yotw
-----END PRIVATE KEY-----";

/// Signing has to reach the crypto backend, which the key-rejection test below
/// never does: it fails while parsing the PEM, before a provider is consulted.
///
/// `jsonwebtoken` 11 resolves its provider from its own features and installs a
/// placeholder that panics when neither `rust_crypto` nor `aws_lc_rs` is
/// enabled, or when both are. A crate that selects no backend therefore builds,
/// passes every existing test, and panics the first time a real service account
/// signs. This is the test that notices.
///
/// `build_jwt` rather than `create_token`, so nothing but the signature is
/// exercised: the exchange that follows needs a listening IAM endpoint.
#[cfg(feature = "service-account-credentials")]
#[test]
fn service_account_credentials_sign_a_jwt_with_a_usable_private_key() {
    use crate::credentials::ServiceAccountCredentials;

    let cred = ServiceAccountCredentials::new("service-account-id", "key-id", TEST_PRIVATE_KEY);

    let jwt = cred.build_jwt().expect("a usable key signs a JWT");

    assert_eq!(
        jwt.split('.').count(),
        3,
        "a signed JWT is header, claims and signature",
    );
}

/// `build_jwt` computes `iat` before touching the key, so an unusable key
/// still exercises that path and then fails on the key itself.
#[cfg(feature = "service-account-credentials")]
#[test]
fn service_account_credentials_reject_unusable_private_key() {
    use crate::credentials::ServiceAccountCredentials;

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
