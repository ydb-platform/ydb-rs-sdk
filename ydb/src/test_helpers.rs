use crate::ClientBuilder;
use once_cell::sync::Lazy;
use tracing::trace;
use url::Url;

pub(crate) static CONNECTION_STRING: Lazy<String> = Lazy::new(|| {
    std::env::var("YDB_CONNECTION_STRING")
        .unwrap_or_else(|_| "grpc://localhost:2136/local".to_string())
        .parse()
        .unwrap()
});

pub(crate) static TLS_CONNECTION_STRING: Lazy<String> = Lazy::new(|| {
    std::env::var("YDB_CONNECTION_STRING")
        .unwrap_or_else(|_| "grpcs://localhost:2135/local".to_string())
        .parse()
        .unwrap()
});

pub(crate) fn test_client_builder() -> ClientBuilder {
    CONNECTION_STRING.as_str().parse().unwrap()
}

pub(crate) fn get_passworded_connection_string() -> String {
    Url::parse_with_params(
        &CONNECTION_STRING,
        &[("token_static_password", "1234"), ("token_static_username", "root")],
    )
    .unwrap()
    .as_str()
    .to_string()
}

pub(crate) fn get_custom_ca_connection_string() -> String {
    trace!("forge ca connection string");
    Url::parse_with_params(
        &TLS_CONNECTION_STRING,
        &[
            ("ca_certificate", "./../ydb_certs/ca.pem"),
            ],
    )
    .unwrap()
    .as_str()
    .to_string()
}

pub(crate) fn test_with_password_builder() -> ClientBuilder {
    ClientBuilder::new_from_connection_string(get_passworded_connection_string()).unwrap()
}

pub(crate) fn test_custom_ca_client_builder() -> ClientBuilder {
    ClientBuilder::new_from_connection_string(get_custom_ca_connection_string()).unwrap()
}
