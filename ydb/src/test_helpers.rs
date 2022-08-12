use crate::ClientBuilder;
use once_cell::sync::Lazy;

pub(crate) static CONNECTION_STRING: Lazy<String> = Lazy::new(|| {
    std::env::var("YDB_CONNECTION_STRING")
        .unwrap_or_else(|_| "grpc://localhost:2136?database=/local".to_string())
        .parse()
        .unwrap()
});

pub(crate) fn test_client_builder() -> ClientBuilder {
    CONNECTION_STRING.as_str().parse().unwrap()
}
