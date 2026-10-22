use crate::errors::{YdbError, YdbResult};
use rustls::crypto::CryptoProvider;
use rustls_platform_verifier::BuilderVerifierExt;
use std::sync::Arc;

#[cfg(feature = "rustls-ring")]
fn crypto_provider() -> YdbResult<Arc<CryptoProvider>> {
    Ok(Arc::new(rustls::crypto::ring::default_provider()))
}

#[cfg(all(feature = "rustls-aws-lc-rs", not(feature = "rustls-ring")))]
fn crypto_provider() -> YdbResult<Arc<CryptoProvider>> {
    Ok(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
}

#[cfg(not(any(feature = "rustls-ring", feature = "rustls-aws-lc-rs")))]
fn crypto_provider() -> YdbResult<Arc<CryptoProvider>> {
    CryptoProvider::get_default().cloned().ok_or_else(|| {
        YdbError::custom(
            "no rustls crypto provider: enable the `rustls-ring` or `rustls-aws-lc-rs` feature \
             of the `ydb` crate, or install a process default provider before creating credentials",
        )
    })
}

/// Blocking HTTP client for token endpoints.
///
/// `reqwest` is built on `rustls-no-provider`, so it panics when a client is
/// created unless the process already installed a provider. The client is given
/// a TLS configuration built from the provider selected by the `ydb` features
/// instead, so nothing depends on, or changes, the process-wide default.
pub(crate) fn blocking_client() -> YdbResult<reqwest::blocking::Client> {
    let tls = rustls::ClientConfig::builder_with_provider(crypto_provider()?)
        .with_safe_default_protocol_versions()
        .map_err(|err| YdbError::custom(format!("rustls protocol versions: {err}")))?
        .with_platform_verifier()
        .map_err(|err| YdbError::custom(format!("rustls platform verifier: {err}")))?
        .with_no_client_auth();

    Ok(reqwest::blocking::Client::builder()
        .use_preconfigured_tls(tls)
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(any(feature = "rustls-ring", feature = "rustls-aws-lc-rs"))]
    #[test]
    fn blocking_client_is_built_without_a_process_default_provider() {
        assert!(CryptoProvider::get_default().is_none());
        blocking_client().unwrap();
    }
}
