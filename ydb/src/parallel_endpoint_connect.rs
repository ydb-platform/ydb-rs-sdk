use crate::{YdbError, YdbResult};
use http::uri::Scheme;
use http::Uri;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::trace;

const PER_ENDPOINT_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const PARALLEL_CONNECT_OVERALL_TIMEOUT: Duration = Duration::from_millis(750);

/// Establish a gRPC channel for the given URI.
///
/// For FQDN endpoints that resolve to multiple IP addresses, dials all
/// addresses in parallel and returns the first successful connection.
/// Unreachable addresses are skipped, similar to gRPC `round_robin` policy
/// in ydb-go-sdk.
pub(crate) async fn connect(uri: Uri, tls_config: &Option<ClientTlsConfig>) -> YdbResult<Channel> {
    let uri = normalize_uri_scheme(uri)?;
    let host = uri
        .host()
        .ok_or_else(|| YdbError::Custom("URI must have a host".to_string()))?;

    if host.parse::<IpAddr>().is_ok() {
        return connect_lazy(uri.clone(), tls_config, None);
    }

    let port = uri_port(&uri);
    let addrs = resolve_socket_addrs(host, port).await?;

    if addrs.is_empty() {
        return Err(dial_error(format!("no addresses resolved for host {host}")));
    }

    if addrs.len() == 1 {
        return connect_lazy(uri, tls_config, None);
    }

    trace!(
        host,
        count = addrs.len(),
        "parallel dial to resolved addresses"
    );
    parallel_connect(addrs, uri, tls_config).await
}

fn connect_lazy(
    uri: Uri,
    tls_config: &Option<ClientTlsConfig>,
    origin: Option<Uri>,
) -> YdbResult<Channel> {
    let endpoint = build_endpoint(uri, tls_config, origin, false)?;
    Ok(endpoint.connect_lazy())
}

async fn connect_eager(
    uri: Uri,
    tls_config: &Option<ClientTlsConfig>,
    origin: Option<Uri>,
) -> YdbResult<Channel> {
    let endpoint = build_endpoint(uri, tls_config, origin, true)?;
    endpoint
        .connect()
        .await
        .map_err(|e| YdbError::TransportDial(Arc::new(e)))
}

fn build_endpoint(
    uri: Uri,
    tls_config: &Option<ClientTlsConfig>,
    origin: Option<Uri>,
    eager: bool,
) -> YdbResult<Endpoint> {
    let tls = uri.scheme() == Some(&Scheme::HTTPS);
    let mut endpoint = Endpoint::from(uri.clone());

    if let Some(ref origin_uri) = origin {
        endpoint = endpoint.origin(origin_uri.clone());
    }

    if tls {
        let domain = origin_uri_host(origin.as_ref(), &uri).ok_or_else(|| {
            YdbError::Custom("URI must have a host for TLS connections".to_string())
        })?;
        endpoint = configure_tls_endpoint(endpoint, domain, tls_config)?;
    }

    endpoint = endpoint.http2_keep_alive_interval(Duration::from_secs(10));
    if eager {
        endpoint = endpoint.connect_timeout(PER_ENDPOINT_CONNECT_TIMEOUT);
    }

    Ok(endpoint)
}

pub(crate) fn normalize_uri_scheme(uri: Uri) -> YdbResult<Uri> {
    let mut parts = uri.into_parts();
    let scheme = parts.scheme.as_ref().unwrap_or(&Scheme::HTTP);

    match scheme.as_str() {
        "grpc" => parts.scheme = Some(Scheme::HTTP),
        "grpcs" => parts.scheme = Some(Scheme::HTTPS),
        _ => {}
    }

    Ok(Uri::from_parts(parts)?)
}

pub(crate) fn configure_tls_endpoint(
    endpoint: Endpoint,
    domain: &str,
    tls_config: &Option<ClientTlsConfig>,
) -> YdbResult<Endpoint> {
    let config = match tls_config {
        Some(config) => config.clone().domain_name(domain.to_string()),
        None => ClientTlsConfig::new()
            .domain_name(domain.to_string())
            .with_native_roots(),
    };

    Ok(endpoint.tls_config(config)?)
}

pub(crate) async fn parallel_connect(
    addrs: Vec<SocketAddr>,
    original_uri: Uri,
    tls_config: &Option<ClientTlsConfig>,
) -> YdbResult<Channel> {
    let scheme = original_uri.scheme().cloned().unwrap_or(Scheme::HTTP);
    let path_and_query = original_uri
        .path_and_query()
        .map(|pq| pq.as_str().to_string());
    let tls_config = tls_config.clone();
    let origin = original_uri.clone();

    let mut tasks = JoinSet::new();

    for addr in addrs {
        let scheme = scheme.clone();
        let path_and_query = path_and_query.clone();
        let tls_config = tls_config.clone();
        let origin = origin.clone();

        tasks.spawn(async move {
            let ip_uri = socket_addr_to_uri(addr, &scheme, path_and_query.as_deref())?;
            connect_eager(ip_uri, &tls_config, Some(origin)).await
        });
    }

    let overall_timeout = tokio::time::sleep(PARALLEL_CONNECT_OVERALL_TIMEOUT);
    tokio::pin!(overall_timeout);

    let mut last_error: Option<YdbError> = None;

    loop {
        tokio::select! {
            biased;
            result = tasks.join_next(), if !tasks.is_empty() => {
                match result {
                    Some(Ok(Ok(channel))) => {
                        tasks.abort_all();
                        return Ok(channel);
                    }
                    Some(Ok(Err(err))) => last_error = Some(err),
                    Some(Err(_)) => {}
                    None => break,
                }
            }
            _ = &mut overall_timeout => {
                tasks.abort_all();
                return Err(last_error.unwrap_or_else(|| {
                    dial_error("connect timeout: no reachable addresses")
                }));
            }
        }

        if tasks.is_empty() {
            break;
        }
    }

    Err(last_error.unwrap_or_else(|| dial_error("failed to connect to any resolved address")))
}

async fn resolve_socket_addrs(host: &str, port: u16) -> YdbResult<Vec<SocketAddr>> {
    tokio::net::lookup_host((host, port))
        .await
        .map(|iter| iter.collect())
        .map_err(|e| dial_error(format!("failed to resolve {host}: {e}")))
}

fn dial_error(message: impl Into<String>) -> YdbError {
    YdbError::Transport(message.into())
}

fn uri_port(uri: &Uri) -> u16 {
    uri.port_u16().unwrap_or_else(|| {
        if uri.scheme() == Some(&Scheme::HTTPS) {
            443
        } else {
            80
        }
    })
}

fn origin_uri_host<'a>(origin: Option<&'a Uri>, fallback: &'a Uri) -> Option<&'a str> {
    origin
        .and_then(|uri| uri.host())
        .or_else(|| fallback.host())
}

fn socket_addr_to_uri(
    addr: SocketAddr,
    scheme: &Scheme,
    path_and_query: Option<&str>,
) -> YdbResult<Uri> {
    let authority = match addr {
        SocketAddr::V4(_) => format!("{}:{}", addr.ip(), addr.port()),
        SocketAddr::V6(_) => format!("[{}]:{}", addr.ip(), addr.port()),
    };

    Ok(Uri::builder()
        .scheme(scheme.clone())
        .authority(authority)
        .path_and_query(path_and_query.unwrap_or(""))
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::NeedRetry;
    use http::uri::Scheme;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn socket_addr_to_uri_ipv4() -> YdbResult<()> {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 2135);
        let uri = socket_addr_to_uri(addr, &Scheme::HTTPS, Some("/local"))?;

        assert_eq!(uri.scheme(), Some(&Scheme::HTTPS));
        assert_eq!(uri.host(), Some("10.0.0.1"));
        assert_eq!(uri.port_u16(), Some(2135));
        assert_eq!(uri.path(), "/local");

        Ok(())
    }

    #[test]
    fn socket_addr_to_uri_ipv6() -> YdbResult<()> {
        let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)), 2135);
        let uri = socket_addr_to_uri(addr, &Scheme::HTTP, None)?;

        assert_eq!(uri.scheme(), Some(&Scheme::HTTP));
        assert_eq!(uri.host(), Some("[::1]"));
        assert_eq!(uri.port_u16(), Some(2135));

        Ok(())
    }

    #[test]
    fn uri_port_defaults_by_scheme() {
        let http_uri = Uri::from_static("http://example.com");
        let https_uri = Uri::from_static("https://example.com");

        assert_eq!(uri_port(&http_uri), 80);
        assert_eq!(uri_port(&https_uri), 443);
    }

    #[test]
    fn dial_errors_are_retryable() {
        let timeout_err = dial_error("connect timeout: no reachable addresses");
        assert!(matches!(
            timeout_err.need_retry(),
            NeedRetry::True | NeedRetry::IdempotentOnly
        ));
    }
}
