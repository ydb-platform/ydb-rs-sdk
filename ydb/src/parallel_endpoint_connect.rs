use crate::{YdbError, YdbResult};
use http::uri::Scheme;
use http::Uri;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::{trace, warn};

/// Timeouts for parallel gRPC dial to resolved addresses.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ConnectTimeouts {
    /// Applied to each per-IP `Endpoint::connect()` in parallel dial.
    pub per_endpoint: Duration,
    /// Upper bound for the whole parallel dial race (should exceed `per_endpoint`).
    pub parallel_overall: Duration,
}

impl Default for ConnectTimeouts {
    fn default() -> Self {
        // Matches ydb-go-sdk DefaultDialTimeout (5s) with margin for the overall race.
        Self {
            per_endpoint: Duration::from_secs(5),
            parallel_overall: Duration::from_secs(6),
        }
    }
}

/// Establish a gRPC channel for the given URI.
///
/// Single-address hostname endpoints use a lazy channel keyed by the original
/// FQDN so tonic can re-resolve DNS when the connection is re-established.
/// Multi-address hostname endpoints use the parallel eager dial path below and
/// the resulting channel is connected to the winning concrete IP address.
pub(crate) async fn connect(
    uri: Uri,
    tls_config: &Option<ClientTlsConfig>,
    timeouts: ConnectTimeouts,
) -> YdbResult<Channel> {
    let uri = normalize_uri_scheme(uri)?;
    let host = uri
        .host()
        .ok_or_else(|| YdbError::Custom("URI must have a host".to_string()))?;

    if parse_host_as_ip(host).is_some() {
        return connect_lazy(uri.clone(), tls_config, None, timeouts);
    }

    let port = uri_port(&uri);
    let addrs = resolve_socket_addrs(host, port).await?;

    if addrs.is_empty() {
        return Err(dial_error(format!("no addresses resolved for host {host}")));
    }

    if addrs.len() == 1 {
        return connect_lazy(uri, tls_config, None, timeouts);
    }

    trace!(
        host,
        count = addrs.len(),
        "parallel dial to resolved addresses"
    );
    parallel_connect(addrs, uri, tls_config, timeouts).await
}

fn connect_lazy(
    uri: Uri,
    tls_config: &Option<ClientTlsConfig>,
    origin: Option<Uri>,
    timeouts: ConnectTimeouts,
) -> YdbResult<Channel> {
    let endpoint = build_endpoint(uri, tls_config, origin, false, timeouts)?;
    Ok(endpoint.connect_lazy())
}

async fn connect_eager(
    uri: Uri,
    tls_config: &Option<ClientTlsConfig>,
    origin: Option<Uri>,
    timeouts: ConnectTimeouts,
) -> YdbResult<Channel> {
    let endpoint = build_endpoint(uri, tls_config, origin, true, timeouts)?;
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
    timeouts: ConnectTimeouts,
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
        endpoint = endpoint.connect_timeout(timeouts.per_endpoint);
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
    // `domain` may include RFC 3986 brackets for IPv6 literals from `Uri::host()`.
    let domain = strip_ipv6_brackets(domain);
    let config = match tls_config {
        // Always set domain_name: parallel dial connects by IP and needs the
        // original FQDN for TLS verification/SNI. For FQDN dial the value
        // matches the URI host; a user-provided domain_name is intentionally
        // overridden to keep IP-based and hostname-based paths consistent.
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
    timeouts: ConnectTimeouts,
) -> YdbResult<Channel> {
    let original_uri = normalize_uri_scheme(original_uri)?;
    let scheme = original_uri.scheme().cloned().unwrap_or(Scheme::HTTP);
    let path_and_query = original_uri
        .path_and_query()
        .map(|pq| pq.as_str().to_string());
    let tls_config_owned = tls_config.clone();
    let origin = original_uri.clone();
    let addrs_len = addrs.len();

    let mut tasks = JoinSet::new();

    for addr in addrs {
        let scheme = scheme.clone();
        let path_and_query = path_and_query.clone();
        let tls_config = tls_config_owned.clone();
        let origin = origin.clone();

        tasks.spawn(async move {
            let ip_uri = socket_addr_to_uri(addr, &scheme, path_and_query.as_deref())?;
            connect_eager(ip_uri, &tls_config, Some(origin), timeouts).await
        });
    }

    let overall_timeout = tokio::time::sleep(timeouts.parallel_overall);
    tokio::pin!(overall_timeout);

    let mut dial_errors: Vec<YdbError> = Vec::new();

    loop {
        tokio::select! {
            biased;
            result = tasks.join_next(), if !tasks.is_empty() => {
                match result {
                    Some(Ok(Ok(channel))) => {
                        tasks.abort_all();
                        return Ok(channel);
                    }
                    Some(Ok(Err(err))) => dial_errors.push(err),
                    Some(Err(join_err)) => {
                        warn!(error = %join_err, "parallel dial task failed unexpectedly");
                    }
                    None => break,
                }
            }
            _ = &mut overall_timeout => {
                tasks.abort_all();
                return Err(parallel_dial_error(&dial_errors, addrs_len, true));
            }
        }

        if tasks.is_empty() {
            break;
        }
    }

    Err(parallel_dial_error(&dial_errors, addrs_len, false))
}

fn parallel_dial_error(errors: &[YdbError], total_addrs: usize, timed_out: bool) -> YdbError {
    if errors.is_empty() {
        return dial_error(if timed_out {
            "connect timeout: no reachable addresses"
        } else {
            "failed to connect to any resolved address"
        });
    }

    let failed = errors.len();
    let last = format!("{:?}", errors.last().unwrap());
    let prefix = if timed_out {
        format!("connect timeout: {failed}/{total_addrs} addresses failed, last error: {last}")
    } else {
        format!(
            "parallel dial failed: {failed}/{total_addrs} addresses unreachable, last error: {last}"
        )
    };

    if failed == 1 {
        return dial_error(prefix);
    }

    let details: Vec<String> = errors.iter().map(|err| format!("{err:?}")).collect();
    dial_error(format!("{prefix}; errors: {}", details.join("; ")))
}

fn strip_ipv6_brackets(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
}

fn parse_host_as_ip(host: &str) -> Option<IpAddr> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Some(ip);
    }

    // http::Uri returns IPv6 hosts in bracketed form, e.g. "[::1]".
    if host.starts_with('[') && host.ends_with(']') && host.len() > 2 {
        return host[1..host.len() - 1].parse().ok();
    }

    None
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
    fn strip_ipv6_brackets_removes_brackets() {
        assert_eq!(strip_ipv6_brackets("[::1]"), "::1");
        assert_eq!(strip_ipv6_brackets("example.com"), "example.com");
    }

    #[test]
    fn parse_host_as_ip_ipv4() {
        assert_eq!(
            parse_host_as_ip("10.0.0.1"),
            Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)))
        );
    }

    #[test]
    fn parse_host_as_ip_ipv6_bracketed() {
        assert_eq!(
            parse_host_as_ip("[::1]"),
            Some(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)))
        );
    }

    #[test]
    fn parse_host_as_ip_hostname() {
        assert_eq!(parse_host_as_ip("example.com"), None);
    }

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

    #[test]
    fn default_timeouts_parallel_overall_exceeds_per_endpoint() {
        let timeouts = ConnectTimeouts::default();
        assert!(timeouts.parallel_overall > timeouts.per_endpoint);
    }

    #[test]
    fn parallel_dial_error_aggregates_multiple_failures() {
        let errors = vec![
            dial_error("connection refused"),
            dial_error("TLS handshake failed"),
        ];
        let err = parallel_dial_error(&errors, 3, false);
        let message = format!("{err:?}");
        assert!(message.contains("parallel dial failed: 2/3 addresses unreachable"));
        assert!(message.contains("connection refused"));
        assert!(message.contains("TLS handshake failed"));
    }

    #[test]
    fn parallel_dial_error_timeout_with_no_failures() {
        let err = parallel_dial_error(&[], 2, true);
        assert!(format!("{err:?}").contains("connect timeout: no reachable addresses"));
    }
}
