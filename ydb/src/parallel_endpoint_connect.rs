use crate::{YdbError, YdbResult};
use http::uri::Scheme;
use http::Uri;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::{trace, warn};

const MAX_PARALLEL_DIAL_ADDRESSES: usize = 16;

/// Timeouts for parallel gRPC dial to resolved addresses.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ConnectTimeouts {
    /// Applied to each per-IP `Endpoint::connect()` in parallel dial.
    pub(crate) per_endpoint: Duration,
    /// Upper bound for the whole parallel dial race (should exceed `per_endpoint`).
    pub(crate) parallel_overall: Duration,
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
///
/// Multi-address hostname endpoints dial resolved IPs in parallel (up to
/// [`MAX_PARALLEL_DIAL_ADDRESSES`] at a time, in batches) and cache a channel
/// connected to the winning concrete IP. That channel does **not** re-resolve
/// the FQDN on tonic reconnect; a dead IP can persist until discovery
/// pessimization removes the endpoint or the process creates a new channel.
pub(crate) async fn connect(
    uri: Uri,
    tls_config: &Option<ClientTlsConfig>,
    timeouts: ConnectTimeouts,
) -> YdbResult<Channel> {
    let (uri, port) = normalize_uri_for_connect(uri)?;
    let host = uri
        .host()
        .ok_or_else(|| YdbError::Custom("URI must have a host".to_string()))?;

    if parse_host_as_ip(host).is_some() {
        return connect_lazy(uri.clone(), tls_config, None, timeouts);
    }

    let addrs = resolve_socket_addrs(host, port).await?;
    connect_resolved(uri, addrs, tls_config, timeouts).await
}

pub(crate) async fn connect_resolved(
    uri: Uri,
    addrs: Vec<SocketAddr>,
    tls_config: &Option<ClientTlsConfig>,
    timeouts: ConnectTimeouts,
) -> YdbResult<Channel> {
    if addrs.is_empty() {
        let host = uri.host().unwrap_or("<unknown>");
        return Err(permanent_dial_error(format!(
            "no addresses resolved for host {host}"
        )));
    }

    if addrs.len() == 1 {
        return connect_lazy(uri, tls_config, None, timeouts);
    }

    trace!(
        host = uri.host().unwrap_or("<unknown>"),
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
        let force_domain_name = origin.is_some();
        endpoint = configure_tls_endpoint(endpoint, domain, tls_config, force_domain_name)?;
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

/// Normalize scheme and extract the connect port as one step.
///
/// Port defaults depend on the original scheme (`grpc`/`grpcs` → 2135). They must
/// be read before [`normalize_uri_scheme`] maps those schemes to `http`/`https`
/// (which would otherwise default to 80/443).
pub(crate) fn normalize_uri_for_connect(uri: Uri) -> YdbResult<(Uri, u16)> {
    let had_explicit_port = uri.port_u16().is_some();
    let port = uri_port(&uri);
    let uri = normalize_uri_scheme(uri)?;
    let uri = if had_explicit_port {
        uri
    } else {
        uri_with_port(uri, port)?
    };
    Ok((uri, port))
}

pub(crate) fn configure_tls_endpoint(
    endpoint: Endpoint,
    domain: &str,
    tls_config: &Option<ClientTlsConfig>,
    force_domain_name: bool,
) -> YdbResult<Endpoint> {
    // `domain` may include RFC 3986 brackets for IPv6 literals from `Uri::host()`.
    let domain = strip_ipv6_brackets(domain);
    let config = match tls_config {
        Some(config) if force_domain_name => {
            // Parallel dial connects by IP; override domain_name so SNI/certificate
            // verification use the original FQDN from `origin`.
            config.clone().domain_name(domain.to_string())
        }
        Some(config) => config.clone(),
        None => ClientTlsConfig::new()
            .domain_name(domain.to_string())
            .with_native_roots(),
    };

    Ok(endpoint.tls_config(config)?)
}

/// Parallel eager dial to pre-resolved addresses.
///
/// `original_uri` must already be scheme-normalized (`http`/`https`); see
/// [`normalize_uri_scheme`].
///
/// Addresses are dialed in batches of [`MAX_PARALLEL_DIAL_ADDRESSES`]; if every
/// address in a batch is unreachable, the next batch is tried. The returned
/// channel is pinned to the winning IP (see [`connect`]).
pub(crate) async fn parallel_connect(
    addrs: Vec<SocketAddr>,
    original_uri: Uri,
    tls_config: &Option<ClientTlsConfig>,
    timeouts: ConnectTimeouts,
) -> YdbResult<Channel> {
    let total_addrs = addrs.len();
    let mut accumulated_errors: Vec<YdbError> = Vec::new();
    let mut any_timed_out = false;
    let deadline = Instant::now() + timeouts.parallel_overall;

    for batch in addrs.chunks(MAX_PARALLEL_DIAL_ADDRESSES) {
        if Instant::now() >= deadline {
            any_timed_out = true;
            break;
        }

        match parallel_connect_batch(
            batch.to_vec(),
            &original_uri,
            tls_config,
            timeouts,
            deadline,
        )
        .await
        {
            Ok(channel) => return Ok(channel),
            Err(failure) => {
                any_timed_out |= failure.timed_out;
                accumulated_errors.extend(failure.errors);
                if failure.timed_out {
                    break;
                }
            }
        }
    }

    Err(parallel_dial_error(
        &accumulated_errors,
        total_addrs,
        any_timed_out,
    ))
}

struct BatchDialFailure {
    errors: Vec<YdbError>,
    timed_out: bool,
}

async fn parallel_connect_batch(
    addrs: Vec<SocketAddr>,
    original_uri: &Uri,
    tls_config: &Option<ClientTlsConfig>,
    timeouts: ConnectTimeouts,
    deadline: Instant,
) -> Result<Channel, BatchDialFailure> {
    let scheme = original_uri.scheme().cloned().unwrap_or(Scheme::HTTP);
    let path_and_query = original_uri
        .path_and_query()
        .map(|pq| pq.as_str().to_string());
    let tls_config_owned = tls_config.clone();
    let origin = original_uri.clone();

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

    let overall_timeout = tokio::time::sleep_until(deadline);
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
                        dial_errors.push(dial_error(format!(
                            "parallel dial task panicked: {join_err}"
                        )));
                    }
                    None => unreachable!("join_next with empty JoinSet"),
                }
            }
            _ = &mut overall_timeout => {
                tasks.abort_all();
                return Err(BatchDialFailure {
                    errors: dial_errors,
                    timed_out: true,
                });
            }
        }

        if tasks.is_empty() {
            break;
        }
    }

    if dial_errors.is_empty() {
        Err(BatchDialFailure {
            errors: vec![dial_error("failed to connect to any resolved address")],
            timed_out: false,
        })
    } else {
        Err(BatchDialFailure {
            errors: dial_errors,
            timed_out: false,
        })
    }
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

    if failed == 1 && !timed_out {
        return errors[0].clone();
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
        .map_err(|e| permanent_dial_error(format!("failed to resolve {host}: {e}")))
}

fn dial_error(message: impl Into<String>) -> YdbError {
    YdbError::transport_dial_failed(message)
}

fn permanent_dial_error(message: impl Into<String>) -> YdbError {
    YdbError::transport_dial_failed_permanent(message)
}

fn uri_with_port(uri: Uri, port: u16) -> YdbResult<Uri> {
    if uri.port_u16().is_some() {
        return Ok(uri);
    }

    let host = uri
        .host()
        .ok_or_else(|| YdbError::Custom("URI must have a host".to_string()))?;
    let mut builder = Uri::builder()
        .scheme(uri.scheme().cloned().unwrap_or(Scheme::HTTP))
        .authority(format!("{host}:{port}"));

    if let Some(path_and_query) = uri.path_and_query() {
        builder = builder.path_and_query(path_and_query.as_str());
    }

    Ok(builder.build()?)
}

fn uri_port(uri: &Uri) -> u16 {
    uri.port_u16()
        .unwrap_or_else(|| match uri.scheme().map(|s| s.as_str()) {
            Some("grpc") | Some("grpcs") => 2135,
            Some("https") => 443,
            Some("http") | None => 80,
            _ => 80,
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
        let grpc_uri = Uri::from_static("grpc://example.com");
        let grpcs_uri = Uri::from_static("grpcs://example.com");
        let http_uri = Uri::from_static("http://example.com");
        let https_uri = Uri::from_static("https://example.com");

        assert_eq!(uri_port(&grpc_uri), 2135);
        assert_eq!(uri_port(&grpcs_uri), 2135);
        assert_eq!(uri_port(&http_uri), 80);
        assert_eq!(uri_port(&https_uri), 443);
    }

    #[test]
    fn normalize_uri_for_connect_preserves_grpc_port_default() -> YdbResult<()> {
        let uri = Uri::from_static("grpc://example.com");
        let (normalized, port) = normalize_uri_for_connect(uri)?;

        assert_eq!(normalized.scheme(), Some(&Scheme::HTTP));
        assert_eq!(normalized.port_u16(), Some(2135));
        assert_eq!(port, 2135);

        Ok(())
    }

    #[test]
    fn normalize_uri_for_connect_preserves_explicit_port() -> YdbResult<()> {
        let uri = Uri::from_static("grpc://example.com:9999");
        let (normalized, port) = normalize_uri_for_connect(uri)?;

        assert_eq!(normalized.port_u16(), Some(9999));
        assert_eq!(port, 9999);

        Ok(())
    }

    #[test]
    fn dial_errors_are_retryable() {
        let timeout_err = dial_error("connect timeout: no reachable addresses");
        assert!(matches!(
            timeout_err.need_retry(),
            NeedRetry::IdempotentOnly
        ));

        let resolve_err = permanent_dial_error("failed to resolve example.com: NXDOMAIN");
        assert!(matches!(resolve_err.need_retry(), NeedRetry::False));
    }

    #[test]
    fn permanent_dial_error_does_not_depend_on_message_wording() {
        let err = permanent_dial_error("NXDOMAIN");
        assert!(matches!(err.need_retry(), NeedRetry::False));
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

    #[test]
    fn parallel_dial_error_timeout_with_single_failure_includes_timeout_context() {
        let errors = vec![dial_error("connection refused")];
        let err = parallel_dial_error(&errors, 3, true);
        let message = format!("{err:?}");
        assert!(message.contains("connect timeout: 1/3 addresses failed"));
        assert!(message.contains("connection refused"));
    }
}
