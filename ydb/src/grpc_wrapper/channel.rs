use crate::YdbResult;
use http::Uri;
use std::time::Duration;

use tonic::transport::Channel;
use tonic::transport::{ClientTlsConfig, Endpoint};
use tracing::trace;

#[tracing::instrument]
async fn create_grpc_channel(uri: Uri) -> YdbResult<Channel> {
    trace!("start work");
    let tls = if let Some(scheme) = uri.scheme_str() {
        scheme == "https" || scheme == "grpcs"
    } else {
        false
    };

    let mut endpoint = Endpoint::from(uri.clone());
    if tls {
        endpoint = endpoint.tls_config(ClientTlsConfig::new())?
    };
    endpoint = endpoint.tcp_keepalive(Some(Duration::from_secs(15))); // tcp keepalive similar to default in golang lib

    trace!("endpoint: {:?}", endpoint);

    Ok(endpoint.connect().await?)
}
