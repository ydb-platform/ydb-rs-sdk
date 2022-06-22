use crate::channel_pool::ChannelErrorInfo;
use crate::client_common::DBCredentials;
use crate::grpc_wrapper::auth::{create_service_with_auth, ServiceWithAuth};
use crate::middlewares::AuthService;
use crate::YdbResult;
use http::Uri;
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tonic::transport::{ClientTlsConfig, Endpoint};
use tracing::trace;

pub(crate) type ChannelWithAuth = ServiceWithAuth<Channel>;

pub(crate) async fn create_grpc_channel_with_auth(
    uri: Uri,
    cred: DBCredentials,
) -> YdbResult<ChannelWithAuth> {
    let channel = create_grpc_channel(uri).await?;
    return Ok(create_service_with_auth(channel, cred));
}

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

    return Ok(endpoint.connect().await?);
}
