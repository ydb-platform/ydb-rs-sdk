use std::time::Duration;
use ydb_protobuf::generated::ydb::status_ids::StatusCode;

use crate::errors;
use crate::errors::{Error, Result};
use crate::internal::client_common::DBCredentials;
use crate::internal::middlewares::{AuthService};
use crate::internal::trait_operation::Operation;
use http::Uri;
use tokio::sync::mpsc;

use tonic::transport::{ClientTlsConfig, Endpoint};
use tower::ServiceBuilder;
use crate::internal::channel_pool::{ChannelErrorInfo, ChannelProxy, ChannelProxyErrorSender};

pub(crate) fn create_grpc_client<T, CB>(uri: Uri, cred: DBCredentials, new_func: CB) -> Result<T>
    where
        CB: FnOnce(AuthService) -> T,
{
    return create_grpc_client_with_error_sender(uri, cred, None, new_func)
}


pub(crate) fn create_grpc_client_with_error_sender<T, CB>(uri: Uri, cred: DBCredentials, error_sender: ChannelProxyErrorSender, new_func: CB) -> Result<T>
    where
        CB: FnOnce(AuthService) -> T,
{
    let channel = create_grpc_channel(uri, error_sender)?;
    return create_client_on_channel(channel, cred, new_func)
}

fn create_client_on_channel<NewFuncT, ClientT>(channel: ChannelProxy, cred: DBCredentials, new_func: NewFuncT) -> Result<ClientT>
where
    NewFuncT: FnOnce(AuthService) -> ClientT,
{
    let auth_service_create = |ch| {
        return AuthService::new(ch, cred.clone());
    };
    let auth_ch = ServiceBuilder::new()
        .layer_fn(auth_service_create)
        .service(channel);
    return Ok(new_func(auth_ch));
}

fn create_grpc_channel(uri: Uri, error_sender: Option<mpsc::Sender<ChannelErrorInfo>>) -> Result<ChannelProxy> {
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
    return Ok(ChannelProxy::new(uri, endpoint.connect_lazy()?, error_sender));
}

pub(crate) fn grpc_read_operation_result<TOp, T>(resp: tonic::Response<TOp>) -> errors::Result<T>
where
    TOp: Operation,
    T: Default + prost::Message,
{
    let resp_inner = resp.into_inner();
    let op = resp_inner
        .operation()
        .ok_or(Error::Custom("no operation object in result".into()))?;
    if op.status() != StatusCode::Success {
        return Err(Error::from(op));
    }
    let opres = op
        .result
        .ok_or(Error::Custom("no result data in operation".into()))?;
    let res: T = T::decode(opres.value.as_slice())?;
    return Ok(res);
}

pub(crate) fn grpc_read_void_operation_result<TOp>(resp: tonic::Response<TOp>) -> errors::Result<()>
where
    TOp: Operation,
{
    let resp_inner = resp.into_inner();
    let op = resp_inner
        .operation()
        .ok_or(Error::Custom("no operation object in result".into()))?;
    if op.status() != StatusCode::Success {
        return Err(Error::from(op));
    }
    return Ok(());
}
