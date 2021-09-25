use ydb_protobuf::generated::ydb::status_ids::StatusCode;

use crate::credentials::Credentials;
use crate::errors;
use crate::errors::{Error, Result};
use crate::internal::middlewares::AuthService;
use crate::internal::trait_operation::Operation;
use http::Uri;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tower::ServiceBuilder;

pub(crate) fn create_grpc_client<T, CB>(
    uri: Uri,
    cred: Box<dyn Credentials>,
    database: String,
    new_func: CB,
) -> Result<T>
where
    CB: FnOnce(AuthService) -> T,
{
    let auth_service_create = |ch| {
        return AuthService::new(ch, cred.clone(), database.clone());
    };
    let channel = create_grpc_channel(uri)?;
    let auth_ch = ServiceBuilder::new()
        .layer_fn(auth_service_create)
        .service(channel);
    return Ok(new_func(auth_ch));
}

fn create_grpc_channel(uri: Uri) -> Result<Channel> {
    let tls = if let Some(scheme) = uri.scheme_str() {
        scheme == "https" || scheme == "grpcs"
    } else {
        false
    };

    let mut endpoint = Endpoint::from(uri);
    if tls {
        endpoint = endpoint.tls_config(ClientTlsConfig::new())?
    };
    return Ok(endpoint.connect_lazy()?);
}

pub(crate) fn grpc_read_result<TOp, T>(resp: tonic::Response<TOp>) -> errors::Result<T>
where
    TOp: Operation,
    T: Default + prost::Message,
{
    let resp_inner = resp.into_inner();
    let op = resp_inner
        .operation()
        .ok_or(Error::Custom("no operation object in result".into()))?;
    if op.status() != StatusCode::Success {
        return Err(Error::from(op.status()));
    }
    let opres = op
        .result
        .ok_or(Error::Custom("no result data in operation".into()))?;
    let res: T = T::decode(opres.value.as_slice())?;
    return Ok(res);
}

pub(crate) fn grpc_read_void_result<TOp>(resp: tonic::Response<TOp>) -> errors::Result<()>
where
    TOp: Operation,
{
    let resp_inner = resp.into_inner();
    let op = resp_inner
        .operation()
        .ok_or(Error::Custom("no operation object in result".into()))?;
    if op.status() != StatusCode::Success {
        return Err(Error::from(op.status()));
    }
    return Ok(());
}
