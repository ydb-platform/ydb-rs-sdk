use ydb_protobuf::generated::ydb::status_ids::StatusCode;

use crate::credentials::Credentials;
use crate::errors;
use crate::errors::{Error, Result};
use crate::internal::discovery::{Discovery, Service};
use crate::internal::middlewares::AuthService;
use crate::internal::trait_operation::Operation;
use std::str::FromStr;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tower::ServiceBuilder;

pub(crate) trait ClientFabric {
    // create grpc client
    // new_func - func for create grpc client from common middleware
    fn create<T, CB>(self: &Self, new_func: CB, service: Service) -> Result<T>
    where
        CB: FnOnce(AuthService) -> T;
}

pub(crate) struct SimpleGrpcClientFabric {
    discovery: Box<dyn Discovery>,
    cred: Box<dyn Credentials>,
    database: String,
}

impl SimpleGrpcClientFabric {
    pub fn new(
        discovery: Box<dyn Discovery>,
        cred: Box<dyn Credentials>,
        database: String,
    ) -> Self {
        SimpleGrpcClientFabric {
            discovery,
            cred,
            database,
        }
    }

    fn channel(self: &Self, service: Service) -> Result<Channel> {
        let uri = self.discovery.endpoint(service)?;
        let channel = Endpoint::from(uri)
            .tls_config(ClientTlsConfig::new())?
            .connect_lazy()?;

        return Ok(channel);
    }
}

impl ClientFabric for SimpleGrpcClientFabric {
    fn create<T, CB>(self: &Self, new_func: CB, service: Service) -> Result<T>
    where
        CB: FnOnce(AuthService) -> T,
    {
        let cred = self.cred.clone();
        let database = self.database.clone();
        let auth_service_create = |ch| {
            return AuthService::new(ch, cred.clone(), database.as_str());
        };

        let auth_ch = ServiceBuilder::new()
            .layer_fn(auth_service_create)
            .service(self.channel(service)?);

        return Ok(new_func(auth_ch));
    }
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
