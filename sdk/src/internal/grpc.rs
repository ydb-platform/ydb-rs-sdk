use ydb_protobuf::generated::ydb::status_ids::StatusCode;

use crate::credentials::Credentials;
use crate::errors;
use crate::errors::{Error, Result};
use crate::internal::discovery::Discovery;
use crate::internal::middlewares::AuthService;
use crate::internal::trait_operation::Operation;
use std::str::FromStr;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tower::ServiceBuilder;

pub(crate) trait ClientFabric {
    // create grpc client
    // new_func - func for create grpc client from common middleware
    fn create<T, CB>(self: &Self, new_func: CB) -> Result<T>
    where
        CB: FnOnce(AuthService) -> T;
}

pub(crate) struct SimpleGrpcClient {
    discovery: Box<dyn Discovery>,
    cred: Box<dyn Credentials>,
    database: String,
}

impl SimpleGrpcClient {
    pub fn new(
        discovery: Box<dyn Discovery>,
        cred: Box<dyn Credentials>,
        database: String,
    ) -> Self {
        SimpleGrpcClient {
            discovery,
            cred,
            database,
        }
    }

    fn channel(self: &Self) -> Result<Channel> {
        let uri = http::uri::Uri::from_str(self.discovery.endpoint()?.as_str())?;

        let channel = Endpoint::from(uri)
            .tls_config(ClientTlsConfig::new())?
            .connect_lazy()?;

        return Ok(channel);
    }
}

impl ClientFabric for SimpleGrpcClient {
    fn create<T, CB>(self: &Self, new_func: CB) -> Result<T>
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
            .service(self.channel()?);

        return Ok(new_func(auth_ch));
    }
}

pub(crate) fn grpc_read_result<TOp, T>(resp: tonic::Response<TOp>) -> errors::Result<T>
where
    TOp: Operation,
    T: Default + prost::Message,
{
    let resp_inner = resp.into_inner();
    let op = resp_inner.operation().unwrap();
    if op.status() != StatusCode::Success {
        return Err(Error::from(op.status()));
    }
    let opres = op.result.unwrap();
    let res: T = T::decode(opres.value.as_slice())?;
    return Ok(res);
}
