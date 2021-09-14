use crate::client::trait_operation::Operation;
use crate::credentials::Credencials;
use crate::errors::{Error, Result};
use http::{HeaderValue, Request, Response};
use std::task::{Context, Poll};
use std::{future::Future, pin::Pin};
use tonic::body::BoxBody;
use tonic::transport::{Body, Channel};
use tower::{Service, ServiceBuilder};
use ydb_protobuf::generated::ydb::status_ids::StatusCode;

pub(crate) struct AuthService {
    ch: Channel,
    cred: Box<dyn Credencials>,
    database: String,
}

impl AuthService {
    pub fn new(ch: Channel, cred: Box<dyn Credencials>, database: &str) -> Self {
        return AuthService {
            ch,
            cred,
            database: database.to_string(),
        };
    }
}

impl Service<Request<BoxBody>> for AuthService {
    type Response = Response<Body>;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    #[allow(clippy::type_complexity)]
    type Future =
        Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        self.ch.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, mut req: Request<BoxBody>) -> Self::Future {
        // let token = MetadataValue::from_str(token.as_str()).unwrap();
        let database = self.database.clone();
        let mut token = String::new();
        self.cred.fill_token(&mut token);

        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.ch.clone();
        let mut ch = std::mem::replace(&mut self.ch, clone);

        Box::pin(async move {
            req.headers_mut()
                .insert("x-ydb-database", HeaderValue::from_str(database.as_str())?);
            req.headers_mut()
                .insert("x-ydb-auth-ticket", HeaderValue::from_str(token.as_str())?);

            let response = ch.call(req).await?;
            Ok(response)
        })
    }
}

pub(crate) fn create_grpc_client<T, CB>(
    channel: &Channel,
    cred: &Box<dyn Credencials>,
    database: &str,
    new_func: CB,
) -> T
where
    CB: FnOnce(AuthService) -> T,
{
    let auth_service_create = |ch| {
        return AuthService::new(ch, cred.clone(), database);
    };

    let auth_ch = ServiceBuilder::new()
        .layer_fn(auth_service_create)
        .service(channel.clone());

    return new_func(auth_ch);
}

pub(crate) fn grpc_read_result<TOp, T>(resp: tonic::Response<TOp>) -> Result<T>
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
