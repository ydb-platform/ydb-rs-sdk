use std::task::{Context, Poll};
use std::{future::Future, pin::Pin};

use http::{HeaderValue, Request, Response};
use tonic::body::BoxBody;
use tonic::transport::{Body, Channel};
use tower::Service;

use crate::credentials::Credentials;

#[derive(Clone, Debug)]
pub(crate) struct AuthService {
    ch: Channel,
    cred: Box<dyn Credentials>,
    database: String,
}

impl AuthService {
    pub fn new(ch: Channel, cred: Box<dyn Credentials>, database: String) -> Self {
        return AuthService { ch, cred, database };
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
        let token_result = self.cred.create_token();

        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.ch.clone();
        let mut ch = std::mem::replace(&mut self.ch, clone);

        Box::pin(async move {
            let token = token_result?;

            req.headers_mut()
                .insert("x-ydb-database", HeaderValue::from_str(database.as_str())?);
            req.headers_mut()
                .insert("x-ydb-auth-ticket", HeaderValue::from_str(token.as_str())?);

            let response = ch.call(req).await?;
            Ok(response)
        })
    }
}
