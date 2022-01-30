use std::task::{Context, Poll};
use std::{future::Future, pin::Pin};

use crate::internal::channel_pool::ChannelProxy;
use http::{HeaderValue, Request, Response};
use tonic::body::BoxBody;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;
use tonic::transport::Body;
use tonic::{Code, Status};
use tower::Service;

use crate::internal::client_common::DBCredentials;

#[derive(Clone, Debug)]
pub(crate) struct AuthService {
    ch: ChannelProxy,
    cred: DBCredentials,
}

impl AuthService {
    pub fn new(ch: ChannelProxy, cred: DBCredentials) -> Self {
        return AuthService { ch, cred };
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
        let database = self.cred.database.clone();
        let token_result = self.cred.credentials.create_token();

        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.ch.clone();
        let mut ch = std::mem::replace(&mut self.ch, clone);

        Box::pin(async move {
            let token_info = token_result?;

            req.headers_mut()
                .insert("x-ydb-database", HeaderValue::from_str(database.as_str())?);
            req.headers_mut().insert(
                "x-ydb-auth-ticket",
                HeaderValue::from_str(token_info.token.as_str())?,
            );

            let response = ch.call(req).await?;
            Ok(response)
        })
    }
}

pub(crate) struct AuthInterceptor {
    cred: DBCredentials,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        let db_name = match AsciiMetadataValue::from_str(self.cred.database.as_str()) {
            Ok(val) => val,
            Err(_err) => {
                return Err(Status::new(
                    Code::InvalidArgument,
                    "non-ascii dbname received for auth interceptor",
                ))
            }
        };
        request.metadata_mut().insert("x-ydb-database", db_name);

        let token_info = match self.cred.credentials.create_token() {
            Ok(token_info) => match AsciiMetadataValue::from_str(token_info.token.as_str()) {
                Ok(val) => val,
                Err(err) => {
                    return Err(Status::new(
                        Code::InvalidArgument,
                        format!("non-ascii token received for auth interceptor: {}", err),
                    ))
                }
            },
            Err(err) => {
                return Err(Status::new(
                    Code::Internal,
                    format!(
                        "error receive auth token for auth interceptor: {}",
                        err.to_string()
                    ),
                ))
            }
        };
        request
            .metadata_mut()
            .insert("x-ydb-auth-ticket", token_info);
        request.metadata_mut().insert(
            "x-ydb-sdk-build-info",
            AsciiMetadataValue::from_str("ydb-go-sdk/0.0.0").unwrap(),
        );
        return Ok(request);
    }
}
