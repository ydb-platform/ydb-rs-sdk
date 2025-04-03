use crate::client_common::{DBCredentials, TokenCache};
use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::grpc_wrapper::runtime_interceptors::{
    GrpcInterceptor, InterceptorError, InterceptorRequest, InterceptorResult, RequestMetadata,
};
use http::HeaderValue;
use secrecy::ExposeSecret;

pub(crate) struct AuthGrpcInterceptor {
    db_name: HeaderValue,
    token_cache: TokenCache,
}
const VERSION_LABEL: &str = concat!("ydb-rust-sdk/", env!("CARGO_PKG_VERSION"));

impl AuthGrpcInterceptor {
    pub fn new(cred: DBCredentials) -> RawResult<AuthGrpcInterceptor> {
        let db_name = HeaderValue::from_str(cred.database.as_str()).map_err(|err| {
            RawError::custom(format!(
                "bad db name for set in headers '{}': {}",
                cred.database.as_str(),
                err
            ))
        })?;

        Ok(AuthGrpcInterceptor {
            db_name,
            token_cache: cred.token_cache,
        })
    }
}

impl GrpcInterceptor for AuthGrpcInterceptor {
    fn on_call(
        &self,
        _metadata: &mut RequestMetadata,
        mut req: InterceptorRequest,
    ) -> InterceptorResult<InterceptorRequest> {
        let token_secret = self.token_cache.token();
        let token_string = token_secret.expose_secret();
        let token = HeaderValue::from_str(token_string.as_str()).map_err(|err| {
            InterceptorError::custom(format!(
                "received bad token (len={}): {}",
                token_string.len(),
                err
            ))
        })?;

        req.headers_mut()
            .insert("x-ydb-database", self.db_name.clone());
        req.headers_mut().insert(
            "x-ydb-sdk-build-info",
            HeaderValue::from_static(VERSION_LABEL),
        );
        req.headers_mut().insert("x-ydb-auth-ticket", token);
        Ok(req)
    }
}
