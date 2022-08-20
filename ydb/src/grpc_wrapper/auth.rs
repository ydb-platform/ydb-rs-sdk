use crate::client_common::{DBCredentials, TokenCache};
use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::grpc_wrapper::runtime_interceptors::{
    GrpcInterceptor, GrpcInterceptorRequestWithMeta, InterceptorError, InterceptorRequest,
    InterceptorResult,
};
use http::HeaderValue;

pub(crate) struct AuthGrpcInterceptor {
    db_name: HeaderValue,
    token_cache: TokenCache,
}

impl AuthGrpcInterceptor {
    pub fn new(cred: DBCredentials) -> RawResult<AuthGrpcInterceptor> {
        let db_name = HeaderValue::from_str(cred.database.as_str()).map_err(|err| {
            RawError::custom(format!(
                "bad db name for set in headers '{}': {}",
                cred.database.as_str(),
                err
            ))
        })?;

        return Ok(AuthGrpcInterceptor {
            db_name,
            token_cache: cred.token_cache,
        });
    }
}

impl GrpcInterceptor for AuthGrpcInterceptor {
    fn on_call(
        &self,
        mut req: InterceptorRequest,
    ) -> InterceptorResult<GrpcInterceptorRequestWithMeta> {
        let token = self.token_cache.token();
        let token = HeaderValue::from_str(token.as_str()).map_err(|err| {
            InterceptorError::custom(format!("received bad token (len={}): {}", token.len(), err))
        })?;

        req.headers_mut()
            .insert("x-ydb-database", self.db_name.clone());
        req.headers_mut().insert(
            "x-ydb-sdk-build-info",
            HeaderValue::from_str("ydb-go-sdk/0.0.0").unwrap(),
        );
        req.headers_mut().insert("x-ydb-auth-ticket", token);
        Ok(GrpcInterceptorRequestWithMeta {
            request: req,
            metadata: None,
        })
    }
}
