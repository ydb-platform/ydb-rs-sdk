use crate::client_common::DBCredentials;
use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::grpc_wrapper::runtime_interceptors::{
    InterceptorError, InterceptorRequest, InterceptorResult,
};
use http::HeaderValue;
use tonic::codegen::InterceptedService;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;
use tonic::{Code, Status};

pub(crate) type ServiceWithAuth<S> = InterceptedService<S, AuthInterceptor>;

pub(crate) fn create_service_with_auth<S>(service: S, cred: DBCredentials) -> ServiceWithAuth<S> {
    ServiceWithAuth::new(service, AuthInterceptor { cred })
}

pub(crate) fn create_auth_interceptor(
    cred: DBCredentials,
) -> RawResult<impl Fn(InterceptorRequest) -> InterceptorResult<InterceptorRequest>> {
    let db_name = HeaderValue::from_str(cred.database.as_str()).map_err(|err| {
        RawError::custom(format!(
            "bad db name for set in headers '{}': {}",
            cred.database.as_str(),
            err
        ))
    })?;

    let build_info = HeaderValue::from_str("ydb-rs-sdk/0.0.0").unwrap();
    return Ok(move |mut req: InterceptorRequest| {
        let token = cred.token_cache.token();
        let token = HeaderValue::from_str(token.as_str()).map_err(|err| {
            InterceptorError::custom(format!("received bad token (len={}): {}", token.len(), err))
        })?;

        req.headers_mut().insert("x-ydb-database", db_name.clone());
        req.headers_mut()
            .insert("x-ydb-sdk-build-info", build_info.clone());
        req.headers_mut().insert("x-ydb-auth-ticket", token);
        Ok(req)
    });
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

        let token = match AsciiMetadataValue::from_str(self.cred.token_cache.token().as_str()) {
            Ok(val) => val,
            Err(err) => {
                return Err(Status::new(
                    Code::InvalidArgument,
                    format!("non-ascii token received for auth interceptor: {}", err),
                ))
            }
        };
        request.metadata_mut().insert("x-ydb-auth-ticket", token);
        request.metadata_mut().insert(
            "x-ydb-sdk-build-info",
            AsciiMetadataValue::from_str("ydb-go-sdk/0.0.0").unwrap(),
        );
        Ok(request)
    }
}
