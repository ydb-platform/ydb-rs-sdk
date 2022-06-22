use crate::client_common::DBCredentials;
use tonic::codegen::InterceptedService;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;
use tonic::{Code, Status};

pub(crate) type ServiceWithAuth<S> = InterceptedService<S, AuthInterceptor>;

pub(crate) fn create_service_with_auth<S>(service: S, cred: DBCredentials) -> ServiceWithAuth<S> {
    return ServiceWithAuth::new(service, AuthInterceptor { cred });
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
        return Ok(request);
    }
}
