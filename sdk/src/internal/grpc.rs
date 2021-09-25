use crate::credentials::Credentials;
use crate::errors::Result;
use crate::internal::discovery::{Discovery, Service};
use crate::internal::grpc_helper::create_grpc_client;
use crate::internal::middlewares::AuthService;

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
}

impl ClientFabric for SimpleGrpcClientFabric {
    fn create<T, CB>(self: &Self, new_func: CB, service: Service) -> Result<T>
    where
        CB: FnOnce(AuthService) -> T,
    {
        return create_grpc_client(
            self.discovery.endpoint(service)?,
            self.cred.clone(),
            self.database.clone(),
            new_func,
        );
    }
}
