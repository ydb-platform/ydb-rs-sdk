use crate::errors::*;
use crate::internal::client_fabric::Middleware;
use crate::internal::grpc::grpc_read_operation_result;
use derivative::Derivative;
use ydb_protobuf::generated::ydb::table::v1::table_service_client::TableServiceClient;
use ydb_protobuf::generated::ydb::table::{ExecuteDataQueryRequest, ExecuteQueryResult};

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Session {
    client: TableServiceClient<Middleware>,
    pub(crate) id: String,

    #[derivative(Debug = "ignore")]
    on_drop_callbacks: Vec<Box<dyn FnOnce() + Send>>,
}

impl Session {
    pub(crate) fn new(client: TableServiceClient<Middleware>, id: String) -> Self {
        return Self {
            client,
            id,
            on_drop_callbacks: Vec::new(),
        };
    }

    pub(crate) fn on_drop(&mut self, f: Box<dyn FnOnce() + Send>) {
        self.on_drop_callbacks.push(f)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        println!("drop session: {}", &self.id);
        while let Some(on_drop) = self.on_drop_callbacks.pop() {
            on_drop()
        }
    }
}
