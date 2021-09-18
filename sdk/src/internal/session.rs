use crate::errors::Result;
use crate::internal::grpc::{grpc_read_result, grpc_read_void_result};
use crate::internal::middlewares::AuthService;
use async_trait::async_trait;
use ydb_protobuf::generated::ydb::operations::OperationParams;
use ydb_protobuf::generated::ydb::table::v1::table_service_client::TableServiceClient;
use ydb_protobuf::generated::ydb::table::{
    CreateSessionRequest, CreateSessionResult, DeleteSessionRequest, ExecuteDataQueryRequest,
    ExecuteQueryResult,
};

#[derive(Debug)]
pub(crate) struct Session {
    client: TableServiceClient<AuthService>,
    id: String,
}

impl Session {
    pub async fn execute(
        self: &mut Self,
        mut req: ExecuteDataQueryRequest,
    ) -> Result<ExecuteQueryResult> {
        req.session_id = self.id.clone();
        grpc_read_result(self.client.execute_data_query(req).await?)
    }

    pub async fn delete(self: &mut Self, params: Option<OperationParams>) -> Result<()> {
        println!("deleting session: {}", self.id);
        grpc_read_void_result(
            self.client
                .delete_session(DeleteSessionRequest {
                    session_id: self.id.clone(),
                    operation_params: params,
                })
                .await?,
        )
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // todo
    }
}

#[async_trait]
pub(crate) trait SessionPool {
    async fn session(
        self: &mut Self,
        client: TableServiceClient<AuthService>,
        req: CreateSessionRequest,
    ) -> Result<Session>;

    fn fast_put_session(self: &mut Self, s: Session);
}

pub(crate) struct SimpleSessionPool {}

impl SimpleSessionPool {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl SessionPool for SimpleSessionPool {
    async fn session(
        self: &mut Self,
        mut client: TableServiceClient<AuthService>,
        req: CreateSessionRequest,
    ) -> Result<Session> {
        let res: CreateSessionResult = grpc_read_result(client.create_session(req).await?)?;
        return Ok(Session {
            client: client,
            id: res.session_id,
        });
    }

    fn fast_put_session(self: &mut Self, _s: Session) {}
}
