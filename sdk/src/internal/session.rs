use crate::errors::{Error, Result};
use crate::internal::grpc::{grpc_read_result, grpc_read_void_result};
use crate::internal::middlewares::AuthService;
use async_trait::async_trait;
use derivative::Derivative;
use std::ops::Deref;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use ydb_protobuf::generated::ydb::table::v1::table_service_client::TableServiceClient;
use ydb_protobuf::generated::ydb::table::{
    CreateSessionRequest, CreateSessionResult, DeleteSessionRequest, ExecuteDataQueryRequest,
    ExecuteQueryResult,
};

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Session {
    client: TableServiceClient<AuthService>,
    id: String,

    #[derivative(Debug = "ignore")]
    on_drop: Box<dyn Fn() + Send>,
}

impl Session {
    pub async fn execute(
        self: &mut Self,
        mut req: ExecuteDataQueryRequest,
    ) -> Result<ExecuteQueryResult> {
        req.session_id = self.id.clone();
        grpc_read_result(self.client.execute_data_query(req).await?)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        println!("drop");
        (self.on_drop)();
    }
}

#[async_trait]
pub(crate) trait SessionPool: Sync + Send {
    async fn session(&self) -> Result<Session>;
    fn clone_pool(&self) -> Box<dyn SessionPool>;
}

struct ClientSessionID {
    pub client: TableServiceClient<AuthService>,
    pub session_id: String,
}

struct SimpleSessionPoolSharedState {
    client: TableServiceClient<AuthService>,
    close_session_sender: RwLock<Option<mpsc::UnboundedSender<ClientSessionID>>>,
}

impl Drop for SimpleSessionPoolSharedState {
    fn drop(&mut self) {
        *self.close_session_sender.write().unwrap() = None
        // it is question about need to close all session from pool
    }
}

#[derive(Clone)]
pub(crate) struct SimpleSessionPool {
    shared_state: Arc<SimpleSessionPoolSharedState>,
}

impl SimpleSessionPool {
    pub fn new(client: TableServiceClient<AuthService>) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(async move { Self::close_sessions(receiver).await });

        Self {
            shared_state: Arc::new(SimpleSessionPoolSharedState {
                client,
                close_session_sender: RwLock::new(Some(sender)),
            }),
        }
    }

    async fn close_session(mut pair: ClientSessionID) -> Result<()> {
        grpc_read_void_result(
            pair.client
                .delete_session(DeleteSessionRequest {
                    session_id: pair.session_id,
                    operation_params: None,
                })
                .await?,
        )
    }

    async fn close_sessions(mut receiver: mpsc::UnboundedReceiver<ClientSessionID>) {
        println!("close loop");
        while let Some(pair) = receiver.recv().await {
            println!("drop-received: {}", pair.session_id);
            let session_id = pair.session_id.clone();
            let res = Self::close_session(pair).await;
            println!(
                "session deleted. id: '{}', error_status: {:?}",
                session_id,
                res.err()
            );
        }
        println!("close loop finished");
    }
}

#[async_trait]
impl SessionPool for SimpleSessionPool {
    async fn session(&self) -> Result<Session> {
        let mut client = self.shared_state.client.clone();
        let res: CreateSessionResult = grpc_read_result(
            client
                .create_session(CreateSessionRequest::default())
                .await?,
        )?;
        let sender = match self
            .shared_state
            .close_session_sender
            .read()
            .unwrap()
            .deref()
        {
            Some(sender) => sender.clone(),
            None => return Err(Error::Custom("pool closed".into())),
        };
        let session_id = res.session_id.clone();
        return Ok(Session {
            client: client.clone(),
            id: session_id.clone(),
            on_drop: Box::new(move || {
                println!("on-drop");
                let _ = sender.send(ClientSessionID {
                    client: client.clone(),
                    session_id: session_id.clone(),
                });
                println!("drop message sended")
            }),
        });
    }

    fn clone_pool(&self) -> Box<dyn SessionPool> {
        Box::new(self.clone())
    }
}
