use crate::errors::{Error, Result};
use crate::internal::grpc::{grpc_read_result, grpc_read_void_result};
use crate::internal::middlewares::AuthService;
use async_trait::async_trait;
use derivative::Derivative;
use futures::executor::block_on;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, RwLock};
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot};
use ydb_protobuf::generated::ydb::operations::OperationParams;
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
    on_drop: Box<dyn Fn()>,
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
pub(crate) trait SessionPool {
    async fn session(
        self: &mut Self,
        client: TableServiceClient<AuthService>,
        req: CreateSessionRequest,
    ) -> Result<Session>;
}

struct ClientSessionID {
    pub client: TableServiceClient<AuthService>,
    pub session_id: String,
}

pub(crate) struct SimpleSessionPool {
    close_session_sender: RwLock<Option<mpsc::UnboundedSender<ClientSessionID>>>,
}

impl SimpleSessionPool {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let (close_finished_sender, close_finished_receiver) = oneshot::channel::<()>();
        tokio::spawn(async move { Self::close_sessions(close_finished_sender, receiver).await });

        Self {
            close_session_sender: RwLock::new(Some(sender)),
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

    async fn close_sessions(
        mut close_finished: oneshot::Sender<()>,
        mut receiver: mpsc::UnboundedReceiver<ClientSessionID>,
    ) {
        println!("close loop");
        while let Some(mut pair) = receiver.recv().await {
            println!("drop-received: {}", pair.session_id);
            let session_id = pair.session_id.clone();
            let res = Self::close_session(pair).await;
            let mut stdout = tokio::io::stdout();
            stdout.write_all(
                format!(
                    "session deleted. id: '{}', error_status: {:?}",
                    session_id,
                    res.err()
                )
                .as_bytes(),
            );
        }
        println!("close loop finished");
        close_finished.send(());
    }
}

impl Drop for SimpleSessionPool {
    fn drop(&mut self) {
        *self.close_session_sender.write().unwrap() = None
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
        let mut sender = match self.close_session_sender.read().unwrap().deref() {
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
}
