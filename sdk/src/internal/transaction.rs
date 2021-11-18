use crate::errors::{Error, Result};
use crate::internal::query::{Query, QueryResult};
use crate::internal::session_pool::SessionPool;
use async_trait::async_trait;
use ydb_protobuf::generated::ydb::table::transaction_control::TxSelector;
use ydb_protobuf::generated::ydb::table::transaction_settings::TxMode;
use ydb_protobuf::generated::ydb::table::{ExecuteDataQueryRequest, ExecuteQueryResult, OnlineModeSettings, SerializableModeSettings, TransactionControl, TransactionSettings};
use ydb_protobuf::generated::ydb::table::v1::table_service_client::TableServiceClient;
use crate::internal::channel_pool::ChannelPool;
use crate::internal::client_fabric::Middleware;
use crate::internal::grpc::grpc_read_result;
use crate::internal::session::Session;

#[derive(Copy, Clone)]
pub enum Mode {
    OnlineReadonly,
    SerializableReadWrite,
}

impl From<Mode> for TxMode {
    fn from(m: Mode) -> Self {
        match m {
            Mode::OnlineReadonly => TxMode::OnlineReadOnly(OnlineModeSettings::default()),
            SerializableReadWrite=> TxMode::SerializableReadWrite(SerializableModeSettings::default()),
        }
    }
}

#[async_trait]
pub trait Transaction {
    async fn query(&mut self, query: Query) -> Result<QueryResult>;
    async fn commit(&mut self) -> Result<()>;
    async fn rollback(&mut self) -> Result<()>;
}

pub struct AutoCommit {
    mode: Mode,
    error_on_truncate_response: bool,
    session_pool: SessionPool,
    channel_pool: ChannelPool<TableServiceClient<Middleware>>,
}

impl AutoCommit {
    pub(crate) fn new(channel_pool: ChannelPool<TableServiceClient<Middleware>>, session_pool: SessionPool, mode: Mode) -> Self {
        return Self {
            mode,
            channel_pool,
            session_pool,
            error_on_truncate_response: false,
        };
    }

    pub fn with_error_on_truncate(mut self, error_on_truncate: bool) -> Self {
        self.error_on_truncate_response = error_on_truncate;
        return self;
    }
}

#[async_trait]
impl Transaction for AutoCommit {
    async fn query(&mut self, query: Query) -> Result<QueryResult> {
        let mut session = self.session_pool.session().await?;
        let req = ExecuteDataQueryRequest {
            session_id: session.id.clone(),
            tx_control: Some(TransactionControl {
            commit_tx: true,
            tx_selector: Some(TxSelector::BeginTx(TransactionSettings {
                tx_mode: Some(self.mode.into()),
            })),
        }),
        ..query.to_proto()?
    };
        println!("session: {:#?}", &session);
        println!("req: {:#?}", &req);
        let proto_res: Result<ExecuteQueryResult> = grpc_read_result(self.channel_pool.create_channel()?.execute_data_query(req).await?);
        println!("res: {:#?}", proto_res);
        return QueryResult::from_proto(proto_res?, self.error_on_truncate_response);
    }

    async fn commit(&mut self) -> Result<()> {
        return Ok(());
    }

    async fn rollback(&mut self) -> Result<()> {
        return Err(Error::from("impossible to rollback autocommit transaction"));
    }
}

// pub struct SerializableReadWrite {
//     error_on_truncate_response: bool,
//     session_pool: SessionPool,
//
//     id: Option<String>,
//     session: Option<Session>,
//     finished: bool,
// }
//
// impl SerializableReadWrite {
//     pub(crate) fn new(session_pool: SessionPool) -> Self {
//         return Self {
//             session_pool,
//             error_on_truncate_response: false,
//
//             id: None,
//             session: None,
//             finished: false,
//         }
//     }
//
//     pub fn with_error_on_truncate(mut self, error_on_truncate: bool) -> Self {
//         self.error_on_truncate_response = error_on_truncate;
//         return self;
//     }
// }
//
// #[async_trait]
// impl Transaction for SerializableReadWrite {
//     async fn query(&mut self, query: Query) -> Result<QueryResult>{
//         todo!();
//     }
//
//     async fn commit(&mut self) -> Result<()>{
//         if !self.finished {
//             self.finished
//         }
//
//         if let Some(session) = &self.session {
//             session.execute()
//         }
//     }
//     async fn rollback(&mut self) -> Result<()>{
//         todo!();
//     }
//
// }