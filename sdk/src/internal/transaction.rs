use crate::errors::{Error, Result};
use crate::internal::query::{Query, QueryResult};
use crate::internal::session_pool::SessionPool;
use async_trait::async_trait;
use ydb_protobuf::generated::ydb::table::transaction_control::TxSelector;
use ydb_protobuf::generated::ydb::table::transaction_settings::TxMode;
use ydb_protobuf::generated::ydb::table::{
    CommitTransactionRequest, CommitTransactionResult, ExecuteDataQueryRequest, ExecuteQueryResult,
    OnlineModeSettings, RollbackTransactionRequest, SerializableModeSettings, TransactionControl,
    TransactionSettings,
};

use crate::errors::Error::Custom;
use crate::internal::client_table::{TableServiceChannelPool, TableServiceClientType};
use crate::internal::grpc::{grpc_read_operation_result, grpc_read_void_operation_result};
use crate::internal::session::Session;

#[derive(Copy, Clone, PartialEq)]
pub enum Mode {
    OnlineReadonly,
    SerializableReadWrite,
}

impl From<Mode> for TxMode {
    fn from(m: Mode) -> Self {
        match m {
            Mode::OnlineReadonly => TxMode::OnlineReadOnly(OnlineModeSettings::default()),
            Mode::SerializableReadWrite => {
                TxMode::SerializableReadWrite(SerializableModeSettings::default())
            }
        }
    }
}

#[async_trait]
pub trait Transaction {
    async fn query(&mut self, query: Query) -> Result<QueryResult>;
    async fn commit(&mut self) -> Result<()>;
    async fn rollback(&mut self) -> Result<()>;
}

// TODO: operations timeout

pub struct AutoCommit {
    mode: Mode,
    error_on_truncate_response: bool,
    session_pool: SessionPool,
    channel_pool: TableServiceChannelPool,
}

impl AutoCommit {
    pub(crate) fn new(
        channel_pool: TableServiceChannelPool,
        session_pool: SessionPool,
        mode: Mode,
    ) -> Self {
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

impl Drop for AutoCommit {
    fn drop(&mut self) {}
}

#[async_trait]
impl Transaction for AutoCommit {
    async fn query(&mut self, query: Query) -> Result<QueryResult> {
        let req = ExecuteDataQueryRequest {
            tx_control: Some(TransactionControl {
                commit_tx: true,
                tx_selector: Some(TxSelector::BeginTx(TransactionSettings {
                    tx_mode: Some(self.mode.into()),
                })),
            }),
            ..query.to_proto()?
        };

        let mut session = self.session_pool.session().await?;
        return session
            .execute_data_query(req, self.error_on_truncate_response)
            .await;
    }

    async fn commit(&mut self) -> Result<()> {
        return Ok(());
    }

    async fn rollback(&mut self) -> Result<()> {
        return Err(Error::from("impossible to rollback autocommit transaction"));
    }
}

pub struct SerializableReadWriteTx {
    error_on_truncate_response: bool,
    session_pool: SessionPool,
    channel_pool: TableServiceChannelPool,

    id: Option<String>,
    session: Option<Session>,
    comitted: bool,
    rollbacked: bool,
    finished: bool,
}

impl SerializableReadWriteTx {
    pub(crate) fn new(channel_pool: TableServiceChannelPool, session_pool: SessionPool) -> Self {
        return Self {
            error_on_truncate_response: false,
            session_pool,
            channel_pool,

            id: None,
            session: None,
            comitted: false,
            rollbacked: false,
            finished: false,
        };
    }

    pub fn with_error_on_truncate(mut self, error_on_truncate: bool) -> Self {
        self.error_on_truncate_response = error_on_truncate;
        return self;
    }
}

impl Drop for SerializableReadWriteTx {
    // rollback if unfinished
    fn drop(&mut self) {
        if !self.finished {
            let session_id = if let Some(session) = &self.session {
                session.id.clone()
            } else {
                return;
            };

            let tx_id = if let Some(tx_id) = &self.id {
                tx_id.clone()
            } else {
                return;
            };

            let pool = self.channel_pool.clone();
            tokio::spawn(async move {
                if let Ok(ch) = pool.create_channel().await {
                    // todo: handle session error
                    let _ = rollback_request(ch, session_id, tx_id).await;
                } else {
                    return;
                };
            });
        };
        return;
    }
}

#[async_trait]
impl Transaction for SerializableReadWriteTx {
    async fn query(&mut self, query: Query) -> Result<QueryResult> {
        let session = if let Some(session) = self.session.as_mut() {
            session
        } else {
            self.session = Some(self.session_pool.session().await?);
            println!("create session from transaction");
            self.session.as_mut().unwrap()
        };
        println!("session: {:#?}", session);

        let tx_selector = if let Some(tx_id) = &self.id {
            println!("tx_id: {}", tx_id);
            TxSelector::TxId(tx_id.clone())
        } else {
            println!("start new transaction");
            TxSelector::BeginTx(TransactionSettings {
                tx_mode: Some(Mode::SerializableReadWrite.into()),
                ..TransactionSettings::default()
            })
        };

        let req = ExecuteDataQueryRequest {
            tx_control: Some(TransactionControl {
                commit_tx: false,
                tx_selector: Some(tx_selector),
                ..TransactionControl::default()
            }),
            ..query.to_proto()?
        };
        let query_result = session
            .execute_data_query(req, self.error_on_truncate_response)
            .await?;
        if self.id.is_none() {
            self.id = query_result.session_id.clone();
        };

        return Ok(query_result);
    }

    async fn commit(&mut self) -> Result<()> {
        if self.comitted {
            // commit many times - ok
            return Ok(());
        }

        if self.finished {
            return Err(Error::Custom(
                format!("commit finished non comitted transaction: {:?}", &self.id).into(),
            ));
        }
        self.finished = true;

        let id = if let Some(id) = &self.id {
            id
        } else {
            // commit non started transaction - ok
            self.comitted = true;
            return Ok(());
        };

        let req = CommitTransactionRequest {
            session_id: self.session.as_mut().unwrap().id.clone(),
            tx_id: id.clone(),
            ..CommitTransactionRequest::default()
        };

        let mut ch = self.channel_pool.create_channel().await?;

        // todo - retries
        let _res: CommitTransactionResult =
            self.session
                .as_mut()
                .unwrap()
                .handle_error(grpc_read_operation_result(
                    ch.commit_transaction(req).await?,
                ))?;

        self.comitted = true;
        return Ok(());
    }

    async fn rollback(&mut self) -> Result<()> {
        // double rollback is ok
        if self.rollbacked {
            return Ok(());
        }

        if self.finished {
            return Err(Error::Custom(
                format!(
                    "rollback finished non rollbacked transaction: {:?}",
                    &self.id
                )
                .into(),
            ));
        }
        self.finished = true;

        let session = if let Some(session) = &mut self.session {
            session
        } else {
            // rollback non started transaction ok
            self.finished = true;
            self.rollbacked = true;
            return Ok(());
        };

        let id = if let Some(id) = &self.id {
            id.clone()
        } else {
            // rollback non started transaction - ok
            self.rollbacked = true;
            return Ok(());
        };

        self.rollbacked = true;
        return session.handle_error(
            rollback_request(
                self.channel_pool.create_channel().await?,
                session.id.clone(),
                id,
            )
            .await,
        );
    }
}

async fn rollback_request(
    mut ch: TableServiceClientType,
    session_id: String,
    tx_id: String,
) -> Result<()> {
    let req = RollbackTransactionRequest {
        session_id,
        tx_id,
        ..RollbackTransactionRequest::default()
    };

    // todo retries
    grpc_read_void_operation_result(ch.rollback_transaction(req).await?)?;

    return Ok(());
}
