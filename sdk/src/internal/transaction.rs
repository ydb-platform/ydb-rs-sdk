use crate::errors::{YdbError, YdbResult};
use crate::internal::client_table::TableServiceChannelPool;
use crate::internal::query::Query;
use crate::internal::result::QueryResult;
use crate::internal::session::Session;
use crate::internal::session_pool::SessionPool;
use async_trait::async_trait;
use ydb_protobuf::generated::ydb::table::transaction_control::TxSelector;
use ydb_protobuf::generated::ydb::table::transaction_settings::TxMode;
use ydb_protobuf::generated::ydb::table::{
    ExecuteDataQueryRequest, OnlineModeSettings, SerializableModeSettings, TransactionControl,
    TransactionSettings,
};

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
    async fn query(&mut self, query: Query) -> YdbResult<QueryResult>;
    async fn commit(&mut self) -> YdbResult<()>;
    async fn rollback(&mut self) -> YdbResult<()>;
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
    async fn query(&mut self, query: Query) -> YdbResult<QueryResult> {
        let req = ExecuteDataQueryRequest {
            tx_control: Some(TransactionControl {
                commit_tx: true,
                tx_selector: Some(TxSelector::BeginTx(TransactionSettings {
                    tx_mode: Some(self.mode.into()),
                })),
            }),
            query: Some(query.query_to_proto()),
            parameters: query.params_to_proto()?,
            ..ExecuteDataQueryRequest::default()
        };

        let mut session = self.session_pool.session().await?;
        return session
            .execute_data_query(req, self.error_on_truncate_response)
            .await;
    }

    async fn commit(&mut self) -> YdbResult<()> {
        return Ok(());
    }

    async fn rollback(&mut self) -> YdbResult<()> {
        return Err(YdbError::from(
            "impossible to rollback autocommit transaction",
        ));
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
            if let (Some(tx_id), Some(mut session)) = (self.id.take(), self.session.take()) {
                tokio::spawn(async move {
                    let _ = session.rollback_transaction(tx_id);
                });
            };
        };
    }
}

#[async_trait]
impl Transaction for SerializableReadWriteTx {
    async fn query(&mut self, query: Query) -> YdbResult<QueryResult> {
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
            query: Some(query.query_to_proto()),
            parameters: query.params_to_proto()?,
            ..ExecuteDataQueryRequest::default()
        };
        let query_result = session
            .execute_data_query(req, self.error_on_truncate_response)
            .await?;
        if self.id.is_none() {
            self.id = query_result.session_id.clone();
        };

        return Ok(query_result);
    }

    async fn commit(&mut self) -> YdbResult<()> {
        if self.comitted {
            // commit many times - ok
            return Ok(());
        }

        if self.finished {
            return Err(YdbError::Custom(
                format!("commit finished non comitted transaction: {:?}", &self.id).into(),
            ));
        }
        self.finished = true;

        let tx_id = if let Some(id) = &self.id {
            id
        } else {
            // commit non started transaction - ok
            self.comitted = true;
            return Ok(());
        };

        if let Some(session) = self.session.as_mut() {
            session.commit_transaction(tx_id.clone()).await?;
            self.comitted = true;
            return Ok(());
        } else {
            return Err(YdbError::InternalError(
                "commit transaction without session (internal error)".into(),
            ));
        }
    }

    async fn rollback(&mut self) -> YdbResult<()> {
        // double rollback is ok
        if self.rollbacked {
            return Ok(());
        }

        if self.finished {
            return Err(YdbError::Custom(
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

        let tx_id = if let Some(id) = &self.id {
            id.clone()
        } else {
            // rollback non started transaction - ok
            self.rollbacked = true;
            return Ok(());
        };

        self.rollbacked = true;

        return session.rollback_transaction(tx_id).await;
    }
}
