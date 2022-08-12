use crate::client::TimeoutSettings;
use crate::errors::{YdbError, YdbResult};
use crate::grpc::operation_params;
use crate::query::Query;
use crate::result::QueryResult;
use crate::session::Session;
use crate::session_pool::SessionPool;
use async_trait::async_trait;
use tracing::trace;
use ydb_grpc::ydb_proto::table::transaction_control::TxSelector;
use ydb_grpc::ydb_proto::table::transaction_settings::TxMode;
use ydb_grpc::ydb_proto::table::{
    ExecuteDataQueryRequest, OnlineModeSettings, SerializableModeSettings, TransactionControl,
    TransactionSettings,
};

#[derive(Copy, Clone, PartialEq, Eq)]
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
pub trait Transaction: Send + Sync {
    async fn query(&mut self, query: Query) -> YdbResult<QueryResult>;
    async fn commit(&mut self) -> YdbResult<()>;
    async fn rollback(&mut self) -> YdbResult<()>;
}

// TODO: operations timeout

pub(crate) struct AutoCommit {
    mode: Mode,
    error_on_truncate_response: bool,
    session_pool: SessionPool,
    timeouts: TimeoutSettings,
}

impl AutoCommit {
    pub(crate) fn new(session_pool: SessionPool, mode: Mode, timeouts: TimeoutSettings) -> Self {
        Self {
            mode,
            session_pool,
            error_on_truncate_response: false,
            timeouts,
        }
    }

    pub(crate) fn with_error_on_truncate(mut self, error_on_truncate: bool) -> Self {
        self.error_on_truncate_response = error_on_truncate;
        self
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
            operation_params: operation_params(self.timeouts.operation_timeout),
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

pub(crate) struct SerializableReadWriteTx {
    error_on_truncate_response: bool,
    session_pool: SessionPool,

    id: Option<String>,
    session: Option<Session>,
    comitted: bool,
    rollbacked: bool,
    finished: bool,
    timeouts: TimeoutSettings,
}

impl SerializableReadWriteTx {
    pub(crate) fn new(session_pool: SessionPool, timeouts: TimeoutSettings) -> Self {
        Self {
            error_on_truncate_response: false,
            session_pool,

            id: None,
            session: None,
            comitted: false,
            rollbacked: false,
            finished: false,
            timeouts,
        }
    }

    pub(crate) fn with_error_on_truncate(mut self, error_on_truncate: bool) -> Self {
        self.error_on_truncate_response = error_on_truncate;
        self
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
            trace!("create session from transaction");
            self.session.as_mut().unwrap()
        };
        trace!("session: {:#?}", session);

        let tx_selector = if let Some(tx_id) = &self.id {
            trace!("tx_id: {}", tx_id);
            TxSelector::TxId(tx_id.clone())
        } else {
            trace!("start new transaction");
            TxSelector::BeginTx(TransactionSettings {
                tx_mode: Some(Mode::SerializableReadWrite.into()),
            })
        };

        let req = ExecuteDataQueryRequest {
            tx_control: Some(TransactionControl {
                commit_tx: false,
                tx_selector: Some(tx_selector),
            }),
            query: Some(query.query_to_proto()),
            parameters: query.params_to_proto()?,
            operation_params: operation_params(self.timeouts.operation_timeout),
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
            return Err(YdbError::Custom(format!(
                "commit finished non comitted transaction: {:?}",
                &self.id
            )));
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
            return Err(YdbError::Custom(format!(
                "rollback finished non rollbacked transaction: {:?}",
                &self.id
            )));
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
