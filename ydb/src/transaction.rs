use crate::client::TimeoutSettings;
use crate::errors::{YdbError, YdbResult};
use crate::grpc_wrapper::raw_table_service::execute_data_query::RawExecuteDataQueryRequest;
use crate::grpc_wrapper::raw_table_service::query_stats::RawQueryStatMode;
use crate::grpc_wrapper::raw_table_service::transaction_control::{
    RawOnlineReadonlySettings, RawTransactionControl, RawTxMode, RawTxSelector, RawTxSettings,
};
use crate::query::Query;
use crate::result::QueryResult;
use crate::session::Session;
use crate::session_pool::SessionPool;
use async_trait::async_trait;
use itertools::Itertools;
use tracing::trace;
use ydb_grpc::ydb_proto::table::transaction_settings::TxMode;
use ydb_grpc::ydb_proto::table::{
    OnlineModeSettings, SerializableModeSettings, SnapshotModeSettings,
};

#[derive(Clone, Debug)]
pub struct TransactionInfo {
    pub(crate) transaction_id: String,
    pub(crate) session_id: String,
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum Mode {
    OnlineReadonly,
    SnapshotReadOnly,
    SerializableReadWrite,
}

impl From<Mode> for TxMode {
    fn from(m: Mode) -> Self {
        match m {
            Mode::OnlineReadonly => TxMode::OnlineReadOnly(OnlineModeSettings::default()),
            Mode::SnapshotReadOnly => TxMode::SnapshotReadOnly(SnapshotModeSettings::default()),
            Mode::SerializableReadWrite => {
                TxMode::SerializableReadWrite(SerializableModeSettings::default())
            }
        }
    }
}

impl From<Mode> for RawTxMode {
    fn from(value: Mode) -> Self {
        match value {
            Mode::OnlineReadonly => Self::OnlineReadOnly(RawOnlineReadonlySettings {
                allow_inconsistent_reads: false,
            }),
            Mode::SnapshotReadOnly => Self::SnapshotReadOnly,
            Mode::SerializableReadWrite => Self::SerializableReadWrite,
        }
    }
}

#[async_trait]
pub trait Transaction: Send + Sync {
    async fn query(&mut self, query: Query) -> YdbResult<QueryResult>;
    async fn commit(&mut self) -> YdbResult<()>;
    async fn rollback(&mut self) -> YdbResult<()>;
    async fn transaction_info(&mut self) -> YdbResult<TransactionInfo> {
        Err(YdbError::custom(
            "Transaction info not available for this transaction type",
        ))
    }
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
        let req = RawExecuteDataQueryRequest {
            session_id: String::default(),
            tx_control: RawTransactionControl {
                commit_tx: true,
                tx_selector: RawTxSelector::Begin(RawTxSettings {
                    mode: self.mode.into(),
                }),
            },
            yql_text: query.text,
            operation_params: self.timeouts.operation_params(),
            params: query
                .parameters
                .into_iter()
                .map(|(k, v)| match v.try_into() {
                    Ok(converted) => Ok((k, converted)),
                    Err(err) => Err(err),
                })
                .try_collect()?,
            keep_in_cache: query.keep_in_cache,
            collect_stats: RawQueryStatMode::None,
        };

        let mut session = self.session_pool.session().await?;
        return session
            .execute_data_query(req, self.error_on_truncate_response)
            .await;
    }

    async fn commit(&mut self) -> YdbResult<()> {
        Ok(())
    }

    async fn rollback(&mut self) -> YdbResult<()> {
        Err(YdbError::from(
            "impossible to rollback autocommit transaction",
        ))
    }
}

pub(crate) struct SerializableReadWriteTx {
    error_on_truncate_response: bool,
    session_pool: SessionPool,

    id: Option<String>,
    session: Option<Session>,
    state: TableTxState,
    timeouts: TimeoutSettings,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableTxState {
    Active,
    Committed,
    RolledBack,
    /// Server ended the transaction after a definitive operation error on a query.
    ServerInvalidated,
}

impl SerializableReadWriteTx {
    pub(crate) fn new(session_pool: SessionPool, timeouts: TimeoutSettings) -> Self {
        Self {
            error_on_truncate_response: false,
            session_pool,

            id: None,
            session: None,
            state: TableTxState::Active,
            timeouts,
        }
    }

    pub(crate) fn with_error_on_truncate(mut self, error_on_truncate: bool) -> Self {
        self.error_on_truncate_response = error_on_truncate;
        self
    }

    fn on_query_error(&mut self, err: &YdbError) {
        if err.invalidates_server_transaction() {
            self.state = TableTxState::ServerInvalidated;
            self.id = None;
        }
    }

    // Private method for transaction initialization using "workaround"
    async fn begin_transaction(&mut self) -> YdbResult<()> {
        // Call query with simple request to create transaction
        let _ = self.query(Query::new("SELECT 1")).await?;
        Ok(())
    }
}

impl Drop for SerializableReadWriteTx {
    // rollback if unfinished
    fn drop(&mut self) {
        if self.state != TableTxState::Active {
            return;
        }
        let tx_id = self.id.take();
        let Some(mut session) = self.session.take() else {
            return;
        };
        if tx_id.is_none() {
            // Query may still be running on the server (timeout/cancel before tx_id).
            session.discard_from_pool();
            return;
        }
        // Successful read-only interactive txs keep the session poolable; rollback runs
        // in the background while the lease is still held via `Session` until it completes.
        tokio::spawn(async move {
            let _ = session.rollback_transaction(tx_id.unwrap()).await;
        });
    }
}

/// Whether an unfinished interactive transaction should mark its session non-poolable on drop.
#[cfg(test)]
pub(crate) fn unfinished_interactive_tx_drop_discards_session(tx_id: &Option<String>) -> bool {
    tx_id.is_none()
}

#[cfg(test)]
mod tx_state_tests {
    use crate::errors::{YdbError, YdbStatusError};
    use ydb_grpc::ydb_proto::status_ids::StatusCode;

    #[test]
    fn operational_query_error_is_detected() {
        let err = YdbError::YdbStatusError(YdbStatusError {
            message: "syntax".into(),
            operation_status: StatusCode::GenericError as i32,
            issues: vec![],
        });
        assert!(err.invalidates_server_transaction());
        assert!(!YdbError::Transport("timeout".into()).invalidates_server_transaction());
    }
}

#[cfg(test)]
mod drop_policy_tests {
    use super::unfinished_interactive_tx_drop_discards_session;

    #[test]
    fn discard_only_when_tx_id_missing() {
        assert!(unfinished_interactive_tx_drop_discards_session(&None));
        assert!(!unfinished_interactive_tx_drop_discards_session(&Some(
            "tx-1".to_string()
        )));
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
        trace!("session: {:#?}", &session);

        let tx_selector = if let Some(tx_id) = &self.id {
            trace!("tx_id: {}", tx_id);
            RawTxSelector::Id(tx_id.clone())
        } else {
            trace!("start new transaction");
            RawTxSelector::Begin(RawTxSettings {
                mode: RawTxMode::SerializableReadWrite,
            })
        };

        let req = RawExecuteDataQueryRequest {
            session_id: session.id.clone(),
            tx_control: RawTransactionControl {
                commit_tx: false,
                tx_selector,
            },
            yql_text: query.text,

            operation_params: self.timeouts.operation_params(),
            params: query
                .parameters
                .into_iter()
                .map(|(k, v)| match v.try_into() {
                    Ok(converted) => Ok((k, converted)),
                    Err(err) => Err(err),
                })
                .try_collect()?,
            keep_in_cache: false,
            collect_stats: RawQueryStatMode::None,
        };
        let query_result = session
            .execute_data_query(req, self.error_on_truncate_response)
            .await;
        if let Err(err) = &query_result {
            self.on_query_error(err);
            return query_result;
        }
        let query_result = query_result?;
        if self.id.is_none() {
            self.id = Some(query_result.tx_id.clone());
        };

        return Ok(query_result);
    }

    async fn commit(&mut self) -> YdbResult<()> {
        match self.state {
            TableTxState::Committed | TableTxState::ServerInvalidated => return Ok(()),
            TableTxState::RolledBack => {
                return Err(YdbError::Custom(format!(
                    "commit rolled back transaction: {:?}",
                    &self.id
                )));
            }
            TableTxState::Active => {}
        }

        let tx_id = if let Some(id) = &self.id {
            id.clone()
        } else {
            // commit non started transaction - ok
            self.state = TableTxState::Committed;
            return Ok(());
        };

        if let Some(session) = self.session.as_mut() {
            session.commit_transaction(tx_id).await?;
            self.state = TableTxState::Committed;
            return Ok(());
        }
        Err(YdbError::InternalError(
            "commit transaction without session (internal error)".into(),
        ))
    }

    async fn rollback(&mut self) -> YdbResult<()> {
        match self.state {
            // go-sdk: rollback after commit is a nop
            TableTxState::Committed
            | TableTxState::ServerInvalidated
            | TableTxState::RolledBack => {
                return Ok(());
            }
            TableTxState::Active => {}
        }

        let session = if let Some(session) = &mut self.session {
            session
        } else {
            // rollback non started transaction ok
            self.state = TableTxState::RolledBack;
            return Ok(());
        };

        let tx_id = if let Some(id) = &self.id {
            id.clone()
        } else {
            // rollback non started transaction - ok
            self.state = TableTxState::RolledBack;
            return Ok(());
        };

        session.rollback_transaction(tx_id).await?;
        self.state = TableTxState::RolledBack;
        Ok(())
    }

    async fn transaction_info(&mut self) -> YdbResult<TransactionInfo> {
        // If transaction_id or session_id are missing, create transaction
        if self.id.is_none() || self.session.is_none() {
            self.begin_transaction().await?;
        }

        Ok(TransactionInfo {
            transaction_id: self.id.clone().unwrap(),
            session_id: self.session.as_ref().unwrap().id.clone(),
        })
    }
}
