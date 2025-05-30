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
use ydb_grpc::ydb_proto::table::{OnlineModeSettings, SerializableModeSettings};

#[derive(Clone, Debug)]
pub(crate) struct TransactionInfo {
    pub(crate) transaction_id: String,
    pub(crate) session_id: String,
}

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

impl From<Mode> for RawTxMode {
    fn from(value: Mode) -> Self {
        match value {
            Mode::OnlineReadonly => Self::OnlineReadOnly(RawOnlineReadonlySettings {
                allow_inconsistent_reads: false,
            }),
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
        Err(YdbError::custom("Transaction info not available for this transaction type"))
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
        if !self.finished {
            if let (Some(tx_id), Some(mut session)) = (self.id.take(), self.session.take()) {
                tokio::spawn(async move {
                    let _ = session.rollback_transaction(tx_id).await;
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
            .await?;
        if self.id.is_none() {
            self.id = Some(query_result.tx_id.clone());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test] 
    async fn test_transaction_info_default_implementation() {
        // Test the default error message for transaction_info method
        // We can't easily test AutoCommit without complex setup, but we can test the trait default
        struct MockTransaction;
        
        #[async_trait::async_trait]
        impl Transaction for MockTransaction {
            async fn query(&mut self, _query: Query) -> YdbResult<QueryResult> {
                unimplemented!()
            }
            async fn commit(&mut self) -> YdbResult<()> {
                unimplemented!()
            }
            async fn rollback(&mut self) -> YdbResult<()> {
                unimplemented!()
            }
            // Use default implementation of transaction_info
        }
        
        let mut mock_tx = MockTransaction;
        let result = mock_tx.transaction_info().await;
        
        assert!(result.is_err());
        if let Err(YdbError::Custom(msg)) = result {
            assert!(msg.contains("Transaction info not available for this transaction type"));
        } else {
            panic!("Expected Custom error with specific message");
        }
    }
}
