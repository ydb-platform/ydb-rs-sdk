use crate::errors::{Error, Result};
use crate::internal::query::{Query, QueryResult};
use crate::internal::session_pool::SessionPool;
use async_trait::async_trait;
use ydb_protobuf::generated::ydb::table::transaction_control::TxSelector;
use ydb_protobuf::generated::ydb::table::transaction_settings::TxMode;
use ydb_protobuf::generated::ydb::table::{
    ExecuteDataQueryRequest, OnlineModeSettings, TransactionControl, TransactionSettings,
};

#[derive(Copy, Clone)]
pub enum Mode {
    ReadOnline,
}

impl From<Mode> for TxMode {
    fn from(m: Mode) -> Self {
        match m {
            Mode::ReadOnline => TxMode::OnlineReadOnly(OnlineModeSettings::default()),
        }
    }
}

#[async_trait]
pub trait Transaction {
    async fn query(self: &mut Self, query: Query) -> Result<QueryResult>;
    async fn commit(self: &mut Self) -> Result<()>;
    async fn rollback(self: &mut Self) -> Result<()>;
}

pub struct AutoCommit {
    mode: Mode,
    error_on_truncate_response: bool,
    session_pool: SessionPool,
}

impl AutoCommit {
    pub(crate) fn new(session_pool: SessionPool, mode: Mode) -> Self {
        return Self {
            mode,
            session_pool,
            error_on_truncate_response: true,
        };
    }

    pub fn with_error_on_truncate(mut self, error_on_truncate: bool) -> Self {
        self.error_on_truncate_response = error_on_truncate;
        return self;
    }
}

#[async_trait]
impl Transaction for AutoCommit {
    async fn query(self: &mut Self, query: Query) -> Result<QueryResult> {
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
        let proto_res = session.execute(req).await?;
        println!("res: {:?}", proto_res);
        return QueryResult::from_proto(proto_res, self.error_on_truncate_response);
    }

    async fn commit(self: &mut Self) -> Result<()> {
        return Ok(());
    }

    async fn rollback(self: &mut Self) -> Result<()> {
        return Err(Error::from("impossible to rollback autocommit transaction"));
    }
}
