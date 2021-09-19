use crate::errors::{Error, Result};
use crate::internal::query::{Query, QueryResult};
use crate::internal::session::SessionPool;
use async_trait::async_trait;
use std::sync::Arc;
use ydb_protobuf::generated::ydb::table::transaction_control::TxSelector;
use ydb_protobuf::generated::ydb::table::transaction_settings::TxMode;
use ydb_protobuf::generated::ydb::table::{
    ExecuteDataQueryRequest, OnlineModeSettings, TransactionControl, TransactionSettings,
};

enum Mode {
    ReadOnline,
}

impl Into<TxMode> for Mode {
    fn into(self) -> TxMode {
        match self {
            Self::ReadOnline => TxMode::OnlineReadOnly(OnlineModeSettings::default()),
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
    session_pool: Box<dyn SessionPool>,
}

#[async_trait]
impl Transaction for AutoCommit {
    async fn query(self: &mut Self, query: Query) -> Result<QueryResult> {
        return Err(Error::from("not implemented"));

        // let mut session = self.session_pool.session().await?;
        // let req = ExecuteDataQueryRequest {
        //     tx_control: Some(TransactionControl {
        //         commit_tx: true,
        //         tx_selector: Some(TxSelector::BeginTx(TransactionSettings {
        //             tx_mode: Some(TxMode::OnlineReadOnly(OnlineModeSettings {
        //                 allow_inconsistent_reads: true,
        //             })),
        //         })),
        //     }),
        //     query: Some(query.into()),
        //     ..ExecuteDataQueryRequest::default()
        // };
    }

    async fn commit(self: &mut Self) -> Result<()> {
        return Ok(());
    }

    async fn rollback(self: &mut Self) -> Result<()> {
        return Err(Error::from("impossible to rollback autocommit transaction"));
    }
}
