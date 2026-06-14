//! Query Service public facade (<https://github.com/ydb-platform/ydb-rs-sdk/issues/207>).
//!
//! Phase 1: streaming [`QueryExecutor::query`] with implicit sessions under the hood.

mod builders;
mod exec;

#[cfg(test)]
mod integration_test;

use std::sync::Arc;
use std::time::Duration;

use crate::client::TimeoutSettings;
use crate::discovery::Discovery;
use crate::errors::YdbResult;
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::result::{ResultSet, Row};

use exec::{new_exec_context, ClientExecContext};

pub use builders::{CallBuilder, QueryExecutor, QueryStreamBuilder, Streamed};

/// Row-to-struct mapping (the sqlx `FromRow` analogue).
pub trait FromYdbRow: Sized {
    fn from_row(row: Row) -> YdbResult<Self>;
}

impl FromYdbRow for Row {
    fn from_row(row: Row) -> YdbResult<Self> {
        Ok(row)
    }
}

pub struct QueryClient {
    ctx: ClientExecContext,
}

impl Clone for QueryClient {
    fn clone(&self) -> Self {
        Self {
            ctx: self.ctx.clone(),
        }
    }
}

impl QueryClient {
    pub(crate) fn new(
        connection_manager: GrpcConnectionManager,
        timeouts: TimeoutSettings,
        _discovery: Arc<Box<dyn Discovery>>,
    ) -> Self {
        Self {
            ctx: new_exec_context(connection_manager, timeouts),
        }
    }

    pub fn clone_with_idempotent_operations(&self, idempotent: bool) -> Self {
        Self {
            ctx: ClientExecContext {
                idempotent_operation: idempotent,
                ..self.ctx.clone()
            },
        }
    }

    pub fn clone_with_retry_timeout(&self, timeout: Duration) -> Self {
        Self {
            ctx: ClientExecContext {
                retry_budget: timeout,
                ..self.ctx.clone()
            },
        }
    }

    pub fn clone_with_no_retry(&self) -> Self {
        Self {
            ctx: ClientExecContext {
                retry_budget: Duration::ZERO,
                ..self.ctx.clone()
            },
        }
    }

    pub fn query(&mut self, text: impl Into<String>) -> QueryStreamBuilder<'_> {
        QueryExecutor::query(self, text)
    }
}

impl QueryExecutor for QueryClient {
    fn query(&mut self, text: impl Into<String>) -> QueryStreamBuilder<'_> {
        CallBuilder::new(&mut self.ctx, text.into())
    }
}

pub struct QueryStream {
    pub(crate) stream: crate::grpc_wrapper::raw_query_service::stream::ExecuteQueryStream,
}

impl Drop for QueryStream {
    fn drop(&mut self) {
        self.stream.cancel();
    }
}

impl QueryStream {
    pub async fn next_result_set(&mut self) -> YdbResult<Option<ResultSet>> {
        match self.stream.next_result_set().await? {
            Some((raw, _)) => Ok(Some(ResultSet::try_from(raw)?)),
            None => Ok(None),
        }
    }

    pub fn stats(&self) -> Option<QueryStats> {
        self.stream
            .stats()
            .map(|total_duration| QueryStats { total_duration })
    }

    pub async fn close(mut self) -> YdbResult<()> {
        self.stream.close().await.map_err(crate::errors::YdbError::from)?;
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct QueryStats {
    pub total_duration: Duration,
}
