use std::time::Instant;

use crate::errors::{YdbError, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_query_service::transaction_control::TransactionId;
use crate::result::Row;
use crate::{TransactionOptions, TxMode};

use crate::session_pool::SessionPoolLease;

use super::builders::{
    CallBuilder, ExecBuilder, Interactive, QueryExecutor, QueryRowBuilder, QueryStreamBuilder,
    ResultSetBuilder, impl_transaction_query_methods,
};
use super::hooks::QueryTxHook;

mod rpc;
mod state;

pub(crate) use state::TransactionExecContext;
use state::TxState;
#[cfg(test)]
use state::{InFlightOperation, ServerTransaction};

pub(super) enum AttemptCompletion {
    Return,
    Retry(YdbError),
    Fail(YdbError),
}

pub(super) enum FailedAttemptCompletion {
    Retry,
    FailCallback,
    FailTransaction(YdbError),
}

pub struct Transaction {
    pub(super) ctx: TransactionExecContext,
}

impl Transaction {
    impl_transaction_query_methods!();

    pub(super) fn new(
        connection_manager: GrpcConnectionManager,
        lease: SessionPoolLease,
        options: TransactionOptions,
        retry_deadline: Option<Instant>,
    ) -> Self {
        Self {
            ctx: TransactionExecContext::new(connection_manager, lease, options, retry_deadline),
        }
    }

    pub fn mode(&self) -> TxMode {
        self.ctx.tx_mode
    }

    pub(crate) fn register_hook(&mut self, hook: impl QueryTxHook) -> YdbResult<()> {
        self.ctx.register_hook(Box::new(hook))
    }

    /// Explicitly open the transaction via `BeginTransaction` RPC.
    ///
    /// By default (lazy tx) the transaction materializes on the first query. Call this when you
    /// need `tx_id` before any YQL, or configure [`TransactionOptions::with_begin`]
    /// on the client so the first operation does this automatically.
    pub async fn begin(&mut self) -> YdbResult<()> {
        self.ctx.ensure_begin().await
    }

    /// Session and transaction ids for topic offset updates inside a transaction.
    pub(crate) async fn identity(&mut self) -> YdbResult<QueryTxIdentity> {
        self.ctx.identity().await
    }

    pub async fn rollback(&mut self) -> YdbResult<()> {
        self.ctx.rollback().await
    }

    async fn commit(&mut self) -> YdbResult<()> {
        self.ctx.commit().await
    }

    pub(super) async fn finish_successful_attempt(&mut self) -> AttemptCompletion {
        match &self.ctx.state {
            TxState::Committed | TxState::RolledBack => return AttemptCompletion::Return,
            TxState::Invalidated(error) => return AttemptCompletion::Retry(error.clone()),
            TxState::Ambiguous(error) => return AttemptCompletion::Fail(error.clone()),
            TxState::Live(_) => {}
        }

        if self.ctx.state.operation_is_in_flight() {
            let error = YdbError::InternalError(
                "query transaction callback ended while an operation was still in progress"
                    .to_string(),
            );
            return match self.ctx.abort_unconfirmed(error.clone()) {
                Ok(()) => AttemptCompletion::Fail(error),
                Err(invariant) => AttemptCompletion::Fail(invariant),
            };
        }

        match self.commit().await {
            Ok(()) => AttemptCompletion::Return,
            // Commit outcome is ambiguous on transport errors; never retry.
            Err(error) => AttemptCompletion::Fail(error),
        }
    }

    pub(super) async fn finish_failed_attempt(&mut self) -> FailedAttemptCompletion {
        match &self.ctx.state {
            TxState::RolledBack | TxState::Invalidated(_) => {
                return FailedAttemptCompletion::Retry;
            }
            TxState::Committed => return FailedAttemptCompletion::FailCallback,
            TxState::Ambiguous(error) if error.invalidates_server_transaction() => {
                return FailedAttemptCompletion::Retry;
            }
            TxState::Ambiguous(error) => {
                return FailedAttemptCompletion::FailTransaction(error.clone());
            }
            TxState::Live(_) => {}
        }
        if self.ctx.state.operation_is_in_flight() {
            let error = YdbError::InternalError(
                "query transaction callback failed while an operation was still in progress"
                    .to_string(),
            );
            return match self.ctx.abort_unconfirmed(error.clone()) {
                Ok(()) => FailedAttemptCompletion::FailTransaction(error),
                Err(invariant) => FailedAttemptCompletion::FailTransaction(invariant),
            };
        }
        match self.ctx.rollback().await {
            Ok(()) => FailedAttemptCompletion::Retry,
            Err(error) => FailedAttemptCompletion::FailTransaction(error),
        }
    }

    pub(crate) async fn uri(&mut self) -> YdbResult<&http::Uri> {
        self.ctx.ensure_begin().await?;
        Ok(self.ctx.session_lease()?.node_uri())
    }

    #[cfg(test)]
    pub(crate) fn transaction_id_for_test(&self) -> Option<&TransactionId> {
        self.ctx.transaction_id()
    }
}

/// Validated query transaction identity passed to other YDB services.
pub(crate) struct QueryTxIdentity {
    pub(crate) transaction_id: TransactionId,
    pub(crate) session_id: String,
}

impl Drop for Transaction {
    fn drop(&mut self) {
        let state = std::mem::replace(&mut self.ctx.state, TxState::RolledBack);
        match state {
            TxState::Live(live) => {
                live.finish_on_drop(self.ctx.connection_manager.clone());
            }
            TxState::Committed
            | TxState::RolledBack
            | TxState::Invalidated(_)
            | TxState::Ambiguous(_) => {}
        }
    }
}

impl QueryExecutor for Transaction {
    type Scope = super::builders::Interactive;

    fn exec(&mut self, text: impl Into<String>) -> ExecBuilder<'_, Self::Scope> {
        Transaction::exec(self, text)
    }

    fn query(&mut self, text: impl Into<String>) -> QueryStreamBuilder<'_, Self::Scope> {
        Transaction::query(self, text)
    }

    fn query_result_set(&mut self, text: impl Into<String>) -> ResultSetBuilder<'_, Self::Scope> {
        Transaction::query_result_set(self, text)
    }

    fn query_row(&mut self, text: impl Into<String>) -> QueryRowBuilder<'_, Row, Self::Scope> {
        Transaction::query_row(self, text)
    }
}

#[cfg(test)]
pub(super) fn mark_begin_in_flight_for_test(tx: &mut TransactionExecContext) {
    tx.live_mut().expect("live test transaction").server =
        ServerTransaction::InFlight(InFlightOperation::Begin);
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::GrpcOptions;
    use crate::client_query::TransactionOptions;
    use crate::errors::YdbError;
    use crate::grpc_connection_manager::GrpcConnectionManager;
    use crate::grpc_wrapper::raw_query_service::transaction_control::TransactionId;
    use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
    use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};
    use crate::session_pool::{SessionPool, SessionPoolSettings};
    use http::Uri;
    use ydb_grpc::ydb_proto::status_ids::StatusCode;

    fn test_connection_manager() -> GrpcConnectionManager {
        GrpcConnectionManager::new(
            SharedLoadBalancer::new_with_balancer(Box::new(StaticLoadBalancer::new(
                Uri::from_static("http://127.0.0.1/bench"),
            ))),
            "bench".to_string(),
            MultiInterceptor::new(),
            GrpcOptions::default(),
        )
    }

    #[tokio::test]
    async fn transaction_rollback_is_nop_when_finished() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let mut ctx = TransactionExecContext::new(
            test_connection_manager(),
            lease,
            TransactionOptions::default(),
            None,
        );
        ctx.live_mut().expect("live transaction").server = ServerTransaction::Started(
            TransactionId::from_server("tx-1".into()).expect("non-empty transaction id"),
        );
        ctx.handle_query_error(&YdbError::YdbStatusError(crate::errors::YdbStatusError {
            message: "bad".into(),
            operation_status: StatusCode::GenericError as i32,
            issues: vec![],
        }))
        .expect("apply transaction error");
        assert!(!ctx.state.is_live());
        assert!(ctx.transaction_id().is_none());
        ctx.rollback().await.expect("rollback nop");
    }

    #[tokio::test]
    async fn in_flight_transaction_cleanup_discards_its_session() {
        let manager = test_connection_manager();
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let session_id = lease.session_id().to_string();
        let mut ctx = TransactionExecContext::new(
            manager.clone(),
            lease,
            TransactionOptions::default(),
            None,
        );
        ctx.live_mut().expect("live transaction").server =
            ServerTransaction::InFlight(InFlightOperation::Begin);
        let live = ctx
            .take_live(TxState::RolledBack)
            .expect("take live transaction");

        live.finish_on_drop(manager);

        let replacement = pool
            .acquire_explicit()
            .await
            .expect("acquire replacement session");
        assert_ne!(replacement.session_id(), session_id);
        replacement.return_to_pool();
    }

    #[tokio::test]
    async fn transaction_commit_rejects_an_unhealthy_session() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let mut lease = pool.acquire_explicit().await.expect("acquire test session");
        lease.invalidate();
        let mut ctx = TransactionExecContext::new(
            test_connection_manager(),
            lease,
            TransactionOptions::default(),
            None,
        );
        ctx.live_mut().expect("live transaction").server = ServerTransaction::Started(
            TransactionId::from_server("tx-1".into()).expect("non-empty transaction id"),
        );

        let err = ctx
            .commit()
            .await
            .expect_err("an unhealthy session must not be committed");
        let YdbError::YdbStatusError(status) = err else {
            panic!("expected BadSession, got {err:?}");
        };
        assert_eq!(status.operation_status, StatusCode::BadSession as i32);
        assert!(matches!(ctx.state, TxState::Ambiguous(_)));
    }

    #[tokio::test]
    async fn conflicting_stream_transaction_id_aborts_the_transaction() {
        let pool = SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1));
        let lease = pool.acquire_explicit().await.expect("acquire test session");
        let mut ctx = TransactionExecContext::new(
            test_connection_manager(),
            lease,
            TransactionOptions::default(),
            None,
        );
        ctx.live_mut()
            .expect("live transaction")
            .server
            .mark_query_in_flight()
            .expect("dispatch first query");
        ctx.apply_stream_transaction_id(Some(
            TransactionId::from_server("tx-1".into()).expect("non-empty transaction id"),
        ))
        .expect("capture first transaction id");

        let error = ctx
            .apply_stream_transaction_id(Some(
                TransactionId::from_server("tx-2".into()).expect("non-empty transaction id"),
            ))
            .expect_err("conflicting transaction id must fail");

        assert!(matches!(error, YdbError::InternalError(_)));
        assert!(matches!(ctx.state, TxState::Ambiguous(_)));
    }
}
