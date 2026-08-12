use std::time::Instant;

use crate::errors::{YdbError, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_query_service::transaction_control::TransactionId;
use crate::session_pool::SessionPoolLease;
use crate::{TransactionOptions, TxMode};

use super::super::hooks::{QueryTxCommitStatus, QueryTxHook};

/// Local transaction lifetime. Only `Live` owns server resources and pending hooks; every
/// terminal state is ownership-free.
pub(super) enum TxState {
    /// The only state that owns a session lease and pending hooks.
    Live(LiveTransaction),
    /// Real, confirmed commit: either `CommitTransaction` succeeded or `commit_tx` completed.
    Committed,
    /// Rollback path was chosen and the SDK must not report a commit.
    RolledBack,
    /// The server ended the transaction after a definitive status on a query.
    Invalidated(YdbError),
    /// A commit or rollback RPC returned an error, so the local end attempt was not confirmed.
    Ambiguous(YdbError),
}

impl TxState {
    pub(super) fn is_live(&self) -> bool {
        matches!(self, Self::Live(_))
    }

    pub(super) fn operation_is_in_flight(&self) -> bool {
        matches!(self, Self::Live(live) if live.server.is_in_flight())
    }
}

pub(super) struct LiveTransaction {
    pub(super) lease: SessionPoolLease,
    pub(super) server: ServerTransaction,
    pub(super) hooks: Vec<Box<dyn QueryTxHook>>,
}

impl LiveTransaction {
    pub(super) fn new(lease: SessionPoolLease) -> Self {
        Self {
            lease,
            server: ServerTransaction::NotStarted,
            hooks: Vec::new(),
        }
    }

    pub(super) fn notify_hooks(&mut self, status: QueryTxCommitStatus) {
        for hook in &mut self.hooks {
            hook.after_commit(status);
        }
    }

    pub(super) fn finish(mut self, status: QueryTxCommitStatus) -> SessionPoolLease {
        self.notify_hooks(status);
        self.lease
    }
}

/// Server-side progress within a live transaction.
///
/// ```text
/// NotStarted
///   |-- BeginTransaction --> InFlight(Begin) --------------------------> Started(id)
///   `-- first query ------> InFlight(QueryAwaitingTransactionId)
///                              `-- response id --> InFlight(Query(id)) --> Started(id)
/// Started(id)
///   |-- query -----------> InFlight(Query(id)) -----------------------> Started(id)
///   `-- commit/rollback -> InFlight(Commit(id) | Rollback(id))
/// ```
///
/// In-flight states retain the lease in the transaction so cancellation is conservative: dropping
/// the transaction discards the session instead of issuing a second finalization RPC.
pub(super) enum ServerTransaction {
    NotStarted,
    Started(TransactionId),
    InFlight(InFlightOperation),
}

pub(super) enum InFlightOperation {
    Begin,
    QueryAwaitingTransactionId,
    Query(TransactionId),
    Commit(TransactionId),
    Rollback(TransactionId),
}

pub(super) enum FinalizationAction {
    CompleteLocally,
    SendRpc,
}

impl ServerTransaction {
    // Moving a transaction id between enum variants requires replacing the complete value. Every
    // rejected transition restores the original state before returning its error.
    pub(super) fn operation_in_progress_error(&self) -> YdbError {
        YdbError::InternalError("query transaction operation is already in progress".to_string())
    }

    fn query_not_in_progress_error() -> YdbError {
        YdbError::InternalError("query transaction is not executing a query".to_string())
    }

    /// Enters the appropriate in-flight query state while preserving an existing transaction ID.
    /// Fails without changing the state when another operation is already in progress.
    pub(super) fn mark_query_in_flight(&mut self) -> YdbResult<()> {
        let previous = std::mem::replace(self, Self::NotStarted);
        match previous {
            Self::NotStarted => {
                *self = Self::InFlight(InFlightOperation::QueryAwaitingTransactionId);
                Ok(())
            }
            Self::Started(transaction_id) => {
                *self = Self::InFlight(InFlightOperation::Query(transaction_id));
                Ok(())
            }
            in_flight => {
                let error = in_flight.operation_in_progress_error();
                *self = in_flight;
                Err(error)
            }
        }
    }

    /// Restores `Started(id)` after an in-flight query completes.
    /// A transaction-starting query must capture its ID before this transition.
    pub(super) fn finish_query(&mut self) -> YdbResult<()> {
        let previous = std::mem::replace(self, Self::NotStarted);
        match previous {
            Self::InFlight(InFlightOperation::Query(transaction_id)) => {
                *self = Self::Started(transaction_id);
                Ok(())
            }
            Self::InFlight(InFlightOperation::QueryAwaitingTransactionId) => {
                *self = Self::InFlight(InFlightOperation::QueryAwaitingTransactionId);
                Err(YdbError::InternalError(
                    "ExecuteQuery response missing transaction id".to_string(),
                ))
            }
            state => {
                let error = Self::query_not_in_progress_error();
                *self = state;
                Err(error)
            }
        }
    }

    /// Restores the state from before a failed query was dispatched.
    pub(super) fn restore_after_query_error(&mut self) -> YdbResult<()> {
        let previous = std::mem::replace(self, Self::NotStarted);
        match previous {
            Self::InFlight(InFlightOperation::QueryAwaitingTransactionId) => Ok(()),
            Self::InFlight(InFlightOperation::Query(transaction_id)) => {
                *self = Self::Started(transaction_id);
                Ok(())
            }
            state => {
                let error = Self::query_not_in_progress_error();
                *self = state;
                Err(error)
            }
        }
    }

    pub(super) fn is_in_flight(&self) -> bool {
        matches!(self, Self::InFlight(_))
    }

    pub(super) fn is_query_in_flight(&self) -> bool {
        matches!(
            self,
            Self::InFlight(
                InFlightOperation::QueryAwaitingTransactionId | InFlightOperation::Query(_)
            )
        )
    }

    /// Captures the transaction ID returned by the query stream and rejects conflicting IDs.
    pub(super) fn capture_query_transaction_id(
        &mut self,
        incoming: TransactionId,
    ) -> YdbResult<()> {
        match self {
            Self::InFlight(InFlightOperation::QueryAwaitingTransactionId) => {
                *self = Self::InFlight(InFlightOperation::Query(incoming));
                Ok(())
            }
            Self::InFlight(InFlightOperation::Query(existing)) if existing == &incoming => Ok(()),
            Self::InFlight(InFlightOperation::Query(existing)) => {
                Err(YdbError::InternalError(format!(
                    "query transaction id changed from {} to {}",
                    existing.as_str(),
                    incoming.as_str()
                )))
            }
            _ => Err(YdbError::InternalError(
                "query response contained a transaction id while no query was in progress"
                    .to_string(),
            )),
        }
    }

    pub(super) fn prepare_commit(&mut self) -> YdbResult<FinalizationAction> {
        self.prepare_finalization(InFlightOperation::Commit)
    }

    pub(super) fn prepare_rollback(&mut self) -> YdbResult<FinalizationAction> {
        self.prepare_finalization(InFlightOperation::Rollback)
    }

    fn prepare_finalization(
        &mut self,
        operation: fn(TransactionId) -> InFlightOperation,
    ) -> YdbResult<FinalizationAction> {
        let previous = std::mem::replace(self, Self::NotStarted);
        match previous {
            Self::NotStarted => Ok(FinalizationAction::CompleteLocally),
            Self::Started(transaction_id) => {
                *self = Self::InFlight(operation(transaction_id));
                Ok(FinalizationAction::SendRpc)
            }
            state => {
                let error = state.operation_in_progress_error();
                *self = state;
                Err(error)
            }
        }
    }
}

pub(crate) struct TransactionExecContext {
    pub(super) connection_manager: GrpcConnectionManager,
    pub(super) tx_mode: TxMode,
    /// When set, the first operation calls `BeginTransaction` RPC instead of lazy `BeginTx` in
    /// `ExecuteQuery`.
    pub(super) begin_before_query: bool,
    pub(super) state: TxState,
    /// Absolute deadline from [`crate::QueryClient::retry_tx`] `.timeout()`, propagated to every
    /// RPC in the callback.
    pub(super) retry_deadline: Option<Instant>,
}

impl TransactionExecContext {
    pub(super) fn new(
        connection_manager: GrpcConnectionManager,
        lease: SessionPoolLease,
        options: TransactionOptions,
        retry_deadline: Option<Instant>,
    ) -> Self {
        Self {
            connection_manager,
            tx_mode: options.mode(),
            begin_before_query: options.begin(),
            state: TxState::Live(LiveTransaction::new(lease)),
            retry_deadline,
        }
    }

    pub(super) fn live(&self) -> YdbResult<&LiveTransaction> {
        match &self.state {
            TxState::Live(live) => Ok(live),
            _ => Err(transaction_finished_error()),
        }
    }

    pub(super) fn live_mut(&mut self) -> YdbResult<&mut LiveTransaction> {
        match &mut self.state {
            TxState::Live(live) => Ok(live),
            _ => Err(transaction_finished_error()),
        }
    }

    pub(super) fn session_lease(&self) -> YdbResult<&SessionPoolLease> {
        Ok(&self.live()?.lease)
    }

    pub(super) fn transaction_id(&self) -> Option<&TransactionId> {
        match &self.state {
            TxState::Live(LiveTransaction {
                server: ServerTransaction::Started(id),
                ..
            }) => Some(id),
            _ => None,
        }
    }

    pub(super) fn register_hook(&mut self, hook: Box<dyn QueryTxHook>) -> YdbResult<()> {
        self.live_mut()?.hooks.push(hook);
        Ok(())
    }

    pub(super) fn take_live(&mut self, replacement: TxState) -> YdbResult<LiveTransaction> {
        let previous = std::mem::replace(&mut self.state, replacement);
        match previous {
            TxState::Live(live) => Ok(live),
            state => {
                self.state = state;
                Err(transaction_finished_error())
            }
        }
    }

    pub(super) fn finish_live(
        &mut self,
        replacement: TxState,
        status: QueryTxCommitStatus,
    ) -> YdbResult<SessionPoolLease> {
        Ok(self.take_live(replacement)?.finish(status))
    }

    pub(in crate::client_query) fn finish_query(&mut self, commit_at_end: bool) -> YdbResult<()> {
        if commit_at_end {
            if !self.live()?.server.is_query_in_flight() {
                let error = YdbError::InternalError(
                    "transaction query completed while no query was in progress".to_string(),
                );
                self.abort_unconfirmed(error.clone())?;
                return Err(error);
            }
            self.finish_live(TxState::Committed, QueryTxCommitStatus::Committed)?
                .return_to_pool();
            return Ok(());
        }

        if let Err(error) = self.live_mut()?.server.finish_query() {
            self.abort_unconfirmed(error.clone())?;
            return Err(error);
        }
        Ok(())
    }

    /// Ends an operation with an unknown server outcome and discards its session lease.
    pub(in crate::client_query) fn abort_unconfirmed(&mut self, error: YdbError) -> YdbResult<()> {
        self.finish_live(TxState::Ambiguous(error), QueryTxCommitStatus::Aborted)?
            .discard();
        Ok(())
    }

    /// Applies a query failure to transaction and session ownership.
    ///
    /// Definitive transaction errors end the transaction, session-breaking uncertainty discards
    /// its lease, and reusable errors restore the state from before the query.
    pub(in crate::client_query) fn handle_query_error(
        &mut self,
        error: &YdbError,
    ) -> YdbResult<()> {
        if error.invalidates_server_transaction() {
            let lease = self.finish_live(
                TxState::Invalidated(error.clone()),
                QueryTxCommitStatus::Aborted,
            )?;
            if error.requires_session_discard() {
                lease.discard();
            } else {
                lease.return_to_pool();
            }
            return Ok(());
        }

        if error.requires_session_discard() {
            return self.abort_unconfirmed(error.clone());
        }

        if self.live()?.server.is_query_in_flight() {
            self.live_mut()?.server.restore_after_query_error()?;
        }
        Ok(())
    }

    pub(in crate::client_query) fn cancel_query(&mut self) -> YdbResult<()> {
        self.abort_unconfirmed(YdbError::InternalError(
            "query response stream was cancelled before completion".into(),
        ))
    }

    /// Captures a stream transaction ID; conflicting IDs make the outcome ambiguous.
    pub(in crate::client_query) fn apply_stream_transaction_id(
        &mut self,
        transaction_id: Option<TransactionId>,
    ) -> YdbResult<()> {
        let Some(transaction_id) = transaction_id else {
            return Ok(());
        };
        if let Err(error) = self
            .live_mut()?
            .server
            .capture_query_transaction_id(transaction_id)
        {
            self.abort_unconfirmed(error.clone())?;
            return Err(error);
        }
        Ok(())
    }
}

fn transaction_finished_error() -> YdbError {
    YdbError::Custom("transaction already finished (committed or rolled back)".to_string())
}
