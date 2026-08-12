use std::collections::HashMap;

use tracing::instrument;

use crate::TxMode;
use crate::errors::{YdbError, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;
use crate::grpc_wrapper::raw_query_service::execute_query::RawExecuteQueryRequest;
use crate::grpc_wrapper::raw_query_service::stream::ExecuteQueryStream;
use crate::grpc_wrapper::raw_query_service::transaction_control::{
    begin_tx_control, tx_id_control,
};
use crate::session_pool::spawn_pool_release;
use crate::traces::helpers::ensure_len_string;
use crate::types::Value;

use super::super::exec::{
    CallOptions, maybe_with_operation_timeout, resolve_effective_timeout, tx_mode_to_raw,
};
use super::super::hooks::QueryTxCommitStatus;
use super::QueryTxIdentity;
use super::state::{
    FinalizationAction, InFlightOperation, LiveTransaction, ServerTransaction,
    TransactionExecContext, TxState,
};

fn ensure_interactive_tx_mode(mode: TxMode) -> YdbResult<()> {
    if mode == TxMode::Implicit {
        return Err(YdbError::Custom(
            "TxMode::Implicit is not available inside Transaction; \
             DDL and other non-transactional statements must run on QueryClient, not inside tx"
                .to_string(),
        ));
    }
    if !mode.supported_in_interactive() {
        return Err(YdbError::Custom(format!(
            "transaction mode {mode:?} is not supported in interactive transactions \
             (use SerializableReadWrite, SnapshotReadOnly, or SnapshotReadWrite)"
        )));
    }
    Ok(())
}

fn reject_per_call_tx_mode_override(
    tx: &TransactionExecContext,
    opts: &CallOptions,
) -> YdbResult<()> {
    if let Some(override_mode) = opts.tx_mode_override
        && override_mode != tx.tx_mode
    {
        return Err(YdbError::Custom(format!(
            "per-call tx_mode {:?} does not match transaction mode {:?}",
            override_mode, tx.tx_mode
        )));
    }
    Ok(())
}

/// Build `tx_control` for an interactive transaction.
///
/// **Lazy start (default):** while `tx_id` is unknown, the first `ExecuteQuery` sends
/// `BeginTx` with `commit_tx: false` — no upfront `BeginTransaction` RPC. The server
/// returns `tx_id` in the response stream; later queries use `TxId`.
///
/// **Explicit begin:** when [`TransactionExecContext::begin_before_query`] is set or
/// [`super::Transaction::begin`] was called, `tx_id` is already known and this
/// function always emits `TxId`.
fn tx_control_for_transaction(
    tx: &TransactionExecContext,
    opts: &CallOptions,
) -> YdbResult<Option<ydb_grpc::ydb_proto::query::TransactionControl>> {
    Ok(Some(match &tx.live()?.server {
        ServerTransaction::Started(id) => {
            reject_per_call_tx_mode_override(tx, opts)?;
            ensure_interactive_tx_mode(tx.tx_mode)?;
            tx_id_control(id, opts.commit_tx)
        }
        ServerTransaction::NotStarted => {
            reject_per_call_tx_mode_override(tx, opts)?;
            ensure_interactive_tx_mode(tx.tx_mode)?;
            begin_tx_control(tx_mode_to_raw(tx.tx_mode)?, opts.commit_tx)
        }
        ServerTransaction::InFlight(_) => {
            return Err(tx.live()?.server.operation_in_progress_error());
        }
    }))
}

impl TransactionExecContext {
    /// Session and transaction ids for cross-service RPCs (e.g. topic `UpdateOffsetsInTransaction`).
    pub(super) async fn identity(&mut self) -> YdbResult<QueryTxIdentity> {
        self.ensure_begin().await?;
        let session_id = self.session_lease()?.session_id().to_string();
        let transaction_id = self
            .transaction_id()
            .ok_or_else(|| YdbError::Custom("query transaction id is not available".to_string()))?
            .clone();
        Ok(QueryTxIdentity {
            session_id,
            transaction_id,
        })
    }

    #[instrument(name = "ydb.ExecuteQuery", skip_all, fields(db.system.name = "ydb", ydb.Query.text = %ensure_len_string(&yql_text), ydb.Query.params = ?parameters, ydb.Query.opts = ?opts))]
    async fn execute_request(
        &self,
        yql_text: String,
        parameters: HashMap<String, Value>,
        opts: &CallOptions,
        concurrent_result_sets: bool,
    ) -> YdbResult<(RawQueryClient, RawExecuteQueryRequest)> {
        let client = self
            .connection_manager
            .get_auth_service_to_node(RawQueryClient::new, self.session_lease()?.node_uri())
            .await?;
        let mut request = RawExecuteQueryRequest::new(
            self.session_lease()?.session_id(),
            yql_text,
            parameters,
            tx_control_for_transaction(self, opts)?,
            opts.collect_stats,
        );
        request.concurrent_result_sets = concurrent_result_sets;
        Ok((client, request))
    }

    /// Open the transaction via `BeginTransaction` RPC (explicit begin).
    #[instrument(name = "ydb.Query.TransactionEnsureBegin", skip_all, fields(db.system.name = "ydb", ydb.tx.mode = ?self.tx_mode, ydb.session.id = tracing::field::Empty), err)]
    pub(super) async fn ensure_begin(&mut self) -> YdbResult<()> {
        match &self.live()?.server {
            ServerTransaction::Started(_) => return Ok(()),
            ServerTransaction::NotStarted => {}
            ServerTransaction::InFlight(_) => {
                return Err(self.live()?.server.operation_in_progress_error());
            }
        }
        ensure_interactive_tx_mode(self.tx_mode)?;
        self.session_lease()?.ensure_healthy()?;
        let client = self
            .connection_manager
            .get_auth_service_to_node(RawQueryClient::new, self.session_lease()?.node_uri())
            .await;
        let mut client = match client {
            Ok(client) => client,
            Err(error) => {
                if error.requires_session_discard() {
                    self.live_mut()?.lease.invalidate();
                }
                return Err(error);
            }
        };
        self.live_mut()?.server = ServerTransaction::InFlight(InFlightOperation::Begin);

        let result = async {
            let session_id = self.session_lease()?.session_id();
            tracing::Span::current().record("ydb.session.id", session_id);
            maybe_with_operation_timeout(
                resolve_effective_timeout(self.retry_deadline, None),
                async {
                    client
                        .begin_transaction(session_id, tx_mode_to_raw(self.tx_mode)?)
                        .await
                        .map_err(Into::into)
                },
            )
            .await
        }
        .await;

        match result {
            Ok(tx_id) => {
                self.live_mut()?.server = ServerTransaction::Started(tx_id);
                Ok(())
            }
            Err(err) => {
                let live = self.live_mut()?;
                live.server = ServerTransaction::NotStarted;
                if err.requires_session_discard() {
                    live.lease.invalidate();
                }
                Err(err)
            }
        }
    }

    async fn before_commit(&mut self) -> YdbResult<()> {
        for hook in &mut self.live_mut()?.hooks {
            hook.before_commit().await?;
        }
        Ok(())
    }

    #[instrument(name = "ydb.Query.TransactionBeginStream", skip_all, fields(db.system.name = "ydb", ydb.tx.mode = ?self.tx_mode, ydb.session.id = tracing::field::Empty), err)]
    pub(in crate::client_query) async fn begin_stream(
        &mut self,
        text: String,
        params: HashMap<String, Value>,
        opts: CallOptions,
        concurrent_result_sets: bool,
    ) -> YdbResult<ExecuteQueryStream> {
        debug_assert!(
            !opts.implicit_session,
            "implicit_session is only available on QueryClient one-shot builders"
        );
        self.live()?;
        let effective_timeout = resolve_effective_timeout(self.retry_deadline, opts.timeout);
        let result: YdbResult<ExecuteQueryStream> =
            maybe_with_operation_timeout(effective_timeout, async {
                self.session_lease()?.ensure_healthy()?;
                tracing::Span::current()
                    .record("ydb.session.id", self.session_lease()?.session_id());
                if self.begin_before_query {
                    self.ensure_begin().await?;
                }
                if opts.commit_tx {
                    self.before_commit().await?;
                }
                let (mut client, req) = self
                    .execute_request(text, params, &opts, concurrent_result_sets)
                    .await?;
                self.live_mut()?.server.mark_query_in_flight()?;
                let stream = client.execute_query(req).await.map_err(YdbError::from)?;
                let mut stream = ExecuteQueryStream::new(stream);
                let first_part = stream.prime_first_part().await.map_err(YdbError::from);
                self.apply_stream_transaction_id(stream.take_captured_tx_id())?;
                first_part?;
                if !stream.in_progress() {
                    let error = YdbError::InternalError(
                        "ExecuteQuery response stream closed before the first part".to_string(),
                    );
                    self.abort_unconfirmed(error.clone())?;
                    return Err(error);
                }
                Ok(stream)
            })
            .await;
        match result {
            Ok(stream) => Ok(stream),
            Err(error) => {
                if self.state.is_live() {
                    self.handle_query_error(&error)?;
                }
                Err(error)
            }
        }
    }

    #[instrument(name = "ydb.Commit", skip_all, fields(db.system.name = "ydb", ydb.tx.id = tracing::field::Empty, ydb.session.id = tracing::field::Empty), err)]
    pub(super) async fn commit(&mut self) -> YdbResult<()> {
        if !self.state.is_live() {
            return Ok(());
        }
        if let Err(hook_error) = self.before_commit().await {
            return match self.rollback().await {
                Err(invariant) if self.state.is_live() => Err(invariant),
                // A dispatched rollback already moved the transaction to a terminal state. Preserve
                // the hook failure that caused it as the operation's primary error.
                Ok(()) | Err(_) => Err(hook_error),
            };
        }
        if matches!(self.live()?.server, ServerTransaction::Started(_))
            && let Err(error) = self.live()?.lease.ensure_healthy()
        {
            let lease = self.finish_live(
                TxState::Ambiguous(error.clone()),
                QueryTxCommitStatus::Aborted,
            )?;
            return lease.finish(Err(error));
        }
        match self.live_mut()?.server.prepare_commit()? {
            FinalizationAction::CompleteLocally => {
                self.finish_live(TxState::Committed, QueryTxCommitStatus::Committed)?
                    .return_to_pool();
                return Ok(());
            }
            FinalizationAction::SendRpc => {}
        }
        let result = async {
            let live = self.live()?;
            let ServerTransaction::InFlight(InFlightOperation::Commit(tx_id)) = &live.server else {
                return Err(YdbError::InternalError(
                    "query transaction is not committing".to_string(),
                ));
            };
            let session_id = live.lease.session_id();
            tracing::Span::current()
                .record("ydb.session.id", session_id)
                .record("ydb.tx.id", tx_id.as_str());
            let mut client = self
                .connection_manager
                .get_auth_service_to_node(RawQueryClient::new, live.lease.node_uri())
                .await?;
            maybe_with_operation_timeout(
                resolve_effective_timeout(self.retry_deadline, None),
                async {
                    client
                        .commit_transaction(session_id, tx_id.as_str())
                        .await
                        .map_err(Into::into)
                },
            )
            .await
        }
        .await;

        let (terminal, hook_status) = match &result {
            Ok(()) => (TxState::Committed, QueryTxCommitStatus::Committed),
            Err(err) => (
                TxState::Ambiguous(err.clone()),
                QueryTxCommitStatus::Aborted,
            ),
        };
        let lease = self.finish_live(terminal, hook_status)?;
        // Do not retry commit: a transport timeout may mean the commit succeeded server-side.
        lease.finish(result)
    }

    #[instrument(name = "ydb.Rollback", skip_all, fields(db.system.name = "ydb", ydb.tx.id = tracing::field::Empty, ydb.session.id = tracing::field::Empty), err)]
    pub(super) async fn rollback(&mut self) -> YdbResult<()> {
        if !self.state.is_live() {
            return Ok(());
        }
        match self.live_mut()?.server.prepare_rollback()? {
            FinalizationAction::CompleteLocally => {
                self.finish_live(TxState::RolledBack, QueryTxCommitStatus::Aborted)?
                    .return_to_pool();
                return Ok(());
            }
            FinalizationAction::SendRpc => {}
        }

        let result = async {
            let live = self.live()?;
            let ServerTransaction::InFlight(InFlightOperation::Rollback(tx_id)) = &live.server
            else {
                return Err(YdbError::InternalError(
                    "query transaction is not rolling back".to_string(),
                ));
            };
            let session_id = live.lease.session_id();
            tracing::Span::current()
                .record("ydb.session.id", session_id)
                .record("ydb.tx.id", tx_id.as_str());
            let mut client = self
                .connection_manager
                .get_auth_service_to_node(RawQueryClient::new, live.lease.node_uri())
                .await?;
            maybe_with_operation_timeout(
                resolve_effective_timeout(self.retry_deadline, None),
                async {
                    client
                        .rollback_transaction(session_id, tx_id.as_str())
                        .await
                        .map_err(Into::into)
                },
            )
            .await
        }
        .await;

        let terminal = match &result {
            Ok(()) => TxState::RolledBack,
            Err(err) => TxState::Ambiguous(err.clone()),
        };
        let lease = self.finish_live(terminal, QueryTxCommitStatus::Aborted)?;
        lease.finish(result)
    }
}

impl LiveTransaction {
    /// Best-effort rollback when [`super::Transaction`] is dropped without explicit finalization.
    pub(super) fn finish_on_drop(mut self, connection_manager: GrpcConnectionManager) {
        self.notify_hooks(QueryTxCommitStatus::Aborted);
        let Self {
            lease,
            server,
            hooks: _,
        } = self;
        let transaction_id = match server {
            ServerTransaction::NotStarted => {
                lease.return_to_pool();
                return;
            }
            ServerTransaction::Started(transaction_id) => transaction_id,
            ServerTransaction::InFlight(_) => {
                lease.discard();
                return;
            }
        };

        spawn_pool_release(async move {
            let client_result = connection_manager
                .get_auth_service_to_node(RawQueryClient::new, lease.node_uri())
                .await;
            let rollback_succeeded = match client_result {
                Ok(mut client) => client
                    .rollback_transaction(lease.session_id(), transaction_id.as_str())
                    .await
                    .is_ok(),
                Err(_) => false,
            };
            if rollback_succeeded {
                lease.return_to_pool();
            } else {
                lease.discard();
            }
        });
    }
}
