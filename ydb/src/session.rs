use crate::client::TimeoutSettings;
use crate::client_table::TableServiceClientType;
use crate::errors::{YdbError, YdbResult};
use crate::grpc_wrapper::raw_table_service::read_rows::RawReadRowsRequest;
use crate::query::Query;
use crate::result::{ExplainResult, QueryResult, StreamResult};
use crate::types::Value;
use derivative::Derivative;
use itertools::Itertools;
use std::sync::atomic::{AtomicI64, Ordering};

use http::Uri;

use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_table_service::client::{CollectStatsMode, RawTableClient};
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;

use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_table_service::bulk_upsert::RawBulkUpsertRequest;
use crate::grpc_wrapper::raw_table_service::commit_transaction::RawCommitTransactionRequest;
use crate::grpc_wrapper::raw_table_service::copy_table::{
    RawCopyTableRequest, RawCopyTablesRequest,
};
use crate::grpc_wrapper::raw_table_service::describe_table::RawDescribeTableRequest;
use crate::grpc_wrapper::raw_table_service::execute_data_query::RawExecuteDataQueryRequest;
use crate::grpc_wrapper::raw_table_service::execute_scheme_query::RawExecuteSchemeQueryRequest;
use crate::grpc_wrapper::raw_table_service::explain_data_query::RawExplainDataQueryRequest;
use crate::grpc_wrapper::raw_table_service::rollback_transaction::RawRollbackTransactionRequest;
use crate::table_service_types::{ColumnDescription, CopyTableItem, TableDescription};
use crate::traces::helpers::ensure_len_string;
use tracing::{debug, trace};
use ydb_grpc::ydb_proto::table::v1::table_service_client::TableServiceClient;
use ydb_grpc::ydb_proto::table::{execute_scan_query_request, ExecuteScanQueryRequest};

static REQUEST_NUMBER: AtomicI64 = AtomicI64::new(0);
static DEFAULT_COLLECT_STAT_MODE: CollectStatsMode = CollectStatsMode::None;

fn req_number() -> i64 {
    REQUEST_NUMBER.fetch_add(1, Ordering::Relaxed)
}

type DropSessionCallback = dyn FnOnce(&mut Session) + Send + Sync;

/// If an RPC is cancelled mid-flight (e.g. operation timeout), the server may still be
/// processing it. Mark the session non-poolable so the next lease gets a fresh session
/// instead of hitting SessionBusy on reuse (aligned with go-sdk context-error handling).
struct InFlightTableRpcGuard<'a> {
    session: &'a mut Session,
    active: bool,
}

impl Drop for InFlightTableRpcGuard<'_> {
    fn drop(&mut self) {
        if self.active {
            self.session.discard_from_pool();
        }
    }
}

/// Await a table RPC under [`InFlightTableRpcGuard`] (discard session on cancel/timeout).
macro_rules! in_flight_table_rpc {
    ($session:expr, $table:ident, $rpc:expr) => {{
        let mut $table = $session.get_table_client().await?;
        let mut guard = InFlightTableRpcGuard {
            session: $session,
            active: true,
        };
        let res = $rpc.await;
        guard.active = false;
        guard.session.handle_raw_result(res)
    }};
}

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Session {
    pub(crate) id: String,

    pub(crate) can_pooled: bool,

    #[derivative(Debug = "ignore")]
    on_drop_callbacks: Vec<Box<DropSessionCallback>>,

    #[derivative(Debug = "ignore")]
    channel_pool: Box<dyn CreateTableClient>,

    timeouts: TimeoutSettings,
}

impl Session {
    pub(crate) fn new<CT: CreateTableClient + 'static>(
        id: String,
        channel_pool: CT,
        timeouts: TimeoutSettings,
    ) -> Self {
        Self {
            id,
            can_pooled: true,
            on_drop_callbacks: Vec::new(),
            channel_pool: Box::new(channel_pool),
            timeouts,
        }
    }

    pub(crate) fn handle_error(&mut self, err: &YdbError) {
        if should_discard_session_from_pool(err) {
            self.discard_from_pool();
        }
    }

    pub(crate) fn discard_from_pool(&mut self) {
        self.can_pooled = false;
    }

    fn handle_raw_result<T>(&mut self, res: RawResult<T>) -> YdbResult<T> {
        let res = res.map_err(YdbError::from);
        if let Err(err) = &res {
            self.handle_error(err);
        }
        res
    }

    pub(crate) async fn commit_transaction(&mut self, tx_id: String) -> YdbResult<()> {
        let session_id = self.id.clone();
        let operation_params = self.timeouts.operation_params();
        in_flight_table_rpc!(self, table, {
            table.commit_transaction(RawCommitTransactionRequest {
                session_id,
                tx_id,
                operation_params,
                collect_stats: DEFAULT_COLLECT_STAT_MODE,
            })
        })?;
        Ok(())
    }

    pub(crate) async fn execute_schema_query(&mut self, query: String) -> YdbResult<()> {
        let session_id = self.id.clone();
        let operation_params = self.timeouts.operation_params();
        in_flight_table_rpc!(self, table, {
            table.execute_scheme_query(RawExecuteSchemeQueryRequest {
                session_id,
                yql_text: query,
                operation_params,
            })
        })?;
        Ok(())
    }

    pub(crate) async fn read_rows(
        &mut self,
        table_path: String,
        keys: Value,
        columns: Vec<String>,
    ) -> YdbResult<crate::ResultSet> {
        debug_assert!(matches!(keys, Value::List(_)));

        let req = RawReadRowsRequest {
            session_id: self.id.clone(),
            path: table_path,
            keys: keys.try_into()?,
            columns,
        };

        let raw_read_rows_response = in_flight_table_rpc!(self, table, table.read_rows(req))?;

        raw_read_rows_response.result_set.try_into()
    }

    pub(crate) async fn execute_bulk_upsert(
        &mut self,
        table_path: String,
        rows: Value,
    ) -> YdbResult<()> {
        let raw_rows: crate::grpc_wrapper::raw_table_service::value::RawTypedValue =
            rows.try_into()?;
        let req = RawBulkUpsertRequest {
            table: table_path,
            rows: raw_rows.into(),
            operation_params: self.timeouts.operation_params(),
        };
        in_flight_table_rpc!(self, table, table.bulk_upsert(req))?;
        Ok(())
    }

    #[tracing::instrument(skip(self, req), fields(req_number=req_number()))]
    pub(crate) async fn execute_data_query(
        &mut self,
        mut req: RawExecuteDataQueryRequest,
        error_on_truncated: bool,
    ) -> YdbResult<QueryResult> {
        req.session_id.clone_from(&self.id);
        req.operation_params = self.timeouts.operation_params();

        trace!(
            "request: {}",
            ensure_len_string(serde_json::to_string(&req)?)
        );

        let mut table = self.get_table_client().await?;
        let mut in_flight = InFlightTableRpcGuard {
            session: self,
            active: true,
        };
        let res = table.execute_data_query(req).await;
        in_flight.active = false;
        let res = in_flight.session.handle_raw_result(res)?;
        trace!(
            "result: {}",
            ensure_len_string(serde_json::to_string(&res)?)
        );
        QueryResult::from_raw_result(error_on_truncated, res)
    }

    #[tracing::instrument(skip(self, query), fields(req_number=req_number()))]
    pub(crate) async fn explain_data_query(
        &mut self,
        query: String,
        collect_full_diagnostics: bool,
    ) -> YdbResult<ExplainResult> {
        let req = RawExplainDataQueryRequest {
            session_id: self.id.clone(),
            yql_text: query,
            operation_params: self.timeouts.operation_params(),
            collect_full_diagnostics,
        };
        trace!(
            "request: {}",
            ensure_len_string(serde_json::to_string(&req)?)
        );

        let res = in_flight_table_rpc!(self, table, table.explain_data_query(req))?;
        trace!(
            "result: {}",
            ensure_len_string(serde_json::to_string(&res)?)
        );
        Ok(res.into())
    }

    #[tracing::instrument(skip(self, query), fields(req_number=req_number()))]
    pub async fn execute_scan_query(&mut self, query: Query) -> YdbResult<StreamResult> {
        let req = ExecuteScanQueryRequest {
            query: Some(query.query_to_proto()),
            parameters: query.params_to_proto()?,
            mode: execute_scan_query_request::Mode::Exec as i32,
            ..ExecuteScanQueryRequest::default()
        };
        debug!(
            "request: {}",
            crate::traces::helpers::ensure_len_string(serde_json::to_string(&req)?)
        );
        let mut in_flight = InFlightTableRpcGuard {
            session: self,
            active: true,
        };
        let mut channel = in_flight.session.get_channel().await?;
        let resp = match channel.stream_execute_scan_query(req).await {
            Ok(resp) => {
                in_flight.active = false;
                resp
            }
            Err(err) => {
                in_flight.active = false;
                let err = YdbError::from(err);
                in_flight.session.handle_error(&err);
                return Err(err);
            }
        };
        let stream = resp.into_inner();
        Ok(StreamResult { results: stream })
    }

    pub(crate) async fn rollback_transaction(&mut self, tx_id: String) -> YdbResult<()> {
        let session_id = self.id.clone();
        let operation_params = self.timeouts.operation_params();
        in_flight_table_rpc!(self, table, {
            table.rollback_transaction(RawRollbackTransactionRequest {
                session_id,
                tx_id,
                operation_params,
            })
        })
    }

    pub async fn copy_table(
        &mut self,
        source_path: String,
        destination_path: String,
    ) -> YdbResult<()> {
        let session_id = self.id.clone();
        let operation_params = self.timeouts.operation_params();
        in_flight_table_rpc!(self, table, {
            table.copy_table(RawCopyTableRequest {
                session_id,
                source_path,
                destination_path,
                operation_params,
            })
        })?;
        Ok(())
    }

    pub async fn copy_tables(&mut self, tables: Vec<CopyTableItem>) -> YdbResult<()> {
        let session_id = self.id.clone();
        let operation_params = self.timeouts.operation_params();
        in_flight_table_rpc!(self, table, {
            table.copy_tables(RawCopyTablesRequest {
                operation_params,
                session_id,
                tables: tables.into_iter().map_into().collect(),
            })
        })?;
        Ok(())
    }

    pub async fn describe_table(&mut self, path: String) -> YdbResult<TableDescription> {
        let session_id = self.id.clone();
        let operation_params = self.timeouts.operation_params();
        let raw_result = in_flight_table_rpc!(self, table, {
            table.describe_table(RawDescribeTableRequest {
                session_id,
                path: path.clone(),
                operation_params,
            })
        })?;

        let columns = raw_result
            .columns
            .into_iter()
            .map(|raw_col| ColumnDescription {
                name: raw_col.name,
                type_value: raw_col.column_type.into_value_example().map_err(|e| {
                    crate::table_service_types::UnknownTypeDescription {
                        error: e.to_string(),
                    }
                }),
                family: raw_col.family,
            })
            .collect();

        let indexes = raw_result
            .indexes
            .into_iter()
            .map(|idx| idx.into())
            .collect();

        Ok(TableDescription {
            columns,
            primary_key: raw_result.primary_key,
            indexes,
            store_type: raw_result.store_type.into(),
        })
    }

    pub fn with_timeouts(mut self, timeouts: TimeoutSettings) -> Self {
        self.timeouts = timeouts;
        self
    }

    // deprecated, use get_table_client instead
    async fn get_channel(&self) -> YdbResult<TableServiceClientType> {
        self.channel_pool.create_grpc_table_client().await
    }

    async fn get_table_client(&mut self) -> YdbResult<RawTableClient> {
        match self.channel_pool.create_table_client(self.timeouts).await {
            Ok(client) => Ok(client),
            Err(err) => {
                self.handle_error(&err);
                Err(err)
            }
        }
    }

    pub(crate) fn on_drop(&mut self, f: Box<dyn FnOnce(&mut Self) + Send + Sync>) {
        self.on_drop_callbacks.push(f)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        trace!("drop session: {}", &self.id);
        while let Some(on_drop) = self.on_drop_callbacks.pop() {
            on_drop(self)
        }
    }
}

/// Whether a failed RPC means the pooled session must not be reused.
///
/// Aligned with go-sdk `xerrors.MustDeleteTableOrQuerySession` (broader than the legacy
/// table pool, which only discarded on `BadSession` / `SessionExpired`). Transient
/// transport failures now invalidate the session to avoid `SessionBusy` on reuse.
pub(crate) fn should_discard_session_from_pool(err: &YdbError) -> bool {
    match err {
        YdbError::YdbStatusError(ydb_err) => {
            use ydb_grpc::ydb_proto::status_ids::StatusCode;
            StatusCode::try_from(ydb_err.operation_status).is_ok_and(|status| {
                matches!(
                    status,
                    StatusCode::BadSession | StatusCode::SessionBusy | StatusCode::SessionExpired
                )
            })
        }
        YdbError::Transport(_) | YdbError::TransportDial(_) => true,
        YdbError::TransportGRPCStatus(status) => {
            use tonic::Code;
            // Intentional parity with go-sdk `xerrors.MustDeleteTableOrQuerySession` on
            // gRPC transport errors (`IsTransportError`): includes `InvalidArgument`,
            // `NotFound`, etc. even though they often reflect request-level issues, because
            // the SDK cannot tell whether the server left a query/tx in flight. YDB operation
            // status errors use the narrower match above (BadSession / SessionBusy / Expired).
            matches!(
                status.code(),
                Code::Cancelled
                    | Code::Unknown
                    | Code::InvalidArgument
                    | Code::DeadlineExceeded
                    | Code::NotFound
                    | Code::AlreadyExists
                    | Code::PermissionDenied
                    | Code::FailedPrecondition
                    | Code::Aborted
                    | Code::Unimplemented
                    | Code::Internal
                    | Code::Unavailable
                    | Code::DataLoss
                    | Code::Unauthenticated
            )
        }
        _ => false,
    }
}

#[cfg(test)]
mod discard_session_tests {
    use super::*;
    use std::sync::Arc;
    use tonic::{Code, Status};
    use ydb_grpc::ydb_proto::status_ids::StatusCode;

    fn ydb_status(status: StatusCode) -> YdbError {
        YdbError::YdbStatusError(crate::errors::YdbStatusError {
            message: "test".into(),
            operation_status: status as i32,
            issues: vec![],
        })
    }

    #[test]
    fn discard_on_bad_session_and_transport() {
        assert!(should_discard_session_from_pool(&ydb_status(
            StatusCode::BadSession
        )));
        assert!(should_discard_session_from_pool(&ydb_status(
            StatusCode::SessionBusy
        )));
        assert!(should_discard_session_from_pool(&YdbError::Transport(
            "connection refused".into()
        )));
        assert!(should_discard_session_from_pool(
            &YdbError::TransportGRPCStatus(Arc::new(Status::new(Code::Unavailable, "node down")))
        ));
        assert!(should_discard_session_from_pool(
            &YdbError::TransportGRPCStatus(Arc::new(Status::new(
                Code::InvalidArgument,
                "bad request"
            )))
        ));
    }

    #[test]
    fn discard_grpc_transport_but_not_ydb_operation_errors() {
        use tonic::{Code, Status};
        assert!(!should_discard_session_from_pool(&ydb_status(
            StatusCode::PreconditionFailed
        )));
        assert!(!should_discard_session_from_pool(
            &YdbError::TransportGRPCStatus(Arc::new(Status::new(
                Code::ResourceExhausted,
                "rate limited"
            )))
        ));
    }

    #[test]
    fn keep_session_on_business_errors() {
        assert!(!should_discard_session_from_pool(&ydb_status(
            StatusCode::PreconditionFailed
        )));
        assert!(!should_discard_session_from_pool(&YdbError::Custom(
            "customer".into()
        )));
    }

    #[test]
    fn discard_from_pool_clears_can_pooled() {
        use crate::client::TimeoutSettings;
        use crate::grpc_connection_manager::GrpcConnectionManager;
        use crate::grpc_wrapper::grpc_limits::DEFAULT_GRPC_MESSAGE_SIZE_LIMIT_BYTES;
        use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
        use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};
        use crate::session::NodePinnedTableClient;
        use http::Uri;

        let mut session = Session::new(
            "test-session".to_string(),
            NodePinnedTableClient::new(
                GrpcConnectionManager::new(
                    SharedLoadBalancer::new_with_balancer(Box::new(StaticLoadBalancer::new(
                        Uri::from_static("http://127.0.0.1/bench"),
                    ))),
                    "bench".to_string(),
                    MultiInterceptor::new(),
                    None,
                    DEFAULT_GRPC_MESSAGE_SIZE_LIMIT_BYTES,
                )
                .unwrap(),
                Uri::from_static("http://127.0.0.1/bench"),
            ),
            TimeoutSettings::default(),
        );
        assert!(session.can_pooled);
        session.discard_from_pool();
        assert!(!session.can_pooled);
    }
}

#[async_trait::async_trait]
pub(crate) trait CreateTableClient: Send + Sync {
    async fn create_grpc_table_client(&self) -> YdbResult<TableServiceClient<InterceptedChannel>>;
    async fn create_table_client(&self, timeouts: TimeoutSettings) -> YdbResult<RawTableClient>;
}

#[async_trait::async_trait]
impl CreateTableClient for GrpcConnectionManager {
    async fn create_grpc_table_client(&self) -> YdbResult<TableServiceClient<InterceptedChannel>> {
        self.get_auth_service(TableServiceClient::<InterceptedChannel>::new)
            .await
    }

    async fn create_table_client(&self, timeouts: TimeoutSettings) -> YdbResult<RawTableClient> {
        self.get_auth_service(RawTableClient::new)
            .await
            .map(|item| item.with_timeout(timeouts))
    }
}

/// Routes table RPCs to the node that owns the pooled query session.
#[derive(Clone)]
pub(crate) struct NodePinnedTableClient {
    connection_manager: GrpcConnectionManager,
    node_uri: Uri,
}

impl NodePinnedTableClient {
    pub(crate) fn new(connection_manager: GrpcConnectionManager, node_uri: Uri) -> Self {
        Self {
            connection_manager,
            node_uri,
        }
    }
}

#[async_trait::async_trait]
impl CreateTableClient for NodePinnedTableClient {
    async fn create_grpc_table_client(&self) -> YdbResult<TableServiceClient<InterceptedChannel>> {
        self.connection_manager
            .get_auth_service_to_node(
                TableServiceClient::<InterceptedChannel>::new,
                &self.node_uri,
            )
            .await
    }

    async fn create_table_client(&self, timeouts: TimeoutSettings) -> YdbResult<RawTableClient> {
        self.connection_manager
            .get_auth_service_to_node(RawTableClient::new, &self.node_uri)
            .await
            .map(|item| item.with_timeout(timeouts))
    }
}
