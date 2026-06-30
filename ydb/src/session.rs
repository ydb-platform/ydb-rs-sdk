use crate::client::TimeoutSettings;
use crate::client_table::TableServiceClientType;
use crate::errors::{YdbError, YdbResult};
use crate::query::Query;
use crate::result::{QueryResult, StreamReadTableResult, StreamResult};
use crate::table_requests::{PreparedDataQuery, ReadTableOptions};
use derivative::Derivative;
use itertools::Itertools;
use std::sync::atomic::{AtomicI64, Ordering};

use http::Uri;

use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_table_service::client::{CollectStatsMode, RawTableClient};
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;

use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_table_service::commit_transaction::RawCommitTransactionRequest;
use crate::grpc_wrapper::raw_table_service::execute_data_query::RawExecuteDataQueryRequest;
use crate::grpc_wrapper::raw_table_service::query_stats::RawQueryStatMode;
use crate::grpc_wrapper::raw_table_service::transaction_control::{
    RawTransactionControl, RawTxSelector, RawTxSettings,
};
use crate::grpc_wrapper::raw_table_service::prepare_data_query::{
    RawPrepareDataQueryRequest, RawPrepareDataQueryResult,
};
use crate::grpc_wrapper::raw_table_service::rollback_transaction::RawRollbackTransactionRequest;
use crate::grpc_wrapper::raw_table_service::stream_read_table::RawStreamReadTableRequest;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use crate::transaction::Mode;
use crate::trace_helpers::ensure_len_string;
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
/// Pooled table session (go-sdk: `table.Session`).
///
/// Not constructed by user code directly — obtain from [`TableClient::retry`] (go-sdk: `Client.Do`).
pub struct Session {
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

    pub(crate) fn operation_params(&self) -> RawOperationParams {
        self.timeouts.operation_params()
    }

    /// Run a table RPC under [`InFlightTableRpcGuard`] (discard session on cancel/timeout).
    pub(crate) async fn in_flight_rpc<T>(
        &mut self,
        rpc: impl AsyncFnOnce(&mut RawTableClient) -> RawResult<T>,
    ) -> YdbResult<T> {
        let mut table = self.get_table_client().await?;
        let mut guard = InFlightTableRpcGuard {
            session: self,
            active: true,
        };
        let res = rpc(&mut table).await;
        guard.active = false;
        guard.session.handle_raw_result(res)
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

    #[tracing::instrument(skip(self, req), fields(req_number=req_number()))]
    pub(crate) async fn execute_data_query(
        &mut self,
        mut req: RawExecuteDataQueryRequest,
        ignore_truncated: bool,
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
        QueryResult::from_raw_result(ignore_truncated, res)
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
            crate::trace_helpers::ensure_len_string(serde_json::to_string(&req)?)
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

    pub async fn prepare_data_query(
        &mut self,
        yql_text: String,
    ) -> YdbResult<PreparedDataQuery> {
        let req = RawPrepareDataQueryRequest {
            session_id: self.id.clone(),
            yql_text: yql_text.clone(),
            operation_params: self.timeouts.operation_params(),
        };
        let raw: RawPrepareDataQueryResult =
            in_flight_table_rpc!(self, table, table.prepare_data_query(req))?;
        Ok(PreparedDataQuery {
            query_id: raw.query_id,
            yql_text,
        })
    }

    pub async fn stream_read_table(
        &mut self,
        path: String,
        options: ReadTableOptions,
    ) -> YdbResult<StreamReadTableResult> {
        let key_range = options
            .key_range
            .map(|range| range.into_raw())
            .transpose()?;
        let req = RawStreamReadTableRequest {
            session_id: self.id.clone(),
            path,
            key_range,
            columns: options.columns,
            ordered: options.ordered,
            row_limit: options.row_limit,
        };
        let mut in_flight = InFlightTableRpcGuard {
            session: self,
            active: true,
        };
        let stream = match in_flight.session.get_table_client().await {
            Ok(mut client) => client.stream_read_table(req).await,
            Err(err) => {
                in_flight.active = false;
                return Err(err);
            }
        };
        in_flight.active = false;
        let stream = in_flight.session.handle_raw_result(stream)?;
        Ok(StreamReadTableResult { parts: stream })
    }

    /// Execute a prepared data query (go-sdk: `Statement.Execute`).
    pub async fn execute_prepared_query(
        &mut self,
        prepared: &PreparedDataQuery,
        query: Query,
        mode: Mode,
    ) -> YdbResult<QueryResult> {
        let params = query
            .parameters
            .into_iter()
            .map(|(k, v)| v.try_into().map(|converted| (k, converted)))
            .try_collect()?;
        let req = RawExecuteDataQueryRequest {
            session_id: String::new(),
            tx_control: RawTransactionControl {
                commit_tx: true,
                tx_selector: RawTxSelector::Begin(RawTxSettings {
                    mode: mode.into(),
                }),
            },
            yql_text: String::new(),
            query_id: None,
            operation_params: self.timeouts.operation_params(),
            params,
            keep_in_cache: query.keep_in_cache,
            collect_stats: RawQueryStatMode::None,
        };
        self.execute_prepared_data_query(prepared, req, false).await
    }

    pub(crate) async fn execute_prepared_data_query(
        &mut self,
        prepared: &PreparedDataQuery,
        req: RawExecuteDataQueryRequest,
        ignore_truncated: bool,
    ) -> YdbResult<QueryResult> {
        let mut req = req;
        req.session_id.clone_from(&self.id);
        req.query_id = Some(prepared.query_id.clone());
        req.yql_text.clear();
        req.operation_params = self.timeouts.operation_params();
        self.execute_data_query(req, ignore_truncated).await
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
                ),
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
