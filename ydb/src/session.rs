use crate::client::TimeoutSettings;
use crate::errors::{YdbError, YdbResult};

use derivative::Derivative;
use http::Uri;

use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_table_service::client::RawTableClient;

use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use tracing::trace;

type DropSessionCallback = dyn FnOnce(&mut TableSession) + Send + Sync;

/// If an RPC is cancelled mid-flight (e.g. operation timeout), the server may still be
/// processing it. Mark the session non-poolable so the next lease gets a fresh session
/// instead of hitting SessionBusy on reuse (aligned with go-sdk context-error handling).
struct InFlightTableRpcGuard<'a> {
    session: &'a mut TableSession,
    active: bool,
}

impl Drop for InFlightTableRpcGuard<'_> {
    fn drop(&mut self) {
        if self.active {
            self.session.discard_from_pool();
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
/// Pooled table session used internally for DDL and describe RPCs.
pub(crate) struct TableSession {
    pub(crate) id: String,

    pub(crate) can_pooled: bool,

    #[derivative(Debug = "ignore")]
    on_drop_callbacks: Vec<Box<DropSessionCallback>>,

    #[derivative(Debug = "ignore")]
    channel_pool: Box<dyn CreateTableClient>,

    timeouts: TimeoutSettings,
}

impl TableSession {
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

    pub fn with_timeouts(mut self, timeouts: TimeoutSettings) -> Self {
        self.timeouts = timeouts;
        self
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

impl Drop for TableSession {
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

        let mut session = TableSession::new(
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
    async fn create_table_client(&self, timeouts: TimeoutSettings) -> YdbResult<RawTableClient>;
}

#[async_trait::async_trait]
impl CreateTableClient for GrpcConnectionManager {
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
    async fn create_table_client(&self, timeouts: TimeoutSettings) -> YdbResult<RawTableClient> {
        self.connection_manager
            .get_auth_service_to_node(RawTableClient::new, &self.node_uri)
            .await
            .map(|item| item.with_timeout(timeouts))
    }
}
