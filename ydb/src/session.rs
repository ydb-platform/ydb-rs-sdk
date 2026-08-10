use crate::client::TimeoutSettings;
use crate::errors::{YdbError, YdbResult};

use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_table_service::client::RawTableClient;

use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use crate::session_pool::SessionPoolLease;

/// Pooled table session used internally for DDL and describe RPCs.
pub(crate) struct TableSession {
    lease: SessionPoolLease,
    connection_manager: GrpcConnectionManager,
    timeouts: TimeoutSettings,
}

impl TableSession {
    pub(crate) fn new(
        lease: SessionPoolLease,
        connection_manager: GrpcConnectionManager,
        timeouts: TimeoutSettings,
    ) -> Self {
        Self {
            lease,
            connection_manager,
            timeouts,
        }
    }

    pub(crate) fn operation_params(&self) -> RawOperationParams {
        self.timeouts.operation_params()
    }

    pub(crate) fn session_id(&self) -> &str {
        self.lease.session_id()
    }

    /// Return the session after a known-local failure, before any RPC has started.
    pub(crate) fn return_to_pool(self) {
        self.lease.return_to_pool();
    }

    /// Consume this lease for one table RPC. Connection and RPC errors are classified for safe
    /// reuse; cancellation or a session-breaking error schedules cleanup.
    pub(crate) async fn in_flight_rpc<T>(
        self,
        rpc: impl AsyncFnOnce(&mut RawTableClient) -> RawResult<T>,
    ) -> YdbResult<T> {
        let Self {
            lease,
            connection_manager,
            timeouts,
        } = self;
        let result = async {
            lease.ensure_healthy()?;
            let mut table = connection_manager
                .get_auth_service_to_node(RawTableClient::new, lease.node_uri())
                .await?
                .with_timeout(timeouts);
            rpc(&mut table).await.map_err(YdbError::from)
        }
        .await;
        lease.finish(result)
    }

    pub fn with_timeouts(mut self, timeouts: TimeoutSettings) -> Self {
        self.timeouts = timeouts;
        self
    }
}
