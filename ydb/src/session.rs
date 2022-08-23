use crate::client::TimeoutSettings;
use crate::client_table::TableServiceClientType;
use crate::errors::{YdbError, YdbResult};
use crate::grpc::{grpc_read_operation_result, grpc_read_void_operation_result, operation_params};
use crate::query::Query;
use crate::result::{QueryResult, StreamResult};
use crate::trait_operation::Operation;
use derivative::Derivative;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_table_service::client::RawTableClient;
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;

use crate::trace_helpers::ensure_len_string;
use tracing::{debug, trace};
use ydb_grpc::ydb_proto::table::keep_alive_result::SessionStatus;
use ydb_grpc::ydb_proto::table::v1::table_service_client::TableServiceClient;
use ydb_grpc::ydb_proto::table::{
    execute_scan_query_request, CommitTransactionRequest, CommitTransactionResult,
    ExecuteDataQueryRequest, ExecuteQueryResult, ExecuteScanQueryRequest,
    ExecuteSchemeQueryRequest, KeepAliveRequest, KeepAliveResult, RollbackTransactionRequest,
};

static REQUEST_NUMBER: AtomicI64 = AtomicI64::new(0);

fn req_number() -> i64 {
    REQUEST_NUMBER.fetch_add(1, Ordering::Relaxed)
}

type DropSessionCallback = dyn FnOnce(&mut Session) + Send + Sync;

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
        if let YdbError::YdbStatusError(err) = err {
            use ydb_grpc::ydb_proto::status_ids::StatusCode;
            if let Some(status) = StatusCode::from_i32(err.operation_status) {
                if status == StatusCode::BadSession || status == StatusCode::SessionExpired {
                    self.can_pooled = false;
                }
            }
        }
    }

    fn handle_operation_result<TOp, T>(&mut self, response: tonic::Response<TOp>) -> YdbResult<T>
    where
        TOp: Operation,
        T: Default + prost::Message,
    {
        let res: YdbResult<T> = grpc_read_operation_result(response);
        if let Err(err) = &res {
            self.handle_error(err);
        }
        res
    }

    pub(crate) async fn commit_transaction(&mut self, tx_id: String) -> YdbResult<()> {
        let mut channel = self.get_channel().await?;

        // todo: retry commit always idempotent
        let response = channel
            .commit_transaction(CommitTransactionRequest {
                session_id: self.id.clone(),
                tx_id,
                operation_params: operation_params(self.timeouts.operation_timeout),
                ..CommitTransactionRequest::default()
            })
            .await?;
        let _: CommitTransactionResult = self.handle_operation_result(response)?;
        Ok(())
    }

    pub(crate) async fn execute_schema_query(&mut self, query: String) -> YdbResult<()> {
        let resp = self
            .get_channel()
            .await?
            .execute_scheme_query(ExecuteSchemeQueryRequest {
                session_id: self.id.clone(),
                yql_text: query,
                operation_params: operation_params(self.timeouts.operation_timeout),
            })
            .await?;

        grpc_read_void_operation_result(resp)
    }

    #[tracing::instrument(skip(self, req), fields(req_number=req_number()))]
    pub(crate) async fn execute_data_query(
        &mut self,
        mut req: ExecuteDataQueryRequest,
        error_on_truncated: bool,
    ) -> YdbResult<QueryResult> {
        req.session_id.clone_from(&self.id);
        if req.operation_params.is_none() {
            req.operation_params = operation_params(self.timeouts.operation_timeout)
        }

        trace!(
            "request: {}",
            ensure_len_string(serde_json::to_string(&req)?)
        );

        let mut channel = self.get_channel().await?;
        let response = channel.execute_data_query(req).await?;
        let operation_result: ExecuteQueryResult = self.handle_operation_result(response)?;

        trace!(
            "response: {}",
            ensure_len_string(serde_json::to_string(&operation_result)?)
        );

        QueryResult::from_proto(operation_result, error_on_truncated)
    }

    #[tracing::instrument(skip(self, query), fields(req_number=req_number()))]
    pub async fn execute_scan_query(&mut self, query: Query) -> YdbResult<StreamResult> {
        let req = ExecuteScanQueryRequest {
            query: Some(query.query_to_proto()),
            parameters: query.params_to_proto()?,
            mode: execute_scan_query_request::Mode::Exec as i32,
            ..ExecuteScanQueryRequest::default()
        };
        debug!("request: {}", serde_json::to_string(&req)?);
        let mut channel = self.get_channel().await?;
        let resp = channel.stream_execute_scan_query(req).await?;
        let stream = resp.into_inner();
        Ok(StreamResult { results: stream })
    }

    pub(crate) async fn rollback_transaction(&mut self, tx_id: String) -> YdbResult<()> {
        let mut channel = self.get_channel().await?;

        // todo: retry commit always idempotent
        let response = channel
            .rollback_transaction(RollbackTransactionRequest {
                session_id: self.id.clone(),
                tx_id,
                operation_params: operation_params(self.timeouts.operation_timeout),
            })
            .await?;
        let res = grpc_read_void_operation_result(response);
        match res {
            Ok(()) => Ok(()),
            Err(err) => {
                self.handle_error(&err);
                Err(err)
            }
        }
    }

    pub(crate) async fn keepalive(&mut self) -> YdbResult<()> {
        let mut channel = self.get_channel().await?;
        let res: YdbResult<KeepAliveResult> = grpc_read_operation_result(
            channel
                .keep_alive(KeepAliveRequest {
                    session_id: self.id.clone(),
                    operation_params: operation_params(self.timeouts.operation_timeout),
                })
                .await?,
        );

        let keepalive_res = match res {
            Err(err) => {
                self.handle_error(&err);
                return Err(err);
            }
            Ok(res) => res,
        };

        if SessionStatus::from_i32(keepalive_res.session_status) == Some(SessionStatus::Ready) {
            return Ok(());
        }

        Err(YdbError::Custom(format!(
            "bad status while session ping: {:?}",
            keepalive_res
        )))
    }

    pub fn with_timeouts(mut self, timeouts: TimeoutSettings) -> Self {
        self.timeouts = timeouts;
        self
    }

    async fn get_channel(&self) -> YdbResult<TableServiceClientType> {
        self.channel_pool.create_grpc_table_client().await
    }

    #[allow(dead_code)]
    pub(crate) fn on_drop(&mut self, f: Box<dyn FnOnce(&mut Self) + Send + Sync>) {
        self.on_drop_callbacks.push(f)
    }

    pub(crate) fn clone_without_ondrop(&self) -> Self {
        Self {
            id: self.id.clone(),
            can_pooled: self.can_pooled,
            on_drop_callbacks: Vec::new(),
            channel_pool: self.channel_pool.clone_box(),
            timeouts: self.timeouts,
        }
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

#[async_trait::async_trait]
pub(crate) trait CreateTableClient: Send + Sync {
    async fn create_grpc_table_client(&self) -> YdbResult<TableServiceClient<InterceptedChannel>>;
    async fn create_table_client(&self, operation_timeout: Duration) -> YdbResult<RawTableClient>;
    fn clone_box(&self) -> Box<dyn CreateTableClient>;
}

#[async_trait::async_trait]
impl CreateTableClient for GrpcConnectionManager {
    async fn create_grpc_table_client(&self) -> YdbResult<TableServiceClient<InterceptedChannel>> {
        self.get_auth_service(TableServiceClient::<InterceptedChannel>::new)
            .await
    }

    async fn create_table_client(&self, operation_timeout: Duration) -> YdbResult<RawTableClient> {
        self.get_auth_service(RawTableClient::new)
            .await
            .map(|item| item.with_timeout(operation_timeout))
    }

    fn clone_box(&self) -> Box<dyn CreateTableClient> {
        Box::new(self.clone())
    }
}
