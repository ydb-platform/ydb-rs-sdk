use crate::client::TimeoutSettings;
use crate::client_table::TableServiceClientType;
use crate::errors::{YdbError, YdbResult};
use crate::grpc::{grpc_read_operation_result, operation_params};
use crate::query::Query;
use crate::result::{QueryResult, StreamResult};
use crate::trait_operation::Operation;
use derivative::Derivative;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_table_service::client::{
    CollectStatsMode, RawTableClient, SessionStatus,
};
use crate::grpc_wrapper::runtime_interceptors::InterceptedChannel;

use crate::grpc_wrapper::raw_errors::RawResult;
use crate::grpc_wrapper::raw_table_service::commit_transaction::RawCommitTransactionRequest;
use crate::grpc_wrapper::raw_table_service::execute_scheme_query::RawExecuteSchemeQueryRequest;
use crate::grpc_wrapper::raw_table_service::keepalive::RawKeepAliveRequest;
use crate::grpc_wrapper::raw_table_service::rollback_transaction::RawRollbackTransactionRequest;
use crate::trace_helpers::ensure_len_string;
use tracing::{debug, trace};
use ydb_grpc::ydb_proto::table::v1::table_service_client::TableServiceClient;
use ydb_grpc::ydb_proto::table::{
    execute_scan_query_request, ExecuteDataQueryRequest, ExecuteQueryResult,
    ExecuteScanQueryRequest,
};

static REQUEST_NUMBER: AtomicI64 = AtomicI64::new(0);
static DEFAULT_COLLECT_STAT_MODE: CollectStatsMode = CollectStatsMode::None;

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

    fn handle_raw_result<T>(&mut self, res: RawResult<T>) -> YdbResult<T> {
        let res = res.map_err(YdbError::from);
        if let Err(err) = &res {
            self.handle_error(err);
        }
        res
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
        let mut table = self.get_table_client().await?;
        let res = table
            .commit_transaction(RawCommitTransactionRequest {
                session_id: self.id.clone(),
                tx_id,
                operation_params: self.timeouts.operation_params(),
                collect_stats: DEFAULT_COLLECT_STAT_MODE,
            })
            .await;
        self.handle_raw_result(res)?;
        Ok(())
    }

    pub(crate) async fn execute_schema_query(&mut self, query: String) -> YdbResult<()> {
        let res = self
            .get_table_client()
            .await?
            .execute_scheme_query(RawExecuteSchemeQueryRequest {
                session_id: self.id.clone(),
                yql_text: query,
                operation_params: self.timeouts.operation_params(),
            })
            .await;
        self.handle_raw_result(res)?;
        Ok(())
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
        debug!("response: {}", serde_json::to_string(&response.get_ref())?);

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
        let mut table = self.get_table_client().await?;
        let res = table
            .rollback_transaction(RawRollbackTransactionRequest {
                session_id: self.id.clone(),
                tx_id,
                operation_params: self.timeouts.operation_params(),
            })
            .await;

        self.handle_raw_result(res)
    }

    pub(crate) async fn keepalive(&mut self) -> YdbResult<()> {
        let mut table = self.get_table_client().await?;
        let res = table
            .keep_alive(RawKeepAliveRequest {
                operation_params: self.timeouts.operation_params(),
                session_id: self.id.clone(),
            })
            .await;

        let res = self.handle_raw_result(res)?;

        if let SessionStatus::Ready = res.session_status {
            Ok(())
        } else {
            let err = YdbError::from_str(format!("bad status while session ping: {:?}", res));
            self.handle_error(&err);
            Err(err)
        }
    }

    pub fn with_timeouts(mut self, timeouts: TimeoutSettings) -> Self {
        self.timeouts = timeouts;
        self
    }

    // deprecated, use get_table_client instead
    async fn get_channel(&self) -> YdbResult<TableServiceClientType> {
        self.channel_pool.create_grpc_table_client().await
    }

    async fn get_table_client(&self) -> YdbResult<RawTableClient> {
        self.channel_pool.create_table_client(self.timeouts).await
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
    async fn create_table_client(&self, timeouts: TimeoutSettings) -> YdbResult<RawTableClient>;
    fn clone_box(&self) -> Box<dyn CreateTableClient>;
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

    fn clone_box(&self) -> Box<dyn CreateTableClient> {
        Box::new(self.clone())
    }
}
