use crate::client::TimeoutSettings;
use crate::client_table::{TableServiceChannelPool, TableServiceClientType};
use crate::errors::{YdbError, YdbResult};
use crate::grpc::{grpc_read_operation_result, grpc_read_void_operation_result, operation_params};
use crate::query::Query;
use crate::result::{QueryResult, StreamResult};
use crate::trait_operation::Operation;
use derivative::Derivative;
use std::sync::atomic::{AtomicI64, Ordering};
use tracing::field::debug;
use tracing::{debug, trace};
use ydb_grpc::ydb_proto::table::keep_alive_result::SessionStatus;
use ydb_grpc::ydb_proto::table::{
    execute_scan_query_request, CommitTransactionRequest, CommitTransactionResult,
    ExecuteDataQueryRequest, ExecuteQueryResult, ExecuteScanQueryRequest,
    ExecuteSchemeQueryRequest, KeepAliveRequest, KeepAliveResult, RollbackTransactionRequest,
};

static request_number: AtomicI64 = AtomicI64::new(0);

fn req_number() -> i64 {
    request_number.fetch_add(1, Ordering::Relaxed)
}

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Session {
    pub(crate) id: String,

    pub(crate) can_pooled: bool,

    #[derivative(Debug = "ignore")]
    on_drop_callbacks: Vec<Box<dyn FnOnce(&mut Self) + Send + Sync>>,

    #[derivative(Debug = "ignore")]
    channel_pool: TableServiceChannelPool,

    timeouts: TimeoutSettings,
}

impl Session {
    pub(crate) fn new(
        id: String,
        channel_pool: TableServiceChannelPool,
        timeouts: TimeoutSettings,
    ) -> Self {
        return Self {
            id,
            can_pooled: true,
            on_drop_callbacks: Vec::new(),
            channel_pool,
            timeouts,
        };
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
        return res;
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
        return Ok(());
    }

    pub(crate) async fn execute_schema_query(&mut self, query: String) -> YdbResult<()> {
        let resp = self
            .channel_pool
            .create_channel()
            .await?
            .execute_scheme_query(ExecuteSchemeQueryRequest {
                session_id: self.id.clone(),
                yql_text: query,
                operation_params: operation_params(self.timeouts.operation_timeout),
                ..ExecuteSchemeQueryRequest::default()
            })
            .await?;

        return grpc_read_void_operation_result(resp);
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

        debug!("request: {}", serde_json::to_string(&req)?);

        let mut channel = self.get_channel().await?;
        let response = channel.execute_data_query(req).await?;
        let operation_result: ExecuteQueryResult = self.handle_operation_result(response)?;

        debug!("response: {}", serde_json::to_string(&operation_result)?);

        return QueryResult::from_proto(operation_result, error_on_truncated);
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
        let mut channel = self.channel_pool.create_channel().await?;
        let resp = channel.stream_execute_scan_query(req).await?;
        let stream = resp.into_inner();
        return Ok(StreamResult { results: stream });
    }

    pub(crate) async fn rollback_transaction(&mut self, tx_id: String) -> YdbResult<()> {
        let mut channel = self.get_channel().await?;

        // todo: retry commit always idempotent
        let response = channel
            .rollback_transaction(RollbackTransactionRequest {
                session_id: self.id.clone(),
                tx_id,
                operation_params: operation_params(self.timeouts.operation_timeout),
                ..RollbackTransactionRequest::default()
            })
            .await?;
        let res = grpc_read_void_operation_result(response);
        return match res {
            Ok(()) => Ok(()),
            Err(err) => {
                self.handle_error(&err);
                return Err(err);
            }
        };
    }

    pub(crate) async fn keepalive(&mut self) -> YdbResult<()> {
        let mut channel = self.get_channel().await?;
        let res: YdbResult<KeepAliveResult> = grpc_read_operation_result(
            channel
                .keep_alive(KeepAliveRequest {
                    session_id: self.id.clone(),
                    operation_params: operation_params(self.timeouts.operation_timeout),
                    ..KeepAliveRequest::default()
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
        return Err(YdbError::Custom(format!(
            "bad status while session ping: {:?}",
            keepalive_res
        )));
    }

    pub fn with_timeouts(mut self, timeouts: TimeoutSettings) -> Self {
        self.timeouts = timeouts;
        return self;
    }

    async fn get_channel(&self) -> YdbResult<TableServiceClientType> {
        return self.channel_pool.create_channel().await;
    }

    #[allow(dead_code)]
    pub(crate) fn on_drop(&mut self, f: Box<dyn FnOnce(&mut Self) + Send + Sync>) {
        self.on_drop_callbacks.push(f)
    }

    pub(crate) fn clone_without_ondrop(&self) -> Self {
        return Self {
            id: self.id.clone(),
            can_pooled: self.can_pooled,
            on_drop_callbacks: Vec::new(),
            channel_pool: self.channel_pool.clone(),
            timeouts: self.timeouts,
        };
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
