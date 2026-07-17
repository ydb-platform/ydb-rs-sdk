use std::collections::HashMap;
use std::future::IntoFuture;
use std::time::Duration;

use crate::client::TimeoutSettings;
use crate::closure;
use crate::errors::{Idempotency, YdbError, YdbResult};
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;
use crate::grpc_wrapper::raw_query_service::execute_script::RawExecuteScriptRequest;
use crate::grpc_wrapper::raw_query_service::fetch_script_results::RawFetchScriptResultsRequest;
use crate::result::ResultSet;
use crate::types::Value;

use futures_util::future::BoxFuture;
use tracing::instrument;

use super::exec::{CallOptions, ClientExecContext, maybe_with_operation_timeout};

/// Long-running script operation started by [`QueryClient::execute_script`].
#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub struct ExecuteScriptOperation {
    pub id: String,
    pub consumed_units: Option<f64>,
}

/// One page of script results from [`QueryClient::fetch_script_results`].
#[derive(Debug)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub struct FetchScriptResult {
    pub result_set_index: i64,
    pub result_set: ResultSet,
    pub next_fetch_token: String,
}

pub struct ExecuteScriptBuilder<'a> {
    ctx: &'a ClientExecContext,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
    results_ttl: Option<Duration>,
}

impl<'a> ExecuteScriptBuilder<'a> {
    pub(crate) fn new(ctx: &'a ClientExecContext, text: String) -> Self {
        Self {
            ctx,
            text,
            params: HashMap::new(),
            opts: CallOptions::default(),
            results_ttl: None,
        }
    }

    /// TTL for script results on the server after execution completes.
    pub fn results_ttl(mut self, ttl: Duration) -> Self {
        self.results_ttl = Some(ttl);
        self
    }

    pub fn param(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.params.insert(name.into(), value.into());
        self
    }

    pub fn params(mut self, params: HashMap<String, Value>) -> Self {
        self.params.extend(params);
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.opts.timeout = Some(timeout);
        self
    }
}

impl<'a> IntoFuture for ExecuteScriptBuilder<'a> {
    type Output = YdbResult<ExecuteScriptOperation>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let results_ttl = self.results_ttl.ok_or_else(|| {
                YdbError::Custom("execute_script requires `.results_ttl(...)`".into())
            })?;
            client_execute_script(self.ctx, self.text, self.params, self.opts, results_ttl).await
        })
    }
}

pub struct FetchScriptResultsBuilder<'a> {
    ctx: &'a ClientExecContext,
    operation_id: String,
    result_set_index: i64,
    fetch_token: String,
    rows_limit: i64,
    opts: CallOptions,
}

impl<'a> FetchScriptResultsBuilder<'a> {
    pub(crate) fn new(ctx: &'a ClientExecContext, operation_id: String) -> Self {
        Self {
            ctx,
            operation_id,
            result_set_index: 0,
            fetch_token: String::new(),
            rows_limit: 0,
            opts: CallOptions::default(),
        }
    }

    pub fn result_set_index(mut self, index: i64) -> Self {
        self.result_set_index = index;
        self
    }

    pub fn fetch_token(mut self, token: impl Into<String>) -> Self {
        self.fetch_token = token.into();
        self
    }

    pub fn rows_limit(mut self, limit: i64) -> Self {
        self.rows_limit = limit;
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.opts.timeout = Some(timeout);
        self
    }
}

impl<'a> IntoFuture for FetchScriptResultsBuilder<'a> {
    type Output = YdbResult<FetchScriptResult>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            client_fetch_script_results(
                self.ctx,
                self.operation_id,
                self.result_set_index,
                self.fetch_token,
                self.rows_limit,
                self.opts,
            )
            .await
        })
    }
}

async fn client_execute_script(
    ctx: &ClientExecContext,
    text: String,
    params: HashMap<String, Value>,
    opts: CallOptions,
    results_ttl: Duration,
) -> YdbResult<ExecuteScriptOperation> {
    // Unlike FetchScriptResults, ExecuteScript is not retried: a server-side start
    // followed by a client transport error would spawn duplicate long-running ops.
    client_execute_script_once(ctx, &text, &params, &opts, results_ttl).await
}

#[instrument(name = "ydb.ExecuteScript", skip_all, fields(db.system.name = "ydb", ydb.Query.text = %crate::traces::helpers::ensure_len_string(text)), err)]
async fn client_execute_script_once(
    ctx: &ClientExecContext,
    text: &str,
    params: &HashMap<String, Value>,
    opts: &CallOptions,
    results_ttl: Duration,
) -> YdbResult<ExecuteScriptOperation> {
    let timeout = opts.timeout;
    let req = RawExecuteScriptRequest {
        yql_text: text.to_string(),
        parameters: params.clone(),
        results_ttl,
        operation_params: TimeoutSettings {
            operation_timeout: timeout,
        }
        .execute_script_operation_params(),
        collect_stats: false,
    };
    let mut client = ctx
        .connection_manager
        .get_auth_service(RawQueryClient::new)
        .await?;
    let (id, consumed_units) = match maybe_with_operation_timeout(timeout, async {
        client.execute_script(req).await.map_err(YdbError::from)
    })
    .await
    {
        Ok(value) => value,
        Err(err) => {
            if matches!(&err, YdbError::Transport(msg) if msg.contains("timed out")) {
                tracing::warn!(
                    ?timeout,
                    "execute_script timed out waiting for RPC response; \
                     a server-side operation may still be running until cancel_after"
                );
            }
            return Err(err);
        }
    };

    Ok(ExecuteScriptOperation { id, consumed_units })
}

#[instrument(name = "ydb.FetchScriptResults", skip_all, fields(db.system.name = "ydb", ydb.operation.id = %operation_id), err)]
async fn client_fetch_script_results(
    ctx: &ClientExecContext,
    operation_id: String,
    result_set_index: i64,
    fetch_token: String,
    rows_limit: i64,
    opts: CallOptions,
) -> YdbResult<FetchScriptResult> {
    // FetchScriptResults is always safe to retry (aligned with Go SDK).
    ctx.retry_budget
        .as_ref()
        .deadline(opts.timeout)
        .retry_on_retriable_errors(
            Idempotency::Idempotent,
            closure!([&ctx, &operation_id, &fetch_token, &opts], async |_| {
                client_fetch_script_results_once(
                    ctx,
                    operation_id,
                    result_set_index,
                    fetch_token,
                    rows_limit,
                    opts,
                )
                .await
            }),
        )
        .await
}

#[instrument(name = "ydb.FetchScriptResultsOnce", skip_all, fields(db.system.name = "ydb", ydb.operation.id = %operation_id), err)]
async fn client_fetch_script_results_once(
    ctx: &ClientExecContext,
    operation_id: &str,
    result_set_index: i64,
    fetch_token: &str,
    rows_limit: i64,
    _opts: &CallOptions,
) -> YdbResult<FetchScriptResult> {
    let req = RawFetchScriptResultsRequest {
        operation_id: operation_id.to_string(),
        result_set_index,
        fetch_token: fetch_token.to_string(),
        rows_limit,
    };
    let mut client = ctx
        .connection_manager
        .get_auth_service(RawQueryClient::new)
        .await?;
    let (index, raw_set, next_token) = client
        .fetch_script_results(req)
        .await
        .map_err(YdbError::from)?;

    Ok(FetchScriptResult {
        result_set_index: index,
        result_set: raw_set.try_into()?,
        next_fetch_token: next_token,
    })
}
