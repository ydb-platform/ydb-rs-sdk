//! One-shot `EXPLAIN`: compile a query and return its plan without running it.
//!
//! Uses the ordinary [`RawExecuteQueryRequest`] / [`RawQueryClient::execute_query`] path with
//! [`RawExecMode::Explain`], kept minimal: implicit session, no transaction control, no
//! parameters. `EXPLAIN` responses carry no result sets — only the plan and AST in `exec_stats`.

use std::collections::HashMap;
use std::future::IntoFuture;
use std::time::Duration;

use futures_util::future::BoxFuture;
use tracing::instrument;

use crate::closure;
use crate::errors::{Idempotency, YdbError, YdbResult};
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;
use crate::grpc_wrapper::raw_query_service::execute_query::{
    RawExecMode, RawExecuteQueryRequest, RawQueryStatsPlan,
};
use crate::grpc_wrapper::raw_query_service::stream::ExecuteQueryStream;
use crate::traces::helpers::ensure_len_string;

use super::exec::ClientExecContext;

/// Query plan and AST produced by [`QueryClient::explain`](crate::QueryClient::explain).
///
/// Both fields are the server's strings, passed through unchanged — their format depends on the
/// YDB version, and the SDK neither parses nor reformats them.
#[derive(Debug)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub struct ExplainResult {
    /// Execution plan, as JSON.
    pub query_plan: String,
    /// Compiled MiniKQL AST.
    pub query_ast: String,
}

/// Awaitable builder for [`QueryClient::explain`](crate::QueryClient::explain).
///
/// Options are deliberately limited to [`timeout`](Self::timeout): the call carries no
/// parameters, no transaction control and no workload-manager pool. Further options can be added
/// as new methods without breaking callers.
pub struct ExplainQueryBuilder<'a> {
    ctx: &'a ClientExecContext,
    text: String,
    timeout: Option<Duration>,
}

impl<'a> ExplainQueryBuilder<'a> {
    pub(crate) fn new(ctx: &'a ClientExecContext, text: String) -> Self {
        Self {
            ctx,
            text,
            timeout: None,
        }
    }

    /// Wall-clock limit for the call, retries included.
    ///
    /// `EXPLAIN` never mutates state, so retriable errors are always retried as idempotent.
    /// Without a timeout, retries continue until a non-retriable error.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    #[cfg(test)]
    pub(crate) fn configured_timeout(&self) -> Option<Duration> {
        self.timeout
    }
}

#[instrument(name = "ydb.Query.ExplainOnce", skip_all, fields(db.system.name = "ydb"), err)]
async fn explain_query_once(
    ctx: &ClientExecContext,
    text: &str,
) -> YdbResult<Option<RawQueryStatsPlan>> {
    let mut client = ctx
        .connection_manager
        .get_auth_service(RawQueryClient::new)
        .await?;

    // Implicit session: EXPLAIN holds no server-side state, so no pool lease is taken.
    let mut req = RawExecuteQueryRequest::new("", text, HashMap::new(), None, false);
    req.exec_mode = RawExecMode::Explain;

    let mut stream = ExecuteQueryStream::new(client.execute_query(req).await?);
    // Drains the stream and checks every part's status. EXPLAIN sends no result sets, so the
    // returned vector is empty; the plan arrives as stream metadata.
    stream.materialize_all_result_sets().await?;
    Ok(stream.take_query_plan())
}

#[instrument(name = "ydb.Query.Explain", skip_all, fields(db.system.name = "ydb", ydb.Query.text = %ensure_len_string(&text)), err)]
async fn explain_query(
    ctx: &ClientExecContext,
    text: String,
    timeout: Option<Duration>,
) -> YdbResult<ExplainResult> {
    // EXPLAIN never executes the query, so retrying is always safe.
    let plan = ctx
        .retry_settings
        .clone()
        .with_deadline(timeout)
        .retry_on_retriable_errors(
            Idempotency::Idempotent,
            closure!([&ctx, &text], async |_| explain_query_once(ctx, text).await),
        )
        .await?
        .ok_or_else(|| {
            // Seen with statements that have nothing to plan, e.g. DDL: the server accepts the
            // EXPLAIN but sends no exec_stats.
            YdbError::Custom(
                "EXPLAIN returned no query plan: the response carried no exec_stats. \
                 Statements without an execution plan (such as DDL) cannot be explained"
                    .to_string(),
            )
        })?;

    Ok(ExplainResult {
        query_plan: plan.query_plan,
        query_ast: plan.query_ast,
    })
}

impl<'a> IntoFuture for ExplainQueryBuilder<'a> {
    type Output = YdbResult<ExplainResult>;
    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(explain_query(self.ctx, self.text, self.timeout))
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::GrpcOptions;
    use crate::grpc_connection_manager::GrpcConnectionManager;
    use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
    use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};
    use crate::retry_settings::RetrySettings;
    use crate::session_pool::{SessionPool, SessionPoolSettings};
    use http::Uri;

    /// Context pointing at a closed port: every attempt fails with a retriable transport error,
    /// so the only thing that can end the retry loop is the deadline.
    fn unreachable_ctx() -> ClientExecContext {
        ClientExecContext {
            connection_manager: GrpcConnectionManager::new(
                SharedLoadBalancer::new_with_balancer(Box::new(StaticLoadBalancer::new(
                    Uri::from_static("http://127.0.0.1:1"),
                ))),
                "test".to_string(),
                MultiInterceptor::new(),
                GrpcOptions::default(),
            ),
            session_pool: SessionPool::new_explicit_bench(SessionPoolSettings::new().with_limit(1)),
            retry_settings: RetrySettings::with_default_backoff(),
        }
    }

    #[test]
    fn timeout_is_recorded_only_when_set() {
        let ctx = unreachable_ctx();
        assert_eq!(
            ExplainQueryBuilder::new(&ctx, "SELECT 1".to_string()).configured_timeout(),
            None
        );
        assert_eq!(
            ExplainQueryBuilder::new(&ctx, "SELECT 1".to_string())
                .timeout(Duration::from_millis(250))
                .configured_timeout(),
            Some(Duration::from_millis(250))
        );
    }

    /// Paired with [`without_timeout_the_retry_loop_keeps_going`], which shows the same error is
    /// retriable: the loop ends here only because `.timeout()` became the retry deadline.
    #[tokio::test]
    async fn timeout_bounds_the_retry_loop() {
        let ctx = unreachable_ctx();
        let start = std::time::Instant::now();
        let err = ExplainQueryBuilder::new(&ctx, "SELECT 1".to_string())
            .timeout(Duration::from_millis(200))
            .await
            .expect_err("unreachable endpoint must fail");

        assert!(
            start.elapsed() < Duration::from_secs(5),
            "call outlived its deadline by too much: {:?}",
            start.elapsed()
        );
        assert!(
            matches!(
                err,
                YdbError::DeadlineExceeded
                    | YdbError::Transport(_)
                    | YdbError::TransportGRPCStatus(_)
            ),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn without_timeout_the_retry_loop_keeps_going() {
        let ctx = unreachable_ctx();
        let outcome = tokio::time::timeout(
            Duration::from_millis(300),
            ExplainQueryBuilder::new(&ctx, "SELECT 1".to_string()).into_future(),
        )
        .await;

        assert!(
            outcome.is_err(),
            "without .timeout() the call must keep retrying, got {outcome:?}"
        );
    }
}
