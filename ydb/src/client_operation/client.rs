use std::future::Future;
use std::time::{Duration, Instant};

use rand::Rng;
use tokio::time::{sleep, timeout};
use tracing::instrument;
use ydb_grpc::ydb_proto::status_ids::StatusCode;

use crate::client::TimeoutSettings;
use crate::errors::{NeedRetry, YdbError, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_operation_service::client::RawOperationClient;
use crate::grpc_wrapper::raw_operation_service::types::{
    RawListOperationsRequest, RawListOperationsResult, RawOperation,
};

use super::types::{ListOperationsRequest, ListOperationsResult, OperationInfo};

const DEFAULT_RETRY_BUDGET: Duration = Duration::from_secs(5);
const INITIAL_RETRY_BACKOFF_MILLISECONDS: u64 = 1;
const MAX_RETRY_BACKOFF_MILLISECONDS: u64 = 1_000;

#[derive(Clone)]
pub struct OperationClient {
    connection_manager: GrpcConnectionManager,
    operation_timeout: Duration,
    retry_budget: Duration,
}

impl OperationClient {
    pub(crate) fn new(
        timeouts: TimeoutSettings,
        connection_manager: GrpcConnectionManager,
    ) -> Self {
        Self {
            connection_manager,
            operation_timeout: timeouts.operation_timeout,
            retry_budget: DEFAULT_RETRY_BUDGET,
        }
    }

    /// Total wall-clock budget for automatic retries (aligned with [`crate::TableClient::clone_with_retry_timeout`]).
    pub fn clone_with_retry_timeout(&self, timeout: Duration) -> Self {
        Self {
            retry_budget: timeout,
            ..self.clone()
        }
    }

    pub fn clone_with_no_retry(&self) -> Self {
        Self {
            retry_budget: Duration::ZERO,
            ..self.clone()
        }
    }

    #[instrument(
        name = "ydb.OperationClient.GetOperation",
        skip_all,
        fields(ydb.operation.id = tracing::field::Empty),
        err
    )]
    pub async fn get_operation(&self, id: impl Into<String>) -> YdbResult<OperationInfo> {
        let id = id.into();
        tracing::Span::current().record("ydb.operation.id", &id);
        self.retry(|| async {
            let mut client = self.raw_client().await?;
            let op = self
                .with_rpc_timeout(|| client.get_operation(&id))
                .await
                .map_err(YdbError::from)?;
            Ok(raw_to_operation_info(op))
        })
        .await
    }

    #[instrument(
        name = "ydb.OperationClient.ListOperations", 
        skip_all, 
        fields(ydb.operation.request = ?request), 
        err
    )]
    pub async fn list_operations(
        &self,
        request: ListOperationsRequest,
    ) -> YdbResult<ListOperationsResult> {
        let raw_req = RawListOperationsRequest {
            kind: request.kind,
            page_size: request.page_size,
            page_token: request.page_token,
        };
        self.retry(|| async {
            let mut client = self.raw_client().await?;
            let result = self
                .with_rpc_timeout(|| client.list_operations(raw_req.clone()))
                .await
                .map_err(YdbError::from)?;
            Ok(raw_to_list_result(result))
        })
        .await
    }

    /// Forgets a completed operation on the server.
    ///
    /// If the operation was already forgotten (e.g. a retry after a successful first attempt
    /// that lost the response), `NOT_FOUND` is treated as success.
    #[instrument(
        name = "ydb.OperationClient.ForgetOperation",
        skip_all,
        fields(ydb.operation.id = tracing::field::Empty),
        err
    )]
    pub async fn forget_operation(&self, id: impl Into<String>) -> YdbResult<()> {
        let id = id.into();
        tracing::Span::current().record("ydb.operation.id", &id);
        self.retry(|| async {
            let mut client = self.raw_client().await?;
            match self.with_rpc_timeout(|| client.forget_operation(&id)).await {
                Ok(()) => Ok(()),
                Err(RawError::YdbStatus(status))
                    if status.operation_status == StatusCode::NotFound as i32 =>
                {
                    Ok(())
                }
                Err(err) => Err(YdbError::from(err)),
            }
        })
        .await
    }

    #[instrument(
        name = "ydb.OperationClient.CancelOperation",
        skip_all,
        fields(ydb.operation.id = tracing::field::Empty),
        err
    )]
    pub async fn cancel_operation(&self, id: impl Into<String>) -> YdbResult<()> {
        let id = id.into();
        tracing::Span::current().record("ydb.operation.id", &id);
        self.retry(|| async {
            let mut client = self.raw_client().await?;
            self.with_rpc_timeout(|| client.cancel_operation(&id))
                .await
                .map_err(YdbError::from)?;
            Ok(())
        })
        .await
    }

    async fn raw_client(&self) -> YdbResult<RawOperationClient> {
        self.connection_manager
            .get_auth_service(RawOperationClient::new)
            .await
    }

    #[instrument(
        name = "ydb.OperationClient.WithRpcTimeout",
        skip_all,
        err
    )]
    async fn with_rpc_timeout<T, F, Fut>(&self, operation: F) -> Result<T, RawError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, RawError>>,
    {
        match timeout(self.operation_timeout, operation()).await {
            Ok(result) => result,
            Err(_) => Err(RawError::custom(format!(
                "operation service rpc timed out after {:?}",
                self.operation_timeout
            ))),
        }
    }

     #[instrument(
        name = "ydb.OperationClient.Retry",
        skip_all,
        fields(ydb.operation.attempt = tracing::field::Empty),
        err
    )]
    async fn retry<T, F, Fut>(&self, mut attempt_fn: F) -> YdbResult<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = YdbResult<T>>,
    {
        let start = Instant::now();
        let mut attempt = 0usize;
        loop {
            attempt += 1;
            tracing::Span::current().record("ydb.operation.attempts", attempt);
            match attempt_fn().await {
                Ok(value) => return Ok(value),
                Err(err) => {
                    if !should_retry(&err) {
                        return Err(err);
                    }
                    match retry_wait(attempt, start.elapsed(), self.retry_budget) {
                        Some(wait) if wait > Duration::ZERO => sleep(wait).await,
                        Some(_) => {}
                        None => return Err(err),
                    }
                }
            }
        }
    }
}

fn should_retry(err: &YdbError) -> bool {
    match err.need_retry() {
        NeedRetry::True | NeedRetry::IdempotentOnly => true,
        NeedRetry::False => false,
    }
}

fn retry_wait(
    attempt: usize,
    time_from_start: Duration,
    retry_budget: Duration,
) -> Option<Duration> {
    if time_from_start >= retry_budget {
        return None;
    }
    let wait = if attempt > 0 {
        let exp_shift = (attempt - 1).min(63) as u32;
        let base_ms = INITIAL_RETRY_BACKOFF_MILLISECONDS
            .saturating_mul(1u64 << exp_shift)
            .min(MAX_RETRY_BACKOFF_MILLISECONDS);
        let base = Duration::from_millis(base_ms);
        let half = base / 2;
        if half.is_zero() {
            base
        } else {
            half + Duration::from_millis(rand::thread_rng().gen_range(0..=half.as_millis() as u64))
        }
    } else {
        Duration::ZERO
    };
    if time_from_start + wait < retry_budget {
        Some(wait)
    } else {
        None
    }
}

fn raw_to_operation_info(raw: RawOperation) -> OperationInfo {
    OperationInfo {
        id: raw.id,
        ready: raw.ready,
        status: raw.status,
        issues: raw.issues,
        consumed_units: raw.consumed_units,
    }
}

fn raw_to_list_result(raw: RawListOperationsResult) -> ListOperationsResult {
    ListOperationsResult {
        operations: raw
            .operations
            .into_iter()
            .map(raw_to_operation_info)
            .collect(),
        next_page_token: raw.next_page_token,
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn retry_wait_bounded_by_budget() {
        let budget = Duration::from_millis(100);
        assert!(retry_wait(1, Duration::ZERO, budget).is_some());
        assert!(retry_wait(10, budget, budget).is_none());
    }
}
