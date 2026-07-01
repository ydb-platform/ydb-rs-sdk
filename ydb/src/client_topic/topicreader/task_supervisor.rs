use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::errors::NeedRetry;
use crate::{YdbError, YdbResult};

pub(super) async fn wait_child_tasks(
    cancellation: &CancellationToken,
    mut tasks: JoinSet<YdbResult<()>>,
    context: &'static str,
) -> YdbResult<()> {
    let first_joined = tasks
        .join_next()
        .await
        .ok_or_else(|| YdbError::custom(format!("{context}: task set is empty")))?;

    let was_cancelled = cancellation.is_cancelled();
    cancellation.cancel();

    let mut selected_error = task_error(first_joined);

    while let Some(joined) = tasks.join_next().await {
        select_error(&mut selected_error, joined);
    }

    if let Some(err) = selected_error {
        return Err(err);
    }

    if was_cancelled {
        Ok(())
    } else {
        Err(YdbError::custom(format!(
            "{context}: all tasks completed without error or cancellation"
        )))
    }
}

/// Fold one JoinSet completion into the running outcome state.
///
/// Policy: among multiple errors observed during task drain, prefer a fatal
/// classification over a retriable one. This keeps real non-retriable root
/// causes from being masked by sibling channel-close retry noise.
fn select_error(
    selected_error: &mut Option<YdbError>,
    joined: Result<YdbResult<()>, tokio::task::JoinError>,
) {
    let Some(err) = task_error(joined) else {
        return;
    };

    if selected_error.is_none() || !is_retriable(&err) {
        *selected_error = Some(err);
    }
}

fn task_error(joined: Result<YdbResult<()>, tokio::task::JoinError>) -> Option<YdbError> {
    match joined {
        Ok(Ok(())) => None,
        Ok(Err(err)) => Some(err),
        Err(join_err) => Some(YdbError::custom(format!(
            "topic reader task failed: {join_err}"
        ))),
    }
}

pub(super) fn is_retriable(err: &YdbError) -> bool {
    match err.need_retry() {
        NeedRetry::True | NeedRetry::IdempotentOnly => true,
        NeedRetry::False => false,
    }
}
