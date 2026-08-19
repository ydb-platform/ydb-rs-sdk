use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::errors::Idempotency;
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

    let mut selected_error = task_error(first_joined, context);

    while let Some(joined) = tasks.join_next().await {
        select_error(&mut selected_error, joined, context);
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
    context: &'static str,
) {
    let Some(err) = task_error(joined, context) else {
        return;
    };

    if selected_error.is_none() || !err.is_retriable(Idempotency::Idempotent) {
        *selected_error = Some(err);
    }
}

fn task_error(
    joined: Result<YdbResult<()>, tokio::task::JoinError>,
    context: &'static str,
) -> Option<YdbError> {
    match joined {
        Ok(Ok(())) => None,
        Ok(Err(err)) => Some(err),
        Err(join_err) => Some(YdbError::custom(format!(
            "{context}: task failed: {join_err}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn returns_child_error_and_cancels_siblings() {
        let cancellation = CancellationToken::new();
        let mut tasks = JoinSet::new();
        tasks.spawn(async { Err(YdbError::custom("root task error")) });

        let sibling_cancellation = cancellation.clone();
        tasks.spawn(async move {
            sibling_cancellation.cancelled().await;
            Ok(())
        });

        let err = wait_child_tasks(&cancellation, tasks, "test task set")
            .await
            .unwrap_err();

        assert!(err.to_string().contains("root task error"));
        assert!(cancellation.is_cancelled());
    }

    #[tokio::test]
    async fn external_cancellation_allows_clean_shutdown() {
        let cancellation = CancellationToken::new();
        let mut tasks = JoinSet::new();

        let task_cancellation = cancellation.clone();
        tasks.spawn(async move {
            task_cancellation.cancelled().await;
            Ok(())
        });
        cancellation.cancel();

        wait_child_tasks(&cancellation, tasks, "test task set")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn normal_completion_without_cancellation_is_an_error() {
        let cancellation = CancellationToken::new();
        let mut tasks = JoinSet::new();
        tasks.spawn(async { Ok(()) });

        let sibling_cancellation = cancellation.clone();
        tasks.spawn(async move {
            sibling_cancellation.cancelled().await;
            Ok(())
        });

        let err = wait_child_tasks(&cancellation, tasks, "test task set")
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("all tasks completed without error or cancellation")
        );
    }
}
