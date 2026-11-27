use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tracing::{error, trace, warn};

use crate::errors::{YdbError, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;

use super::SessionPoolLease;
use super::session::SessionIdentity;

#[derive(Debug)]
pub(super) enum CleanupTask {
    DeleteSession(Arc<SessionIdentity>),
    RollbackTransaction {
        lease: SessionPoolLease,
        transaction_id: String,
    },
}

#[derive(Debug)]
enum CleanupCommand {
    Run(CleanupTask),
    Shutdown { completed: oneshot::Sender<()> },
}

/// Submits server-side session resources to the cleanup worker.
#[derive(Clone)]
pub(super) struct SessionCleanup {
    sender: mpsc::UnboundedSender<CleanupCommand>,
}

impl SessionCleanup {
    pub(super) fn submit_delete(&self, identity: Arc<SessionIdentity>) {
        self.submit(CleanupTask::DeleteSession(identity));
    }

    pub(super) fn submit_rollback(&self, lease: SessionPoolLease, transaction_id: String) {
        self.submit(CleanupTask::RollbackTransaction {
            lease,
            transaction_id,
        });
    }

    fn submit(&self, task: CleanupTask) {
        if let Err(error) = self.sender.send(CleanupCommand::Run(task)) {
            error!(command = ?error.0, "session cleanup worker is stopped; cleanup was not submitted");
        }
    }

    /// Stop accepting cleanup requests and wait for every accepted task to finish.
    pub(super) async fn shutdown(&self) -> YdbResult<()> {
        let (completed, completion) = oneshot::channel();
        self.sender
            .send(CleanupCommand::Shutdown { completed })
            .map_err(|_| {
                YdbError::InternalError(
                    "session cleanup worker stopped before shutdown was submitted".to_string(),
                )
            })?;
        completion.await.map_err(|_| {
            YdbError::InternalError(
                "session cleanup worker stopped before shutdown completed".to_string(),
            )
        })
    }
}

pub(super) fn start_session_cleanup_worker(
    connection_manager: GrpcConnectionManager,
    cleanup_timeout: Duration,
) -> SessionCleanup {
    let (sender, receiver) = mpsc::unbounded_channel();
    let execute = move |task| {
        let connection_manager = connection_manager.clone();
        async move { execute_cleanup_task(connection_manager, cleanup_timeout, task).await }
    };
    tokio::spawn(run_cleanup_worker(receiver, execute));
    SessionCleanup { sender }
}

async fn run_cleanup_worker<Execute, ExecuteFuture>(
    mut receiver: mpsc::UnboundedReceiver<CleanupCommand>,
    execute: Execute,
) where
    Execute: Fn(CleanupTask) -> ExecuteFuture + Send + 'static,
    ExecuteFuture: Future<Output = ()> + Send + 'static,
{
    let mut tasks = JoinSet::new();

    let mut shutdown_completion = None;
    loop {
        // Cleanup tasks may submit follow-up work. In particular, a failed rollback drops its
        // lease, which submits DeleteSession. Session-pool shutdown has already stopped external
        // producers, so the worker is quiescent once both the task set and command queue are empty.
        if shutdown_completion.is_some() && tasks.is_empty() && receiver.is_empty() {
            break;
        }

        tokio::select! {
            command = receiver.recv() => match command {
                Some(CleanupCommand::Run(task)) => {
                    tasks.spawn(execute(task));
                }
                Some(CleanupCommand::Shutdown { completed }) => {
                    if shutdown_completion.is_some() {
                        error!("session cleanup worker received multiple shutdown commands");
                    } else {
                        shutdown_completion = Some(completed);
                    }
                }
                None => {
                    drain_cleanup_tasks(&mut tasks).await;
                    return;
                }
            },
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                report_cleanup_task_result(result);
            }
        }
    }

    let Some(completed) = shutdown_completion else {
        error!("session cleanup worker exited without a shutdown command");
        return;
    };
    if completed.send(()).is_err() {
        trace!("session cleanup shutdown caller was dropped");
    }
}

async fn drain_cleanup_tasks(tasks: &mut JoinSet<()>) {
    while let Some(result) = tasks.join_next().await {
        report_cleanup_task_result(result);
    }
}

fn report_cleanup_task_result(result: Result<(), tokio::task::JoinError>) {
    if let Err(err) = result {
        error!(error = %err, "session cleanup task failed");
    }
}

async fn execute_cleanup_task(
    connection_manager: GrpcConnectionManager,
    cleanup_timeout: Duration,
    task: CleanupTask,
) {
    match task {
        CleanupTask::DeleteSession(identity) => {
            delete_session(connection_manager, cleanup_timeout, identity).await;
        }
        CleanupTask::RollbackTransaction {
            lease,
            transaction_id,
        } => {
            rollback_transaction(connection_manager, cleanup_timeout, lease, transaction_id).await;
        }
    }
}

async fn delete_session(
    connection_manager: GrpcConnectionManager,
    cleanup_timeout: Duration,
    identity: Arc<SessionIdentity>,
) {
    let delete = async {
        let mut client = connection_manager
            .get_auth_service_to_node(RawQueryClient::new, &identity.node_uri)
            .await?;
        client
            .delete_session(&identity.session_id)
            .await
            .map_err(YdbError::from)
    };

    match tokio::time::timeout(cleanup_timeout, delete).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            warn!(session_id = %identity.session_id, error = %err, "DeleteSession cleanup failed")
        }
        Err(_) => {
            warn!(session_id = %identity.session_id, ?cleanup_timeout, "DeleteSession cleanup timed out")
        }
    }
}

async fn rollback_transaction(
    connection_manager: GrpcConnectionManager,
    cleanup_timeout: Duration,
    lease: SessionPoolLease,
    transaction_id: String,
) {
    let rollback = async {
        let mut client = connection_manager
            .get_auth_service_to_node(RawQueryClient::new, lease.node_uri())
            .await?;
        client
            .rollback_transaction(lease.session_id(), &transaction_id)
            .await
            .map_err(YdbError::from)
    };

    match tokio::time::timeout(cleanup_timeout, rollback).await {
        Ok(Ok(())) => lease.return_to_pool(),
        Ok(Err(err)) => {
            warn!(
                session_id = lease.session_id(),
                transaction_id,
                error = %err,
                "RollbackTransaction during session cleanup failed"
            );
        }
        Err(_) => {
            warn!(
                session_id = lease.session_id(),
                transaction_id,
                ?cleanup_timeout,
                "RollbackTransaction during session cleanup timed out"
            );
        }
    }
}

#[cfg(test)]
pub(super) fn start_noop_session_cleanup_worker() -> SessionCleanup {
    start_test_session_cleanup_worker(|task| async move {
        if let CleanupTask::RollbackTransaction { lease, .. } = task {
            lease.return_to_pool();
        }
    })
}

#[cfg(test)]
pub(super) fn start_test_session_cleanup_worker<Execute, ExecuteFuture>(
    execute: Execute,
) -> SessionCleanup
where
    Execute: Fn(CleanupTask) -> ExecuteFuture + Send + 'static,
    ExecuteFuture: Future<Output = ()> + Send + 'static,
{
    let (sender, receiver) = mpsc::unbounded_channel();
    tokio::spawn(run_cleanup_worker(receiver, execute));
    SessionCleanup { sender }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};
    use std::time::Duration;

    use http::Uri;
    use tokio::sync::{Semaphore, mpsc};

    use super::*;

    fn identity(session_id: &str) -> Arc<SessionIdentity> {
        Arc::new(SessionIdentity {
            session_id: session_id.to_string(),
            node_uri: Uri::from_static("http://127.0.0.1/test"),
        })
    }

    fn start_test_worker<Execute, ExecuteFuture>(
        execute: Execute,
    ) -> (SessionCleanup, tokio::task::JoinHandle<()>)
    where
        Execute: Fn(CleanupTask) -> ExecuteFuture + Send + 'static,
        ExecuteFuture: Future<Output = ()> + Send + 'static,
    {
        let (sender, receiver) = mpsc::unbounded_channel();
        let worker = tokio::spawn(run_cleanup_worker(receiver, execute));
        (SessionCleanup { sender }, worker)
    }

    #[tokio::test]
    async fn shutdown_waits_for_all_concurrent_deletes_and_stops_worker() {
        let (started_sender, mut started_receiver) = mpsc::unbounded_channel();
        let release = Arc::new(Semaphore::new(0));
        let (cleanup, worker) = start_test_worker({
            let release = release.clone();
            move |task| {
                let started_sender = started_sender.clone();
                let release = release.clone();
                async move {
                    match task {
                        CleanupTask::DeleteSession(identity) => {
                            let _ = started_sender.send(identity.session_id.clone());
                            if let Ok(permit) = release.acquire().await {
                                permit.forget();
                            }
                        }
                        CleanupTask::RollbackTransaction { lease, .. } => {
                            lease.return_to_pool();
                        }
                    }
                }
            }
        });

        for session_id in ["one", "two", "three"] {
            cleanup.submit_delete(identity(session_id));
        }

        let shutdown_cleanup = cleanup.clone();
        let shutdown = tokio::spawn(async move { shutdown_cleanup.shutdown().await });

        let mut started = Vec::new();
        for _ in 0..3 {
            started.push(
                tokio::time::timeout(Duration::from_secs(1), started_receiver.recv())
                    .await
                    .expect("all deletes must start concurrently")
                    .expect("delete observer must remain open"),
            );
        }
        started.sort();
        assert_eq!(started, ["one", "three", "two"]);
        assert!(!shutdown.is_finished());

        release.add_permits(3);
        shutdown
            .await
            .expect("shutdown task must finish")
            .expect("cleanup shutdown must succeed");
        worker.await.expect("cleanup worker must stop");
        assert!(cleanup.sender.is_closed());
        assert!(
            cleanup
                .sender
                .send(CleanupCommand::Run(CleanupTask::DeleteSession(identity(
                    "late",
                ))))
                .is_err()
        );
    }

    #[tokio::test]
    async fn closing_channel_drains_queued_deletes() {
        let completed = Arc::new(Mutex::new(Vec::new()));
        let (cleanup, worker) = start_test_worker({
            let completed = completed.clone();
            move |task| {
                let completed = completed.clone();
                async move {
                    match task {
                        CleanupTask::DeleteSession(identity) => {
                            completed
                                .lock()
                                .expect("completed lock must not be poisoned")
                                .push(identity.session_id.clone());
                        }
                        CleanupTask::RollbackTransaction { lease, .. } => {
                            lease.return_to_pool();
                        }
                    }
                }
            }
        });

        cleanup.submit_delete(identity("one"));
        cleanup.submit_delete(identity("two"));
        drop(cleanup);

        tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .expect("cleanup worker must drain and stop")
            .expect("cleanup worker task must succeed");
        let mut completed = completed
            .lock()
            .expect("completed lock must not be poisoned")
            .clone();
        completed.sort();
        assert_eq!(completed, ["one", "two"]);
    }

    #[tokio::test]
    async fn shutdown_drains_cleanup_submitted_by_running_task() {
        let completed = Arc::new(Mutex::new(Vec::new()));
        let cleanup_slot = Arc::new(OnceLock::<SessionCleanup>::new());
        let (cleanup, worker) = start_test_worker({
            let completed = completed.clone();
            let cleanup_slot = cleanup_slot.clone();
            move |task| {
                let completed = completed.clone();
                let cleanup_slot = cleanup_slot.clone();
                async move {
                    if let CleanupTask::DeleteSession(session) = task {
                        completed
                            .lock()
                            .expect("completed lock must not be poisoned")
                            .push(session.session_id.clone());
                        if session.session_id == "parent" {
                            cleanup_slot
                                .get()
                                .expect("cleanup handle must be initialized")
                                .submit_delete(identity("child"));
                        }
                    }
                }
            }
        });
        assert!(cleanup_slot.set(cleanup.clone()).is_ok());

        cleanup.submit_delete(identity("parent"));
        cleanup.shutdown().await.expect("shutdown must succeed");
        worker.await.expect("cleanup worker must stop");

        let completed = completed
            .lock()
            .expect("completed lock must not be poisoned");
        assert_eq!(*completed, ["parent", "child"]);
    }
}
