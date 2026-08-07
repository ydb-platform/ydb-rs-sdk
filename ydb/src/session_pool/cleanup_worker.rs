use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{error, warn};

use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;

use super::session::SessionIdentity;

enum CleanupCommand {
    DeleteSession(Arc<SessionIdentity>),
}

/// Submits server-side session resources to the cleanup worker.
#[derive(Clone)]
pub(super) struct SessionCleanup {
    sender: mpsc::UnboundedSender<CleanupCommand>,
}

impl SessionCleanup {
    pub(super) fn submit_delete(&self, identity: Arc<SessionIdentity>) {
        if self
            .sender
            .send(CleanupCommand::DeleteSession(identity.clone()))
            .is_err()
        {
            error!(
                session_id = %identity.session_id,
                node_uri = %identity.node_uri,
                "session cleanup worker is stopped; DeleteSession was not submitted"
            );
        }
    }
}

pub(super) fn start_session_cleanup_worker(
    connection_manager: GrpcConnectionManager,
    delete_timeout: Duration,
) -> SessionCleanup {
    let (sender, receiver) = mpsc::unbounded_channel();
    let delete = move |identity| {
        let connection_manager = connection_manager.clone();
        async move { delete_session(connection_manager, delete_timeout, identity).await }
    };
    tokio::spawn(run_cleanup_worker(receiver, delete));
    SessionCleanup { sender }
}

async fn run_cleanup_worker<Delete, DeleteFuture>(
    mut receiver: mpsc::UnboundedReceiver<CleanupCommand>,
    delete: Delete,
) where
    Delete: Fn(Arc<SessionIdentity>) -> DeleteFuture,
    DeleteFuture: Future<Output = ()> + Send + 'static,
{
    let mut tasks = JoinSet::new();

    loop {
        tokio::select! {
            command = receiver.recv() => match command {
                Some(CleanupCommand::DeleteSession(identity)) => {
                    tasks.spawn(delete(identity));
                }
                None => {
                    drain_delete_tasks(&mut tasks).await;
                    return;
                }
            },
            result = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(result) = result {
                    report_delete_task_result(result);
                }
            }
        }
    }
}

async fn drain_delete_tasks(tasks: &mut JoinSet<()>) {
    while let Some(result) = tasks.join_next().await {
        report_delete_task_result(result);
    }
}

fn report_delete_task_result(result: Result<(), tokio::task::JoinError>) {
    if let Err(err) = result {
        error!(error = %err, "session cleanup task failed");
    }
}

async fn delete_session(
    connection_manager: GrpcConnectionManager,
    delete_timeout: Duration,
    identity: Arc<SessionIdentity>,
) {
    let mut client = match connection_manager
        .get_auth_service_to_node(RawQueryClient::new, &identity.node_uri)
        .await
    {
        Ok(client) => client,
        Err(err) => {
            warn!(session_id = %identity.session_id, error = %err, "failed to connect for DeleteSession");
            return;
        }
    };

    match tokio::time::timeout(delete_timeout, client.delete_session(&identity.session_id)).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            warn!(session_id = %identity.session_id, error = %err, "DeleteSession failed")
        }
        Err(_) => {
            warn!(session_id = %identity.session_id, ?delete_timeout, "DeleteSession timed out")
        }
    }
}

#[cfg(test)]
pub(super) fn start_noop_session_cleanup_worker() -> SessionCleanup {
    let (sender, receiver) = mpsc::unbounded_channel();
    tokio::spawn(run_cleanup_worker(receiver, |_| async {}));
    SessionCleanup { sender }
}
