use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::task::JoinHandle;

use crate::grpc_wrapper::raw_errors::{RawError, RawResult};
use crate::grpc_wrapper::raw_query_service::client::RawQueryClient;
use crate::grpc_wrapper::raw_query_service::status::check_status;
use ydb_grpc::ydb_proto::query::SessionState;

/// Explicit Query Service session: CreateSession + AttachSession stream kept alive.
pub(crate) struct AttachedQuerySession {
    session_id: String,
    attach_task: JoinHandle<()>,
    attach_alive: Arc<AtomicBool>,
}

impl Drop for AttachedQuerySession {
    fn drop(&mut self) {
        self.attach_task.abort();
        tracing::warn!(
            session_id = %self.session_id,
            "query session dropped without explicit close; server-side session may leak until idle timeout"
        );
    }
}

impl AttachedQuerySession {
    pub async fn open(client: &mut RawQueryClient) -> RawResult<Self> {
        let session_id = client.create_session().await?;
        let mut attach_stream = client.attach_session(&session_id).await?;
        let first = attach_stream
            .message()
            .await?
            .ok_or_else(|| RawError::custom("attach session stream closed"))?;
        check_attach_state(&first)?;

        let attach_alive = Arc::new(AtomicBool::new(true));
        let alive_flag = attach_alive.clone();
        let attach_task = tokio::spawn(async move {
            while let Ok(Some(state)) = attach_stream.message().await {
                if check_attach_state(&state).is_err() {
                    break;
                }
            }
            alive_flag.store(false, Ordering::Release);
        });

        Ok(Self {
            session_id,
            attach_task,
            attach_alive,
        })
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn ensure_alive(&self) -> RawResult<()> {
        if self.attach_alive.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(RawError::custom(
                "query attach session stream closed; create a new transaction",
            ))
        }
    }

    pub async fn close(self, client: &mut RawQueryClient) {
        self.attach_task.abort();
        let _ = client.delete_session(&self.session_id).await;
    }
}

fn check_attach_state(state: &SessionState) -> RawResult<()> {
    check_status(state.status, &state.issues)
}
