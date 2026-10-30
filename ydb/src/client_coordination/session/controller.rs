use std::{
    collections::HashMap,
    sync::{Arc, atomic},
};
use tracing::log::{debug, trace};

use tokio::sync::{Mutex, mpsc};
use ydb_grpc::ydb_proto::coordination::{SessionRequest, session_request};

use crate::{YdbError, YdbResult};

pub trait IdentifiedMessage {
    fn id(&self) -> u64;
    fn set_id(&mut self, id: u64);
}

pub struct RequestController<Response: IdentifiedMessage> {
    last_req_id: atomic::AtomicU64,
    messages_sender: mpsc::UnboundedSender<SessionRequest>,
    active_requests: Arc<Mutex<HashMap<u64, tokio::sync::mpsc::UnboundedSender<Response>>>>,
}

impl<Response: IdentifiedMessage> RequestController<Response> {
    pub fn new(messages_sender: mpsc::UnboundedSender<SessionRequest>) -> Self {
        Self {
            last_req_id: atomic::AtomicU64::new(0),
            messages_sender,
            active_requests: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn send<Request: IdentifiedMessage + Into<session_request::Request>>(
        &self,
        mut req: Request,
    ) -> YdbResult<tokio::sync::mpsc::UnboundedReceiver<Response>> {
        let curr_id = self.last_req_id.fetch_add(1, atomic::Ordering::AcqRel);

        let (tx, rx): (
            tokio::sync::mpsc::UnboundedSender<Response>,
            tokio::sync::mpsc::UnboundedReceiver<Response>,
        ) = tokio::sync::mpsc::unbounded_channel();

        req.set_id(curr_id);

        // Registered before sending: the reply is dispatched by the session's
        // receiver task, which can handle it before this call resumes.
        self.active_requests.lock().await.insert(curr_id, tx);

        if self
            .messages_sender
            .send(SessionRequest {
                request: Some(req.into()),
            })
            .is_err()
        {
            self.active_requests.lock().await.remove(&curr_id);
            return Err(YdbError::Custom("can't send".to_string()));
        }

        Ok(rx)
    }

    pub async fn get_response(&self, response: Response) -> YdbResult<()> {
        let id = response.id();
        let waiter = self.active_requests.lock().await.remove(&id);
        match waiter {
            Some(sender) => {
                // A closed channel means the caller stopped waiting, e.g. its
                // future was dropped by a timeout. That is not a session failure,
                // so it must not end the session for the other callers.
                if sender.send(response).is_err() {
                    debug!("dropped response for abandoned request id: {}", id);
                }
            }
            None => {
                trace!("got response for already unknown id: {}", id);
            }
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc_wrapper::raw_coordination_service::session::release_semaphore::{
        RawReleaseSemaphoreRequest, RawReleaseSemaphoreResult,
    };

    fn released(req_id: u64) -> RawReleaseSemaphoreResult {
        RawReleaseSemaphoreResult {
            req_id,
            released: true,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn send_registers_request_before_sending_it() {
        let (messages_tx, mut messages_rx) = mpsc::unbounded_channel();
        let controller = Arc::new(RequestController::<RawReleaseSemaphoreResult>::new(
            messages_tx,
        ));

        let held = controller.active_requests.lock().await;
        let sending = tokio::spawn({
            let controller = controller.clone();
            async move {
                controller
                    .send(RawReleaseSemaphoreRequest::new("semaphore".to_string()))
                    .await
            }
        });
        // On a current-thread runtime this runs `send` until it waits for the lock.
        tokio::task::yield_now().await;
        assert_eq!(
            messages_rx.try_recv().unwrap_err(),
            mpsc::error::TryRecvError::Empty,
            "request left before its reply channel was registered"
        );
        drop(held);

        let mut rx = sending.await.unwrap().unwrap();
        assert!(messages_rx.recv().await.is_some());
        controller.get_response(released(0)).await.unwrap();
        assert_eq!(rx.recv().await.unwrap().req_id, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn response_to_abandoned_request_keeps_session() {
        let (messages_tx, _messages_rx) = mpsc::unbounded_channel();
        let controller = RequestController::<RawReleaseSemaphoreResult>::new(messages_tx);

        let rx = controller
            .send(RawReleaseSemaphoreRequest::new("semaphore".to_string()))
            .await
            .unwrap();
        drop(rx);

        assert!(controller.get_response(released(0)).await.is_ok());
        assert!(controller.active_requests.lock().await.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_send_unregisters_request() {
        let (messages_tx, messages_rx) = mpsc::unbounded_channel();
        drop(messages_rx);
        let controller = RequestController::<RawReleaseSemaphoreResult>::new(messages_tx);

        let result = controller
            .send(RawReleaseSemaphoreRequest::new("semaphore".to_string()))
            .await;

        assert!(matches!(result, Err(YdbError::Custom(message)) if message == "can't send"));
        assert!(controller.active_requests.lock().await.is_empty());
    }
}
