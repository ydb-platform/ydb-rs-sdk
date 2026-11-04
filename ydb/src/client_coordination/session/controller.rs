use std::{
    collections::HashMap,
    sync::{Arc, atomic},
};
use tracing::log::trace;

use tokio::sync::{Mutex, mpsc};
use ydb_grpc::ydb_proto::coordination::{SessionRequest, session_request};

use crate::{YdbError, YdbResult};

pub trait IdentifiedMessage {
    fn id(&self) -> u64;
    fn set_id(&mut self, id: u64);
}

pub struct RequestController<Response: IdentifiedMessage> {
    last_req_id: atomic::AtomicU64,
    closed: atomic::AtomicBool,
    messages_sender: mpsc::UnboundedSender<SessionRequest>,
    active_requests: Arc<Mutex<HashMap<u64, tokio::sync::mpsc::UnboundedSender<Response>>>>,
}

impl<Response: IdentifiedMessage> RequestController<Response> {
    pub fn new(messages_sender: mpsc::UnboundedSender<SessionRequest>) -> Self {
        Self {
            last_req_id: atomic::AtomicU64::new(0),
            closed: atomic::AtomicBool::new(false),
            messages_sender,
            active_requests: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn send<Request: IdentifiedMessage + Into<session_request::Request>>(
        &self,
        mut req: Request,
    ) -> YdbResult<tokio::sync::mpsc::UnboundedReceiver<Response>> {
        if self.closed.load(atomic::Ordering::Acquire) {
            return Err(YdbError::Custom(
                "coordination session is closed".to_string(),
            ));
        }

        let curr_id = self.last_req_id.fetch_add(1, atomic::Ordering::AcqRel);

        let (tx, rx): (
            tokio::sync::mpsc::UnboundedSender<Response>,
            tokio::sync::mpsc::UnboundedReceiver<Response>,
        ) = tokio::sync::mpsc::unbounded_channel();

        req.set_id(curr_id);
        let mut active_requests = self.active_requests.lock().await;
        if self.closed.load(atomic::Ordering::Acquire) {
            return Err(YdbError::Custom(
                "coordination session is closed".to_string(),
            ));
        }
        active_requests.insert(curr_id, tx);
        drop(active_requests);

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

    pub async fn close(&self) {
        self.closed.store(true, atomic::Ordering::Release);
        self.active_requests.lock().await.clear();
    }

    pub async fn get_response(&self, response: Response) -> YdbResult<()> {
        let waiter = self.active_requests.lock().await.remove(&response.id());
        match waiter {
            Some(sender) => {
                sender
                    .send(response)
                    .map_err(|_| YdbError::Custom("can't send".to_string()))?;
            }
            None => {
                trace!("got response for already unknown id: {}", response.id());
            }
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;
    use crate::grpc_wrapper::raw_coordination_service::session::create_semaphore::{
        RawCreateSemaphoreRequest, RawCreateSemaphoreResult,
    };

    #[tokio::test]
    async fn registers_waiter_before_dispatching_request() {
        let (messages_tx, mut messages_rx) = mpsc::unbounded_channel();
        let controller = Arc::new(RequestController::<RawCreateSemaphoreResult>::new(
            messages_tx,
        ));

        let registration_lock = controller.active_requests.lock().await;
        let send_controller = controller.clone();
        let send_task = tokio::spawn(async move {
            send_controller
                .send(RawCreateSemaphoreRequest::new(
                    "semaphore".to_string(),
                    1,
                    Vec::new(),
                ))
                .await
        });

        assert!(
            timeout(Duration::from_millis(50), messages_rx.recv())
                .await
                .is_err(),
            "request was dispatched before its response waiter was registered"
        );

        drop(registration_lock);

        let mut response_rx = send_task
            .await
            .expect("send task should not panic")
            .expect("request should be accepted");
        let message = messages_rx
            .recv()
            .await
            .expect("registered request should be dispatched");
        let req_id = match message.request {
            Some(session_request::Request::CreateSemaphore(request)) => request.req_id,
            other => panic!("unexpected request: {other:?}"),
        };

        controller
            .get_response(RawCreateSemaphoreResult { req_id })
            .await
            .expect("response should be routed to its waiter");
        assert!(response_rx.recv().await.is_some());
    }

    #[tokio::test]
    async fn closing_controller_unblocks_in_flight_request() {
        let (messages_tx, _messages_rx) = mpsc::unbounded_channel();
        let controller = RequestController::<RawCreateSemaphoreResult>::new(messages_tx);

        let mut response_rx = controller
            .send(RawCreateSemaphoreRequest::new(
                "semaphore".to_string(),
                1,
                Vec::new(),
            ))
            .await
            .expect("request should be accepted before close");

        controller.close().await;

        assert!(
            timeout(Duration::from_millis(50), response_rx.recv())
                .await
                .expect("closing the controller should wake in-flight callers")
                .is_none()
        );
        assert!(
            controller
                .send(RawCreateSemaphoreRequest::new(
                    "semaphore".to_string(),
                    1,
                    Vec::new(),
                ))
                .await
                .is_err()
        );
    }
}
