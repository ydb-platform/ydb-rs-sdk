use std::{
    collections::HashMap,
    sync::{atomic, Arc},
};
use tracing::log::trace;

use tokio::sync::{mpsc, Mutex};
use ydb_grpc::ydb_proto::coordination::{session_request, SessionRequest};

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
        self.messages_sender
            .send(SessionRequest {
                request: Some(req.into()),
            })
            .map_err(|_| YdbError::Custom("can't send".to_string()))?;

        {
            let mut active_requests = self.active_requests.lock().await;
            active_requests.insert(curr_id, tx);
        }

        Ok(rx)
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
