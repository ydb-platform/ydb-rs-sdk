use std::{
    borrow::BorrowMut,
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use rand::RngCore;
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinHandle,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use tracing::log::trace;
use ydb_grpc::ydb_proto::coordination::{
    session_request::{
        self, AcquireSemaphore, CreateSemaphore, DescribeSemaphore, PingPong, SessionStart,
    },
    session_response::{CreateSemaphoreResult, DescribeSemaphoreResult, SessionStarted},
    SessionRequest, SessionResponse,
};

use crate::{
    client_coordination::list_types::SemaphoreDescription,
    grpc_connection_manager::GrpcConnectionManager,
    grpc_wrapper::{
        self,
        grpc_stream_wrapper::AsyncGrpcStreamWrapper,
        raw_coordination_service::{
            describe_node::RawDescribeNodeRequest,
            session::{
                acquire_semaphore::{RawAcquireSemaphoreRequest, RawAcquireSemaphoreResult},
                create_semaphore::{RawCreateSemaphoreRequest, RawCreateSemaphoreResult, self},
                delete_semaphore::{RawDeleteSemaphoreRequest, RawDeleteSemaphoreResult},
                describe_semaphore::{RawDescribeSemaphoreRequest, RawDescribeSemaphoreResult},
                release_semaphore::RawReleaseSemaphoreResult,
                update_semaphore::{RawUpdateSemaphoreRequest, RawUpdateSemaphoreResult},
                RawSessionResponse,
            },
        },
    },
    AcquireCount, AcquireOptions, CoordinationClient, DescribeOptions, SessionOptions, YdbError,
    YdbResult,
};

use super::{create_options::SemaphoreLimit, describe_options::WatchOptions, lease::Lease};

pub trait IdentifiedMessage {
    fn id(&self) -> u64;
    fn set_id(&mut self, id: u64);
}

pub struct RequestController<Response: IdentifiedMessage> {
    last_req_id: u64,
    messages_sender: mpsc::UnboundedSender<SessionRequest>,
    active_requests: Arc<Mutex<HashMap<u64, tokio::sync::oneshot::Sender<Response>>>>,
}

impl<Response: IdentifiedMessage> RequestController<Response> {
    pub fn new(messages_sender: mpsc::UnboundedSender<SessionRequest>) -> Self {
        Self {
            last_req_id: 0,
            messages_sender,
            active_requests: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn send<Request: IdentifiedMessage + Into<session_request::Request>>(
        &mut self,
        mut req: Request,
    ) -> YdbResult<tokio::sync::oneshot::Receiver<Response>> {
        self.last_req_id += 1;
        req.set_id(self.last_req_id);

        let (tx, rx): (
            tokio::sync::oneshot::Sender<Response>,
            tokio::sync::oneshot::Receiver<Response>,
        ) = tokio::sync::oneshot::channel();

        self.messages_sender.send(SessionRequest {
            request: Some(req.into()),
        });

        {
            let mut active_requests = self.active_requests.lock().await;
            active_requests.insert(self.last_req_id, tx);
        }

        Ok(rx)
    }

    pub async fn get_response(&mut self, response: Response) {
        let waiter = self.active_requests.lock().await.remove(&response.id());
        match waiter {
            Some(sender) => {
                sender.send(response);
            }
            None => {
                trace!("got response for already forgotten id: {}", response.id());
            }
        }
    }
}

#[allow(dead_code)]
pub struct Session {
    id: u64,
    path: String,

    cancellation_token: CancellationToken,

    receiver_loop: JoinHandle<()>,

    raw_sender: mpsc::UnboundedSender<SessionRequest>,
    create_semaphore: RequestController<RawCreateSemaphoreResult>,
    describe_semaphore: RequestController<RawDescribeSemaphoreResult>,
    acquire_semaphore: RequestController<RawAcquireSemaphoreResult>,
    update_semaphore: RequestController<RawUpdateSemaphoreResult>,
    delete_semaphore: RequestController<RawDeleteSemaphoreResult>,
    pub release_semaphore: RequestController<RawReleaseSemaphoreResult>,

    protection_key: Vec<u8>,

    connection_manager: GrpcConnectionManager,
}

#[allow(dead_code)]
impl Session {
    pub(crate) async fn new(
        path: String,
        seq_no: u64,
        options: SessionOptions,
        connection_manager: GrpcConnectionManager,
    ) -> YdbResult<Self> {
        let mut coordination_service = connection_manager
            .get_auth_service(
                grpc_wrapper::raw_coordination_service::client::RawCoordinationClient::new,
            )
            .await?;

        let protection_key = vec![0; 16];
        rand::thread_rng().fill_bytes(&mut protection_key);

        let session_start_request = SessionStart {
            path,
            seq_no,
            session_id: 0,
            timeout_millis: options.timeout.as_millis() as u64,
            description: options.description.unwrap_or_default(),
            protection_key: protection_key.clone(),
        };

        let mut stream = coordination_service.session(session_start_request).await?;
        let start_response = stream.receive::<RawSessionResponse>().await?;

        let session_response: SessionStarted;
        if let RawSessionResponse::SessionStarted(response) = start_response {
            session_response = response;
        } else {
            return Err(YdbError::Custom("unexpected session answer".to_string()));
        }

        let create_semaphore = RequestController::new(stream.clone_sender());


        let receiver_loop = tokio::spawn(async move {
            let mut message_receiver = 
            loop {}
        });

        Ok(Self {
            id: session_response.session_id,
            path,

            cancellation_token: CancellationToken::new(),
            connection_manager,
        })
    }

    pub fn alive(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    pub async fn create_semaphore(
        &mut self,
        name: String,
        limit: SemaphoreLimit,
        data: Option<Vec<u8>>,
    ) -> YdbResult<()> {
        let rx = self
            .create_semaphore
            .send(RawCreateSemaphoreRequest::new(
                name,
                limit,
                data.unwrap_or_default(),
            ))
            .await?;
        rx.await?;
        Ok(())
    }

    pub async fn describe_semaphore(
        &mut self,
        name: String,
        options: DescribeOptions,
    ) -> YdbResult<SemaphoreDescription> {
        let rx = self
            .describe_semaphore
            .send(RawDescribeSemaphoreRequest::new(
                name,
                options.with_owners,
                options.with_waiters,
                None,
            ))
            .await?;
        let result = rx.await?;
        Ok(result.semaphore_description)
    }

    pub async fn watch_semaphore(
        &mut self,
        _name: String,
        _options: WatchOptions,
    ) -> YdbResult<mpsc::Receiver<SemaphoreDescription>> {
        unimplemented!()
    }

    pub async fn update_semaphore(&mut self, name: String, data: Option<Vec<u8>>) -> YdbResult<()> {
        let rx = self
            .update_semaphore
            .send(RawUpdateSemaphoreRequest::new(name, data))
            .await?;
        rx.await?;
        Ok(())
    }

    pub async fn delete_semaphore(&mut self, name: String) -> YdbResult<()> {
        let rx = self
            .delete_semaphore
            .send(RawDeleteSemaphoreRequest::new(name, false))
            .await?;
        rx.await?;
        Ok(())
    }

    pub async fn force_delete_semaphore(&mut self, name: String) -> YdbResult<()> {
        let rx = self
            .delete_semaphore
            .send(RawDeleteSemaphoreRequest::new(name, true))
            .await?;
        rx.await?;
        Ok(())
    }

    pub async fn acquire_semaphore(
        &mut self,
        name: String,
        count: AcquireCount,
        options: AcquireOptions,
    ) -> YdbResult<Lease> {
        let rx = self
            .acquire_semaphore
            .send(RawAcquireSemaphoreRequest::new(
                name.clone(),
                count,
                options.timeout,
                options.ephemeral,
                options.data,
            ))
            .await?;
        let response = rx.await?;
        if response.acquired {
            Ok(Lease::new(
                self,
                name,
                self.cancellation_token.child_token(),
            ))
        } else {
            Err(YdbError::Custom("failed to acquire semaphore".to_string()))
        }
    }

    pub fn client(&self) -> CoordinationClient {
        unimplemented!()
    }

    async fn receive_messages_loop_iteration(
        server_messages_receiver: &mut AsyncGrpcStreamWrapper<SessionRequest, SessionResponse>,
        create_semaphore: &mut mpsc::Sender<RawCreateSemaphoreResult>,
        describe_semaphore: &mut mpsc::Sender<RawDescribeSemaphoreResult>,
        acquire_semaphore: &mut mpsc::Sender<RawAcquireSemaphoreResult>,
    ) -> YdbResult<()> {
        match server_messages_receiver
            .receive::<RawSessionResponse>()
            .await
        {
            Ok(message) => match message {
                RawSessionResponse::SessionStarted(_started_response_body) => {
                    return Err(YdbError::Custom(
                        "Unexpected message type in stream reader: init_response".to_string(),
                    ));
                }
                RawSessionResponse::Ping(ping_request) => {
                    // TODO: send pong
                }
                RawSessionResponse::Pong(pong_response) => {
                    // noop
                }
                RawSessionResponse::CreateSemaphoreResult(semaphore_created) => {
                    create_semaphore.send(semaphore_created).await;
                }
                RawSessionResponse::DescribeSemaphoreResult(semaphore_description) => {
                    describe_semaphore.send(semaphore_description).await;
                }

                RawServerMessage::Write(write_response_body) => {
                    for raw_ack in write_response_body.acks {
                        let write_ack = WriteAck::from(raw_ack);
                        let mut reception_queue = confirmation_reception_queue.lock().unwrap();
                        let reception_ticket = reception_queue.try_get_ticket();
                        match reception_ticket {
                            None => {
                                return Err(YdbError::Custom(
                                    "Expected reception ticket to be actually present".to_string(),
                                ));
                            }
                            Some(ticket) => {
                                if write_ack.seq_no != ticket.get_seq_no() {
                                    return Err(YdbError::custom(format!(
                                        "Reception ticket and write ack seq_no mismatch. Seqno from ack: {}, expected: {}",
                                        write_ack.seq_no, ticket.get_seq_no()
                                    )));
                                }
                                ticket.send_confirmation_if_needed(write_ack.status);
                            }
                        }
                    }
                }
                RawServerMessage::UpdateToken(_update_token_response_body) => {}
            },
            Err(some_err) => {
                return Err(YdbError::from(some_err));
            }
        }
        Ok(())
    }
}
