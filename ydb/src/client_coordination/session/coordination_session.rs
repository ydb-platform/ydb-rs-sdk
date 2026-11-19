use std::sync::Arc;

use rand::RngCore;
use tokio::{
    sync::mpsc::{self, UnboundedSender},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::trace;
use ydb_grpc::ydb_proto::coordination::{
    SessionRequest, SessionResponse,
    session_request::{self, SessionStart},
    session_response::SessionStarted,
};

use crate::{
    AcquireOptions, AcquireOptionsBuilder, DescribeOptions, DescribeOptionsBuilder, SessionOptions,
    YdbError, YdbResult,
    grpc_connection_manager::GrpcConnectionManager,
    grpc_wrapper::{
        self,
        grpc_stream_wrapper::AsyncGrpcStreamWrapper,
        raw_coordination_service::session::{
            RawSessionResponse,
            acquire_semaphore::{RawAcquireSemaphoreRequest, RawAcquireSemaphoreResult},
            create_semaphore::{RawCreateSemaphoreRequest, RawCreateSemaphoreResult},
            delete_semaphore::{RawDeleteSemaphoreRequest, RawDeleteSemaphoreResult},
            describe_semaphore::{
                RawDescribeSemaphoreRequest, RawDescribeSemaphoreResult, SemaphoreDescription,
            },
            release_semaphore::RawReleaseSemaphoreResult,
            update_semaphore::{RawUpdateSemaphoreRequest, RawUpdateSemaphoreResult},
        },
    },
};

use super::{controller::RequestController, lease::Lease};

#[derive(Clone)]
struct MethodControllers {
    create_semaphore: Arc<RequestController<RawCreateSemaphoreResult>>,
    describe_semaphore: Arc<RequestController<RawDescribeSemaphoreResult>>,
    acquire_semaphore: Arc<RequestController<RawAcquireSemaphoreResult>>,
    update_semaphore: Arc<RequestController<RawUpdateSemaphoreResult>>,
    delete_semaphore: Arc<RequestController<RawDeleteSemaphoreResult>>,
    release_semaphore: Arc<RequestController<RawReleaseSemaphoreResult>>,
}

#[allow(dead_code)]
pub struct CoordinationSession {
    id: u64,
    path: String,

    cancellation_token: CancellationToken,
    receiver_loop: JoinHandle<()>,

    raw_sender: mpsc::UnboundedSender<SessionRequest>,
    method_controllers: MethodControllers,

    protection_key: Vec<u8>,

    connection_manager: GrpcConnectionManager,
}

#[allow(dead_code)]
impl CoordinationSession {
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

        let mut protection_key = vec![0; 16];
        rand::thread_rng().fill_bytes(&mut protection_key);

        let session_start_request = SessionStart {
            path: path.clone(),
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
        trace!("session started! {:?}", session_response);

        let cancellation_token = CancellationToken::new();

        let method_controllers = MethodControllers {
            create_semaphore: Arc::new(RequestController::new(stream.clone_sender())),
            update_semaphore: Arc::new(RequestController::new(stream.clone_sender())),
            delete_semaphore: Arc::new(RequestController::new(stream.clone_sender())),
            describe_semaphore: Arc::new(RequestController::new(stream.clone_sender())),
            acquire_semaphore: Arc::new(RequestController::new(stream.clone_sender())),
            release_semaphore: Arc::new(RequestController::new(stream.clone_sender())),
        };

        let raw_sender = stream.clone_sender();

        let loop_token = cancellation_token.clone();
        let loop_sender = stream.clone_sender();
        let loop_controllers = method_controllers.clone();

        let receiver_loop = tokio::spawn(async move {
            let mut receiver = stream;
            loop {
                match CoordinationSession::receive_messages_loop_iteration(
                    &mut receiver,
                    &loop_sender,
                    &loop_controllers,
                )
                .await
                {
                    Ok(()) => {}
                    Err(_iteration_error) => {
                        loop_token.cancel();
                        return;
                    }
                };
            }
        });

        Ok(Self {
            id: session_response.session_id,
            path,

            receiver_loop,
            cancellation_token,

            raw_sender,
            method_controllers,

            protection_key,

            connection_manager,
        })
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn alive(&self) -> CancellationToken {
        self.cancellation_token.child_token()
    }

    /// Awaits a method's reply, giving up if the session is lost.
    ///
    /// The receiver loop reports a lost session by cancelling the session's
    /// token and returning. That leaves nothing to answer a request already in
    /// flight: the loop holds a clone of the controllers, so the senders
    /// outlive it and the response channel is never closed. Without this race
    /// the caller waits for a reply that cannot arrive, with no deadline.
    ///
    /// Biased towards the reply, so one that lands in the same moment the
    /// session ends is still delivered rather than discarded.
    async fn recv_while_alive<Response>(
        &self,
        rx: &mut mpsc::UnboundedReceiver<Response>,
    ) -> YdbResult<Response> {
        tokio::select! {
            biased;
            response = rx.recv() => {
                response.ok_or_else(|| YdbError::Custom("channel closed".to_string()))
            }
            () = self.cancellation_token.cancelled() => {
                Err(YdbError::Custom("coordination session lost".to_string()))
            }
        }
    }

    pub async fn create_semaphore<S: ToString>(
        &self,
        name: S,
        limit: u64,
        data: Vec<u8>,
    ) -> YdbResult<()> {
        let request = RawCreateSemaphoreRequest::new(name.to_string(), limit, data);

        let mut rx = self
            .method_controllers
            .create_semaphore
            .send(request)
            .await?;

        self.recv_while_alive(&mut rx).await?;
        Ok(())
    }

    pub async fn describe_semaphore<S: ToString>(
        &self,
        name: S,
    ) -> YdbResult<SemaphoreDescription> {
        let options = DescribeOptionsBuilder::default()
            .with_owners(true)
            .with_waiters(true)
            .build()
            .unwrap();

        self.describe_semaphore_with_params(name, options).await
    }

    pub async fn describe_semaphore_with_params<S: ToString>(
        &self,
        name: S,
        options: DescribeOptions,
    ) -> YdbResult<SemaphoreDescription> {
        let mut rx = self
            .method_controllers
            .describe_semaphore
            .send(RawDescribeSemaphoreRequest::new(
                name.to_string(),
                options.with_owners,
                options.with_waiters,
                None,
            ))
            .await?;

        let result = self.recv_while_alive(&mut rx).await?;
        Ok(result.semaphore_description)
    }

    pub async fn update_semaphore<S: ToString>(&self, name: S, data: Vec<u8>) -> YdbResult<()> {
        let mut rx = self
            .method_controllers
            .update_semaphore
            .send(RawUpdateSemaphoreRequest::new(name.to_string(), data))
            .await?;

        self.recv_while_alive(&mut rx).await?;
        Ok(())
    }

    pub async fn delete_semaphore<S: ToString>(&self, name: S) -> YdbResult<()> {
        let mut rx = self
            .method_controllers
            .delete_semaphore
            .send(RawDeleteSemaphoreRequest::new(name.to_string(), false))
            .await?;

        self.recv_while_alive(&mut rx).await?;
        Ok(())
    }

    pub async fn force_delete_semaphore<S: ToString>(&self, name: S) -> YdbResult<()> {
        let mut rx = self
            .method_controllers
            .delete_semaphore
            .send(RawDeleteSemaphoreRequest::new(name.to_string(), true))
            .await?;

        self.recv_while_alive(&mut rx).await?;
        Ok(())
    }

    pub async fn acquire_semaphore<S: ToString>(&self, name: S, count: u64) -> YdbResult<Lease> {
        let options = AcquireOptionsBuilder::default().build()?;
        self.acquire_semaphore_with_params(name, count, options)
            .await
    }

    pub async fn acquire_semaphore_with_params<S: ToString>(
        &self,
        name: S,
        count: u64,
        options: AcquireOptions,
    ) -> YdbResult<Lease> {
        let mut rx = self
            .method_controllers
            .acquire_semaphore
            .send(RawAcquireSemaphoreRequest::new(
                name.to_string(),
                count,
                options.timeout,
                options.ephemeral,
                options.data,
            ))
            .await?;
        let response = self.recv_while_alive(&mut rx).await?;
        if response.acquired {
            Ok(Lease::new(
                self.method_controllers.release_semaphore.clone(),
                name.to_string(),
                self.cancellation_token.child_token(),
            ))
        } else {
            Err(YdbError::Custom("failed to acquire semaphore".to_string()))
        }
    }

    async fn receive_messages_loop_iteration(
        server_messages_receiver: &mut AsyncGrpcStreamWrapper<SessionRequest, SessionResponse>,
        raw_sender: &UnboundedSender<SessionRequest>,
        method_controllers: &MethodControllers,
    ) -> YdbResult<()> {
        let response = server_messages_receiver
            .receive::<RawSessionResponse>()
            .await;

        trace!("received response: {:?}", response);

        match response {
            Ok(message) => match message {
                RawSessionResponse::SessionStarted(_started_response_body) => {
                    return Err(YdbError::Custom(
                        "Unexpected message type in stream reader: init_response".to_string(),
                    ));
                }
                RawSessionResponse::Ping(ping_request) => {
                    let pong = session_request::PingPong {
                        opaque: ping_request.opaque,
                    };
                    raw_sender
                        .send(SessionRequest {
                            request: Some(session_request::Request::Pong(pong)),
                        })
                        .map_err(|_| YdbError::Custom("can't send".to_string()))?;
                }
                RawSessionResponse::Pong(_pong_response) => {
                    // noop
                }
                RawSessionResponse::CreateSemaphoreResult(semaphore_created) => {
                    method_controllers
                        .create_semaphore
                        .get_response(semaphore_created)
                        .await?;
                }
                RawSessionResponse::UpdateSemaphoreResult(semaphore_updated) => {
                    method_controllers
                        .update_semaphore
                        .get_response(semaphore_updated)
                        .await?;
                }
                RawSessionResponse::DescribeSemaphoreResult(semaphore_description) => {
                    method_controllers
                        .describe_semaphore
                        .get_response(semaphore_description)
                        .await?;
                }
                RawSessionResponse::DeleteSemaphoreResult(semaphore_deleted) => {
                    method_controllers
                        .delete_semaphore
                        .get_response(semaphore_deleted)
                        .await?;
                }
                RawSessionResponse::AcquireSemaphoreResult(semaphore_acquired) => {
                    method_controllers
                        .acquire_semaphore
                        .get_response(semaphore_acquired)
                        .await?;
                }
                RawSessionResponse::AcquireSemaphorePending(_) => {
                    // TODO: send to the same conversation
                }
                RawSessionResponse::ReleaseSemaphoreResult(semaphore_released) => {
                    method_controllers
                        .release_semaphore
                        .get_response(semaphore_released)
                        .await?;
                }

                _ => return Err(YdbError::custom("unimplemented")),
            },
            Err(some_err) => {
                return Err(YdbError::from(some_err));
            }
        }
        Ok(())
    }
}
