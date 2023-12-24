use std::sync::Arc;

use rand::RngCore;
use tokio::{
    sync::mpsc::{self, UnboundedSender},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use ydb_grpc::ydb_proto::coordination::{
    session_request::{self, SessionStart},
    session_response::SessionStarted,
    SessionRequest, SessionResponse,
};

use crate::{
    grpc_connection_manager::GrpcConnectionManager,
    grpc_wrapper::{
        self,
        grpc_stream_wrapper::AsyncGrpcStreamWrapper,
        raw_coordination_service::session::{
            acquire_semaphore::{RawAcquireSemaphoreRequest, RawAcquireSemaphoreResult},
            create_semaphore::{RawCreateSemaphoreRequest, RawCreateSemaphoreResult},
            delete_semaphore::{RawDeleteSemaphoreRequest, RawDeleteSemaphoreResult},
            describe_semaphore::{
                RawDescribeSemaphoreRequest, RawDescribeSemaphoreResult, SemaphoreDescription,
            },
            release_semaphore::RawReleaseSemaphoreResult,
            update_semaphore::{RawUpdateSemaphoreRequest, RawUpdateSemaphoreResult},
            RawSessionResponse,
        },
    },
    AcquireCount, AcquireOptions, CoordinationClient, DescribeOptions, SessionOptions, YdbError,
    YdbResult,
};

use super::{
    controller::RequestController, create_options::SemaphoreLimit, describe_options::WatchOptions,
    lease::Lease,
};

#[allow(dead_code)]
pub struct Session {
    id: u64,
    path: String,

    cancellation_token: CancellationToken,

    receiver_loop: JoinHandle<()>,

    raw_sender: mpsc::UnboundedSender<SessionRequest>,
    create_semaphore: Arc<RequestController<RawCreateSemaphoreResult>>,
    describe_semaphore: Arc<RequestController<RawDescribeSemaphoreResult>>,
    acquire_semaphore: Arc<RequestController<RawAcquireSemaphoreResult>>,
    update_semaphore: Arc<RequestController<RawUpdateSemaphoreResult>>,
    delete_semaphore: Arc<RequestController<RawDeleteSemaphoreResult>>,
    pub(crate) release_semaphore: Arc<RequestController<RawReleaseSemaphoreResult>>,
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
        println!("session started! {:?}", session_response);

        let cancellation_token = CancellationToken::new();

        let create_semaphore = Arc::new(RequestController::new(stream.clone_sender()));
        let update_semaphore = Arc::new(RequestController::new(stream.clone_sender()));
        let delete_semaphore = Arc::new(RequestController::new(stream.clone_sender()));
        let describe_semaphore = Arc::new(RequestController::new(stream.clone_sender()));
        let acquire_semaphore = Arc::new(RequestController::new(stream.clone_sender()));
        let release_semaphore = Arc::new(RequestController::new(stream.clone_sender()));

        let raw_sender = stream.clone_sender();

        let loop_token = cancellation_token.clone();
        let loop_sender = stream.clone_sender();
        let loop_create_semaphore = create_semaphore.clone();
        let loop_update_semaphore = update_semaphore.clone();
        let loop_delete_semaphore = delete_semaphore.clone();
        let loop_describe_semaphore = describe_semaphore.clone();
        let loop_acquire_semaphore = acquire_semaphore.clone();
        let loop_release_semaphore = release_semaphore.clone();

        let receiver_loop = tokio::spawn(async move {
            let mut receiver = stream;
            loop {
                match Session::receive_messages_loop_iteration(
                    &mut receiver,
                    &loop_sender,
                    &loop_create_semaphore,
                    &loop_update_semaphore,
                    &loop_delete_semaphore,
                    &loop_acquire_semaphore,
                    &loop_describe_semaphore,
                    &loop_release_semaphore,
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

            raw_sender,

            create_semaphore,
            update_semaphore,
            describe_semaphore,
            acquire_semaphore,
            delete_semaphore,
            release_semaphore,

            cancellation_token,
            protection_key,

            connection_manager,
        })
    }

    pub fn alive(&self) -> CancellationToken {
        self.cancellation_token.child_token()
    }

    pub async fn create_semaphore(
        &self,
        name: String,
        limit: SemaphoreLimit,
        data: Option<Vec<u8>>,
    ) -> YdbResult<()> {
        let request = RawCreateSemaphoreRequest::new(name, limit, data.unwrap_or_default());
        warn!("sening request: {:?}", request);

        let mut rx = self.create_semaphore.send(request).await?;

        warn!("awaiting response");
        match rx.recv().await {
            Some(_) => Ok(()),
            None => Err(YdbError::Custom("channel closed".to_string())),
        }
    }

    pub async fn describe_semaphore(
        &self,
        name: String,
        options: DescribeOptions,
    ) -> YdbResult<SemaphoreDescription> {
        let mut rx = self
            .describe_semaphore
            .send(RawDescribeSemaphoreRequest::new(
                name,
                options.with_owners,
                options.with_waiters,
                None,
            ))
            .await?;
        let result = rx.recv().await.unwrap();
        Ok(result.semaphore_description)
    }

    pub async fn watch_semaphore(
        &self,
        _name: String,
        _options: WatchOptions,
    ) -> YdbResult<mpsc::Receiver<SemaphoreDescription>> {
        unimplemented!()
    }

    pub async fn update_semaphore(&self, name: String, data: Option<Vec<u8>>) -> YdbResult<()> {
        let mut rx = self
            .update_semaphore
            .send(RawUpdateSemaphoreRequest::new(name, data))
            .await?;
        rx.recv().await.unwrap();
        Ok(())
    }

    pub async fn delete_semaphore(&self, name: String) -> YdbResult<()> {
        let mut rx = self
            .delete_semaphore
            .send(RawDeleteSemaphoreRequest::new(name, false))
            .await?;
        rx.recv().await.unwrap();
        Ok(())
    }

    pub async fn force_delete_semaphore(&self, name: String) -> YdbResult<()> {
        let mut rx = self
            .delete_semaphore
            .send(RawDeleteSemaphoreRequest::new(name, true))
            .await?;
        rx.recv().await.unwrap();
        Ok(())
    }

    pub async fn acquire_semaphore(
        &self,
        name: String,
        count: AcquireCount,
        options: AcquireOptions,
    ) -> YdbResult<Lease> {
        let mut rx = self
            .acquire_semaphore
            .send(RawAcquireSemaphoreRequest::new(
                name.clone(),
                count,
                options.timeout,
                options.ephemeral,
                options.data,
            ))
            .await?;
        let response = rx.recv().await.unwrap();
        if response.acquired {
            Ok(Lease::new(
                self.release_semaphore.clone(),
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
        raw_sender: &UnboundedSender<SessionRequest>,
        create_semaphore: &Arc<RequestController<RawCreateSemaphoreResult>>,
        update_semaphore: &Arc<RequestController<RawUpdateSemaphoreResult>>,
        delete_semaphore: &Arc<RequestController<RawDeleteSemaphoreResult>>,
        acquire_semaphore: &Arc<RequestController<RawAcquireSemaphoreResult>>,
        describe_semaphore: &Arc<RequestController<RawDescribeSemaphoreResult>>,
        release_semaphore: &Arc<RequestController<RawReleaseSemaphoreResult>>,
    ) -> YdbResult<()> {
        let response = server_messages_receiver
            .receive::<RawSessionResponse>()
            .await;

        println!("received response: {:?}", response);

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
                    create_semaphore.get_response(semaphore_created).await?;
                }
                RawSessionResponse::UpdateSemaphoreResult(semaphore_updated) => {
                    update_semaphore.get_response(semaphore_updated).await?;
                }
                RawSessionResponse::DescribeSemaphoreResult(semaphore_description) => {
                    describe_semaphore
                        .get_response(semaphore_description)
                        .await?;
                }
                RawSessionResponse::DeleteSemaphoreResult(semaphore_deleted) => {
                    delete_semaphore.get_response(semaphore_deleted).await?;
                }
                RawSessionResponse::AcquireSemaphoreResult(semaphore_acquired) => {
                    acquire_semaphore.get_response(semaphore_acquired).await?;
                }
                RawSessionResponse::AcquireSemaphorePending(_) => {
                    // TODO: send to the same conversation
                }
                RawSessionResponse::ReleaseSemaphoreResult(semaphore_released) => {
                    release_semaphore.get_response(semaphore_released).await?;
                }
                _ => todo!(),
            },
            Err(some_err) => {
                return Err(YdbError::from(some_err));
            }
        }
        Ok(())
    }
}
