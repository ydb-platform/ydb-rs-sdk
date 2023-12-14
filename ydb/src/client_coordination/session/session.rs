use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use ydb_grpc::ydb_proto::coordination::session_request::SessionStart;

use crate::{
    client_coordination::list_types::SemaphoreDescription,
    grpc_connection_manager::GrpcConnectionManager,
    grpc_wrapper::{self, raw_coordination_service::session::RawSessionResponse},
    AcquireCount, AcquireOptions, CoordinationClient, DescribeOptions, YdbResult,
};

use super::{create_options::SemaphoreLimit, describe_options::WatchOptions, lease::Lease};

#[allow(dead_code)]
pub struct Session {
    cancellation_token: CancellationToken,
    connection_manager: GrpcConnectionManager,
}

#[allow(dead_code)]
impl Session {
    pub(crate) async fn new(connection_manager: GrpcConnectionManager) -> YdbResult<Self> {
        let mut coordination_service = connection_manager
            .get_auth_service(
                grpc_wrapper::raw_coordination_service::client::RawCoordinationClient::new,
            )
            .await?;

        let session_start_request = SessionStart {
            path: "TODO".to_string(),
            session_id: 0,
            timeout_millis: 100,
            description: "TODO".to_string(),
            seq_no: 1,
            protection_key: vec![0, 1, 2, 3],
        };

        let mut stream = coordination_service.session(session_start_request).await?;
        let start_response = stream.receive::<RawSessionResponse>().await?;
        let start_response = RawSessionResponse::Started(response);

        start_response.Ok(Self {
            cancellation_token: CancellationToken::new(),
            connection_manager,
        })
    }

    pub fn alive(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    pub async fn create_semaphore(
        &mut self,
        _name: String,
        _limit: SemaphoreLimit,
        _data: Option<Vec<u8>>,
    ) -> YdbResult<()> {
        unimplemented!()
    }

    pub async fn describe_semaphore(
        &mut self,
        _name: String,
        _options: DescribeOptions,
    ) -> YdbResult<SemaphoreDescription> {
        unimplemented!()
    }

    pub async fn watch_semaphore(
        &mut self,
        _name: String,
        _options: WatchOptions,
    ) -> YdbResult<mpsc::Receiver<SemaphoreDescription>> {
        unimplemented!()
    }

    pub async fn update_semaphore(
        &mut self,
        _name: String,
        _data: Option<Vec<u8>>,
    ) -> YdbResult<()> {
        unimplemented!()
    }

    pub async fn delete_semaphore(&mut self, _name: String) -> YdbResult<()> {
        unimplemented!()
    }

    pub async fn force_delete_semaphore(&mut self, _name: String) -> YdbResult<()> {
        unimplemented!()
    }

    pub async fn acquire_semaphore(
        &self,
        _name: String,
        _count: AcquireCount,
        _options: AcquireOptions,
    ) -> YdbResult<Lease> {
        unimplemented!()
    }

    pub fn client(&self) -> CoordinationClient {
        unimplemented!()
    }
}
