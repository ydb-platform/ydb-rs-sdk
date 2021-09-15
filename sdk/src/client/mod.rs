mod internal;
mod trait_operation;

use crate::client::internal::AuthService;
use crate::credentials::Credencials;
use crate::errors::Result;
use tonic::transport::{Channel, ClientTlsConfig};
use tower::ServiceBuilder;
use ydb_protobuf::generated::ydb::discovery::v1::discovery_service_client::DiscoveryServiceClient;
use ydb_protobuf::generated::ydb::discovery::{
    ListEndpointsRequest, ListEndpointsResult, WhoAmIRequest, WhoAmIResult,
};

pub struct Client {
    start_endpoint: String,
    cred: Box<dyn Credencials>,
    database: String,

    // state
    channel: Option<Channel>,
}

impl Client {
    pub fn new(endpoint: &str, cred: Box<dyn Credencials>, database: &str) -> Result<Self> {
        return Ok(Client {
            start_endpoint: endpoint.to_string(),
            cred,
            database: database.to_string(),

            channel: None,
        });
    }

    pub async fn endpoints(
        self: &mut Self,
        mut req: ListEndpointsRequest,
    ) -> Result<ListEndpointsResult> {
        if req.database.is_empty() {
            req.database = self.database.clone();
        }
        internal::grpc_read_result(self.client_discovery()?.list_endpoints(req).await?)
    }

    pub async fn who_am_i(self: &mut Self, req: WhoAmIRequest) -> Result<WhoAmIResult> {
        internal::grpc_read_result(self.client_discovery()?.who_am_i(req).await?)
    }

    fn client_discovery(self: &mut Self) -> Result<DiscoveryServiceClient<AuthService>> {
        return self.create_grpc_client(DiscoveryServiceClient::new);
    }

    fn channel(self: &mut Self) -> Result<Channel> {
        if let Some(ch) = &self.channel {
            return Ok(ch.clone());
        }

        let channel = Channel::from_shared(self.start_endpoint.clone())?
            .tls_config(ClientTlsConfig::new())?
            .connect_lazy()?;

        self.channel = Some(channel.clone());
        return Ok(channel);
    }

    fn create_grpc_client<T, CB>(self: &mut Self, new_func: CB) -> Result<T>
    where
        CB: FnOnce(AuthService) -> T,
    {
        let cred = self.cred.clone();
        let database = self.database.clone();
        let auth_service_create = |ch| {
            return AuthService::new(ch, cred.clone(), database.as_str());
        };

        let channel = self.channel()?;

        let auth_ch = ServiceBuilder::new()
            .layer_fn(auth_service_create)
            .service(channel);

        return Ok(new_func(auth_ch));
    }
}

mod test {
    use super::*;

    fn create_client() -> Result<Client> {
        let token = crate::credentials::StaticToken::from(std::env::var("IAM_TOKEN")?.as_str());
        let database = std::env::var("DB_NAME")?;

        return Client::new(
            "https://ydb.serverless.yandexcloud.net:2135",
            Box::new(token),
            database.as_str(),
        );
    }

    #[tokio::test]
    async fn who_am_i() -> Result<()> {
        let res = create_client()?.who_am_i(WhoAmIRequest::default()).await?;
        assert!(res.user.len() > 0);
        Ok(())
    }

    #[tokio::test]
    async fn endpoints() -> Result<()> {
        let res = create_client()?
            .endpoints(ListEndpointsRequest::default())
            .await?;
        println!("endpoints: {:?}", res);
        Ok(())
    }
}
