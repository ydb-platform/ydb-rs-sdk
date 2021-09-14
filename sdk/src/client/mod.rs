mod internal;

use crate::client::internal::AuthService;
use crate::credentials::Credencials;
use crate::errors::{Error, Result};
use prost::Message; // for decode result messages from bytes
use tonic::transport::Channel;
use tower::ServiceBuilder;
use ydb_protobuf::generated::ydb::discovery::v1::discovery_service_client::DiscoveryServiceClient;
use ydb_protobuf::generated::ydb::discovery::{WhoAmIRequest, WhoAmIResult};
use ydb_protobuf::generated::ydb::status_ids::StatusCode;

pub struct Client {
    channel: Channel,
    cred: Box<dyn Credencials>,
    database: String,
}

impl Client {
    pub fn new(endpoint: &str, cred: Box<dyn Credencials>, database: &str) -> Result<Self> {
        let tls = tonic::transport::ClientTlsConfig::new();
        let channel = tonic::transport::Channel::from_shared(endpoint.to_string())?
            .tls_config(tls)?
            .connect_lazy()?;

        return Ok(Client {
            channel,
            cred,
            database: database.to_string(),
        });
    }

    fn create_client<T, CB>(self: &Self, create_client: CB) -> T
    where
        CB: FnOnce(AuthService) -> T,
    {
        let auth_service_create = |ch| {
            return internal::AuthService::new(ch, self.cred.clone(), self.database.as_str());
        };

        let auth_ch = ServiceBuilder::new()
            .layer_fn(auth_service_create)
            .service(self.channel.clone());

        // return DiscoveryServiceClient::new(auth_ch);
        return create_client(auth_ch);
    }

    pub async fn who_am_i(self: &Self) -> Result<String> {
        let op = self
            .create_client(DiscoveryServiceClient::new)
            .who_am_i(WhoAmIRequest {
                include_groups: false,
            })
            .await?
            .into_inner()
            .operation
            .unwrap();
        if op.status() != StatusCode::Success {
            return Err(Error::from(op.status()));
        }
        let opres = op.result.unwrap();
        println!("res url: {:?}", opres.type_url);

        let res: WhoAmIResult = WhoAmIResult::decode(opres.value.as_slice())?;
        println!("res: {:?}", res.user);
        return Ok(res.user);
    }
}

mod test {
    use super::*;

    #[tokio::test]
    async fn who_am_i() -> Result<()> {
        let token = crate::credentials::StaticToken::from(std::env::var("IAM_TOKEN")?.as_str());
        let database = std::env::var("DB_NAME")?;
        let client = Client::new(
            "https://ydb.serverless.yandexcloud.net:2135",
            Box::new(token),
            database.as_str(),
        )?;
        client.who_am_i().await?;
        return Ok(());
    }
}
