mod internal;

use bytes::Buf;

use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::{Request, Status};
use ydb::{
    discovery::{ListEndpointsRequest, WhoAmIRequest},
    status_ids::StatusCode,
};
use ydb_protobuf::generated::ydb;

use crate::client::internal::AuthService;
use crate::credentials::Credencials;
use crate::errors::{Error, Result};
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;
use tower::ServiceBuilder;
use ydb_protobuf::generated::ydb::discovery::v1::discovery_service_client::DiscoveryServiceClient;
use ydb_protobuf::generated::ydb::discovery::{WhoAmIResponse, WhoAmIResult};

pub struct Client<C>
where
    C: Credencials,
{
    channel: Channel,
    cred: C,
    endpoint: String,
}

impl<C> Client<C>
where
    C: Credencials,
{
    pub fn new(endpoint: &str, cred: C) -> Result<Self> {
        let tls = tonic::transport::ClientTlsConfig::new();
        let channel = tonic::transport::Channel::from_shared(endpoint.to_string())?
            .tls_config(tls)?
            .connect_lazy()?;

        return Ok(Client {
            channel: channel,
            cred,
            endpoint: endpoint.to_string(),
        });
    }

    fn set_auth_header(
        self: &Self,
        mut req: Request<()>,
    ) -> std::result::Result<Request<()>, Status> {
        let mut token = String::new();
        self.cred.fill_token(&mut token);

        let token = MetadataValue::from_str(token.as_str()).unwrap();
        let database = MetadataValue::from_str(std::env::var("DB_NAME").unwrap().as_str()).unwrap();

        println!("rekby-auth");
        req.metadata_mut().insert("x-ydb-auth-ticket", token);
        req.metadata_mut().insert("x-ydb-database", database);
        return Ok(req);
    }

    fn create_client_discovery(
        self: &Self,
    ) -> DiscoveryServiceClient<
        InterceptedService<Channel, fn(Request<()>) -> std::result::Result<Request<()>, Status>>,
    > {

        let auth_service_create = |ch| {
            return internal::AuthService::new(
                ch,
                self.cred.clone(),
                "/ru-central1/b1g7h2ccv6sa5m9rotq4/etn00qhcjn6pap901icc",
            );
        };

        let auth_ch = ServiceBuilder::new()
            .layer_fn(auth_service_create)
            .service(self.channel.clone());


        return ydb_protobuf::generated::ydb::discovery::v1
        ::discovery_service_client::DiscoveryServiceClient::new(auth_ch);
    }

    pub async fn who_am_i(self: &Self) -> Result<String> {
        let op = self
            .create_client_discovery()
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
        let token = crate::credentials::StaticToken::from(std::env::var("IAM_TOKEN")?);
        let client = Client::new(
            "https://ydb.serverless.yandexcloud.net:2135",
            Box::new(token),
        )?;
        client.who_am_i().await?;
        return Ok(());
    }
}
