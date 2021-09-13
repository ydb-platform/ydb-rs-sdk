use bytes::Buf;

use tonic::metadata::{ MetadataValue};
use tonic::{Request, Status};
use prost::Message;
use ydb_protobuf::generated::ydb;
use ydb::{
    discovery::{ListEndpointsRequest, WhoAmIRequest},
    status_ids::StatusCode,
};

use crate::errors::{Error,Result};
use ydb_protobuf::generated::ydb::discovery::{WhoAmIResult, WhoAmIResponse};
use ydb_protobuf::generated::ydb::discovery::v1::discovery_service_client::DiscoveryServiceClient;
use tonic::transport::Channel;
use tonic::codegen::InterceptedService;

pub struct Client {
    channel: tonic::transport::Channel,
    endpoint: String,
}

impl Client {
    pub fn new(endpoint: &str) -> Result<Self> {
        let tls = tonic::transport::ClientTlsConfig::new();
        let channel =
            tonic::transport::Channel::from_shared(endpoint.to_string())?
                .tls_config(tls)?
                .connect_lazy()?;
        return Ok(Client {
            channel,
            endpoint: endpoint.to_string()
        });
    }

    fn create_client_discovery(self: &Self) -> DiscoveryServiceClient<
        InterceptedService<Channel,  fn(Request<()>) -> std::result::Result<Request<()>, Status>>
    >
    {
        let ch = self.channel.clone();
        return ydb_protobuf::generated::ydb::discovery::v1
        ::discovery_service_client::DiscoveryServiceClient::with_interceptor(ch, set_auth_header);

    }

    pub async fn who_am_i(
        self: &Self,
    ) -> Result<String>
    {
        let op = self.create_client_discovery()
            .who_am_i(WhoAmIRequest {
                include_groups: false
            })
            .await?.into_inner().operation.unwrap();
        if op.status() != StatusCode::Success {
            return Err(Error::from(op.status()));
        }
        let opres = op.result.unwrap();
        println!("res url: {:?}", opres.type_url);

        let res:WhoAmIResult  = WhoAmIResult::decode(opres.value.as_slice())?;
        println!("res: {:?}", res.user);
        return Ok(res.user);
    }
}

fn set_auth_header(mut req: Request<()>) -> std::result::Result<Request<()>, Status> {
    let token = MetadataValue::from_str(std::env::var("IAM_TOKEN").unwrap().as_str()).unwrap();
    let database = MetadataValue::from_str(std::env::var("DB_NAME").unwrap().as_str()).unwrap();

    println!("rekby-auth");
    req.metadata_mut().insert("x-ydb-auth-ticket", token);
    req.metadata_mut().insert("x-ydb-database", database);
    return Ok(req);
}

mod test {
    use super::*;

    #[tokio::test]
    async fn who_am_i() -> Result<()> {
        let token = crate::credentials::StaticToken::from(std::env::var("IAM_TOKEN")?);
        let client = Client::new("https://ydb.serverless.yandexcloud.net:2135")?;
        client.who_am_i().await?;
        return Ok(());
    }
}
