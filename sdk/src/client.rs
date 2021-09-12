use bytes::Buf;

use tonic::metadata::{ MetadataValue};
use tonic::{Request, Status};
use prost::Message;
use ydb_protobuf::generated::ydb;
use ydb::{
    discovery::{ListEndpointsRequest, WhoAmIRequest},
    status_ids::StatusCode,
};

use crate::errors::Error;
use ydb_protobuf::generated::ydb::discovery::{WhoAmIResult, WhoAmIResponse};

pub struct Client {}

impl Client {
    pub fn new() -> Self {
        return Client {};
    }

    pub async fn who_am_i(
        self: &Self,
    ) -> Result<String, Box<dyn std::error::Error>>
    {
        let tls = tonic::transport::ClientTlsConfig::new();
        let channel =
            tonic::transport::Channel::from_static("https://ydb.serverless.yandexcloud.net:2135")
                .tls_config(tls)?
                .connect()
                .await?;

        let mut discovery_client = ydb_protobuf::generated::ydb::discovery::v1
        ::discovery_service_client::DiscoveryServiceClient::with_interceptor(channel, set_auth_header);
        let op = discovery_client
            .who_am_i(WhoAmIRequest {
                include_groups: false
            })
            .await?.into_inner().operation.unwrap();
        if op.status() != StatusCode::Success {
            return Err(Box::new(Error::from_str(format!("Bad status code: {:?}", op.status()).as_str())));
        }
        let opres = op.result.unwrap();
        println!("res url: {:?}", opres.type_url);

        let res:WhoAmIResult  = WhoAmIResult::decode(opres.value.as_slice())?;
        println!("res: {:?}", res.user);
        return Ok(res.user);
    }
}

fn set_auth_header(mut req: Request<()>) -> Result<Request<()>, Status> {
    let token = MetadataValue::from_str(std::env::var("IAM_TOKEN").unwrap().as_str()).unwrap();
    let database = MetadataValue::from_str("/ru-central1/b1g7h2ccv6sa5m9rotq4/etn00qhcjn6pap901icc").unwrap();

    println!("rekby-auth");
    req.metadata_mut().insert("x-ydb-auth-ticket", token);
    req.metadata_mut().insert("x-ydb-database", database);
    return Ok(req);
}

mod test {
    use super::*;

    #[tokio::test]
    async fn who_am_i() -> Result<(), Box<dyn std::error::Error>> {
        let token = crate::credentials::StaticToken::from(std::env::var("IAM_TOKEN")?);
        let client = Client::new();
        let id = client.who_am_i().await?;
        assert_eq!(
            id,
            "asd"
        );
        return Ok(());
    }
}
