use tonic::metadata::{Ascii, MetadataValue};
use tonic::{Request, Status};
use ydb_protobuf::generated::ydb::discovery::{ListEndpointsRequest, WhoAmIRequest};

pub struct Client {}

impl Client {
    pub fn new() -> Self {
        return Client {};
    }

    pub async fn who_am_i(
        self: &Self,
    ) -> Result<ydb_protobuf::generated::ydb::status_ids::StatusCode, Box<dyn std::error::Error>>
    {
        let tls = tonic::transport::ClientTlsConfig::new();
        let channel =
            tonic::transport::Channel::from_static("https://ydb.serverless.yandexcloud.net:2135")
                .tls_config(tls)?
                .connect()
                .await?;

        let mut discovery_client = ydb_protobuf::generated::ydb::discovery::v1
        ::discovery_service_client::DiscoveryServiceClient::with_interceptor(channel, set_auth_header);
        let res = discovery_client
            .list_endpoints(ListEndpointsRequest {
                database: "/ru-central1/b1g7h2ccv6sa5m9rotq4/etn00qhcjn6pap901icc".to_string(),
                service: vec![],
            })
            .await?;
        let op = res.into_inner().operation.unwrap();

        return Ok(op.status());
    }
}

fn set_auth_header(mut req: Request<()>) -> Result<Request<()>, Status> {
    let token = MetadataValue::from_str(std::env::var("IAM_TOKEN").unwrap().as_str()).unwrap();
    println!("rekby-auth");
    req.metadata_mut().insert("authorization", token);
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
            ydb_protobuf::generated::ydb::status_ids::StatusCode::Success
        );
        return Ok(());
    }
}
