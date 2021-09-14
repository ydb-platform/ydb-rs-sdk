mod internal;
mod trait_operation;

use crate::client::internal::AuthService;
use crate::client::trait_operation::Operation;
use crate::credentials::Credencials;
use crate::errors::{Error, Result};
use ydb_protobuf::generated::ydb::discovery::v1::discovery_service_client::DiscoveryServiceClient;
use ydb_protobuf::generated::ydb::discovery::{WhoAmIRequest, WhoAmIResult};
use ydb_protobuf::generated::ydb::status_ids::StatusCode;

pub struct Client {
    discovery_client: DiscoveryServiceClient<AuthService>,
}

impl Client {
    pub fn new(endpoint: &str, cred: Box<dyn Credencials>, database: &str) -> Result<Self> {
        let tls = tonic::transport::ClientTlsConfig::new();
        let channel = tonic::transport::Channel::from_shared(endpoint.to_string())?
            .tls_config(tls)?
            .connect_lazy()?;

        let create_grpc_client =
            |new_func| internal::create_grpc_client(&channel, &cred, database, new_func);

        return Ok(Client {
            discovery_client: create_grpc_client(DiscoveryServiceClient::new),
        });
    }

    fn grpc_read_result<TOp, T>(resp: tonic::Response<TOp>) -> Result<T>
    where
        TOp: Operation,
        T: Default + prost::Message,
    {
        let resp_inner = resp.into_inner();
        let op = resp_inner.operation().unwrap();
        if op.status() != StatusCode::Success {
            return Err(Error::from(op.status()));
        }
        let opres = op.result.unwrap();
        let res: T = T::decode(opres.value.as_slice())?;
        return Ok(res);
    }

    pub async fn who_am_i(self: &mut Self) -> Result<String> {
        let res = self
            .discovery_client
            .who_am_i(WhoAmIRequest {
                include_groups: false,
            })
            .await?;
        let res: WhoAmIResult = Self::grpc_read_result(res)?;
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
        let mut client = Client::new(
            "https://ydb.serverless.yandexcloud.net:2135",
            Box::new(token),
            database.as_str(),
        )?;
        client.who_am_i().await?;
        return Ok(());
    }
}
