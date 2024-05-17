use crate::client_common::{DBCredentials, TokenCache};
use crate::credentials::{
    credencials_ref, AccessTokenCredentials, CredentialsRef, GCEMetadata, StaticCredentials,
};
use crate::dicovery_pessimization_interceptor::DiscoveryPessimizationInterceptor;
use crate::discovery::{Discovery, TimerDiscovery};
use crate::errors::{YdbError, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::auth::AuthGrpcInterceptor;
use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};
use crate::{Client, Credentials};
use http::Uri;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

type ParamHandler = fn(&str, ClientBuilder) -> YdbResult<ClientBuilder>;

static PARAM_HANDLERS: Lazy<Mutex<HashMap<String, ParamHandler>>> = Lazy::new(|| {
    Mutex::new({
        let mut m: HashMap<String, ParamHandler> = HashMap::new();

        m.insert("database".to_string(), database);
        m.insert("token".to_string(), token);
        m.insert("token_cmd".to_string(), token_cmd);
        m.insert("token_metadata".to_string(), token_metadata);
        m.insert("token_static_password".to_string(), token_static_password);
        m.insert("ca_certificate".to_string(), ca_certificate);
        m
    })
});

// TODO: ParamHandler to Fn trait
#[allow(dead_code)]
pub(crate) fn register(param_name: &str, handler: ParamHandler) -> YdbResult<()> {
    let mut lock = PARAM_HANDLERS.lock()?;
    if lock.contains_key(param_name) {
        return Err(YdbError::Custom(format!(
            "param handler already exist for '{}'",
            param_name
        )));
    };

    lock.insert(param_name.to_string(), handler);
    Ok(())
}

fn database(uri: &str, mut client_builder: ClientBuilder) -> YdbResult<ClientBuilder> {
    for (key, value) in url::Url::parse(uri)?.query_pairs() {
        if key != "database" {
            continue;
        };

        client_builder.database = value.to_string();
    }
    Ok(client_builder)
}

fn token(uri: &str, mut client_builder: ClientBuilder) -> YdbResult<ClientBuilder> {
    for (key, value) in url::Url::parse(uri)?.query_pairs() {
        if key != "token" {
            continue;
        };

        client_builder.credentials = credencials_ref(
            crate::credentials::AccessTokenCredentials::from(value.as_ref()),
        );
    }
    Ok(client_builder)
}

fn token_cmd(uri: &str, mut client_builder: ClientBuilder) -> YdbResult<ClientBuilder> {
    for (key, value) in url::Url::parse(uri)?.query_pairs() {
        if key != "token_cmd" {
            continue;
        };

        client_builder.credentials = credencials_ref(
            crate::credentials::CommandLineCredentials::from_cmd(value.as_ref())?,
        );
    }
    Ok(client_builder)
}

fn token_metadata(uri: &str, mut client_builder: ClientBuilder) -> YdbResult<ClientBuilder> {
    for (key, value) in url::Url::parse(uri)?.query_pairs() {
        if key != "token_metadata" {
            continue;
        };

        match value.as_ref() {
            "google" | "yandex-cloud" => {
                client_builder.credentials = credencials_ref(GCEMetadata::new())
            }
            _ => {
                return Err(YdbError::Custom(format!(
                    "unknown metadata format: '{}'",
                    value
                )))
            }
        }
    }
    Ok(client_builder)
}

fn token_static_password(uri: &str, mut client_builder: ClientBuilder) -> YdbResult<ClientBuilder> {
    let mut username = Option::<String>::default();
    let mut password = Option::<String>::default();

    for (key, value) in url::Url::parse(uri)?.query_pairs() {
        match key.as_ref() {
            "token_static_password" => {
                password = Some(value.as_ref().to_string());
            }
            "token_static_username" => {
                username = Some(value.as_ref().to_string());
            }
            _ => {
                continue;
            }
        }
    }
    if username.is_none() {
        return Err(YdbError::Custom(
            "username was not provided for password authentication".to_string(),
        ));
    }
    if password.is_none() {
        return Err(YdbError::Custom(
            "password was not provided for password authentication".to_string(),
        ));
    }
    let username = username.unwrap();
    let password = password.unwrap();

    if client_builder.database.is_empty() {
        client_builder = database(uri, client_builder)?;
    }
    if client_builder.cert_path.is_none() {
        client_builder = ca_certificate(uri, client_builder)?;
    }

    let endpoint: Uri = Uri::from_str(client_builder.endpoint.as_str())?;

    let creds = match client_builder.cert_path.as_ref() {
        Some(path) => StaticCredentials::new_with_ca(
            username,
            password,
            endpoint,
            client_builder.database.clone(),
            path.clone(),
        ),
        None => StaticCredentials::new(
            username,
            password,
            endpoint,
            client_builder.database.clone(),
        )
    };
    client_builder.credentials = credencials_ref(creds);

    Ok(client_builder)
}

fn ca_certificate(uri: &str, mut client_builder: ClientBuilder) -> YdbResult<ClientBuilder> {
    for (key, value) in url::Url::parse(uri)?.query_pairs() {
        if key != "ca_certificate" {
            continue;
        };
        client_builder.cert_path = Some(value.as_ref().to_string());
        break;
    }

    Ok(client_builder)
}

pub struct ClientBuilder {
    pub(crate) credentials: CredentialsRef,
    pub(crate) database: String,
    discovery_interval: Duration,
    pub(crate) endpoint: String,
    discovery: Option<Box<dyn Discovery>>,
    pub cert_path: Option<String>,
}

impl ClientBuilder {
    pub fn new_from_connection_string<T: Into<String>>(s: T) -> Result<Self, YdbError> {
        let s = s.into();
        let s = s.as_str();
        let mut client_builder = ClientBuilder::new();
        client_builder.parse_host_and_path(s)?;

        let handlers = PARAM_HANDLERS.lock()?;

        for (key, _) in url::Url::parse(s)?.query_pairs() {
            if let Some(handler) = handlers.get(key.as_ref()) {
                client_builder = handler(s, client_builder)?;
            }
        }
        Ok(client_builder)
    }

    pub fn client(self) -> YdbResult<Client> {
        let db_cred = DBCredentials {
            token_cache: TokenCache::new(self.credentials.clone())?,
            database: self.database.clone(),
        };

        let endpoint: Uri = Uri::from_str(self.endpoint.as_str())?;
        let static_balancer = StaticLoadBalancer::new(endpoint);

        let interceptor =
            MultiInterceptor::new().with_interceptor(AuthGrpcInterceptor::new(db_cred.clone())?);

        let discovery_connection_manager = GrpcConnectionManager::new(
            SharedLoadBalancer::new_with_balancer(Box::new(static_balancer)),
            db_cred.database.clone(),
            interceptor.clone(),
            self.cert_path.clone(),
        );

        let discovery = match self.discovery {
            Some(discovery_box) => discovery_box,
            None => Box::new(TimerDiscovery::new(
                discovery_connection_manager,
                self.endpoint.as_str(),
                self.discovery_interval,
                Box::new(db_cred.token_cache.clone()),
            )?),
        };

        let discovery = Arc::new(discovery);

        let interceptor =
            interceptor.with_interceptor(DiscoveryPessimizationInterceptor::new(discovery.clone()));

        let load_balancer = SharedLoadBalancer::new(discovery.as_ref().as_ref());
        let connection_manager =
            GrpcConnectionManager::new(load_balancer, db_cred.database.clone(), interceptor, self.cert_path);

        Client::new(db_cred, discovery, connection_manager)
    }

    pub fn with_credentials<T: 'static + Credentials>(mut self, cred: T) -> Self {
        self.credentials = credencials_ref(cred);
        self
    }

    pub fn with_database<T: Into<String>>(mut self, database: T) -> Self {
        self.database = database.into();
        self
    }

    pub fn with_endpoint<T: Into<String>>(mut self, endpoint: T) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    /// Set discovery implementation
    ///
    /// Example:
    /// ```no_run
    /// # use ydb::{ClientBuilder, StaticDiscovery, YdbResult};
    ///
    /// # fn main()->YdbResult<()>{
    /// let discovery = StaticDiscovery::new_from_str("grpc://localhost:2136")?;
    /// let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136/local")?.with_discovery(discovery).client()?;
    /// # return Ok(());
    /// # }
    /// ```
    pub fn with_discovery<T: 'static + Discovery>(mut self, discovery: T) -> Self {
        self.discovery = Some(Box::new(discovery));
        self
    }

    fn new() -> Self {
        Self {
            credentials: credencials_ref(AccessTokenCredentials::from("")),
            database: "/local".to_string(),
            discovery_interval: Duration::from_secs(60),
            endpoint: "grpc://localhost:2135".to_string(),
            discovery: None,
            cert_path: None,
        }
    }

    fn parse_host_and_path(&mut self, s: &str) -> YdbResult<()> {
        let url = url::Url::parse(s)?;

        self.endpoint = format!(
            "{}://{}:{}",
            url.scheme(),
            url.host().unwrap(),
            url.port().unwrap()
        );
        self.database = url.path().to_string();
        Ok(())
    }
}

// allow "asd".parse() for create builder
impl FromStr for ClientBuilder {
    type Err = YdbError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ClientBuilder::new_from_connection_string(s)
    }
}

#[cfg(test)]
mod test {
    use crate::{ClientBuilder, YdbError, YdbResult};

    #[test]
    fn database_from_path() -> YdbResult<()> {
        let builder = ClientBuilder::new_from_connection_string("http://asd:222/qwe1")?;
        assert_eq!(builder.database, "/qwe1".to_string());
        Ok(())
    }

    #[test]
    fn database_from_arg() -> YdbResult<()> {
        let builder = ClientBuilder::new_from_connection_string("http://asd:222/?database=/qwe2")?;
        assert_eq!(builder.database, "/qwe2".to_string());
        Ok(())
    }

    #[test]
    fn password_without_username() -> YdbResult<()> {
        let builder = ClientBuilder::new_from_connection_string(
            "http://asd:222/qwe1?token_static_password=hello",
        );

        match builder {
            Err(YdbError::Custom(_)) => Ok(()),
            _ => Err(YdbError::Custom(
                "expected connection string parsing failure".to_string(),
            )),
        }
    }
}
