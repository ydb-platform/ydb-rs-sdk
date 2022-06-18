use crate::client_common::{DBCredentials, TokenCache};
use crate::credentials::{credencials_ref, CredentialsRef, GCEMetadata, StaticToken};
use crate::discovery::{Discovery, TimerDiscovery};
use crate::errors::{YdbError, YdbResult};
use crate::{Client, Credentials};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;

type ParamHandler = fn(&str, ClientBuilder) -> YdbResult<ClientBuilder>;

static PARAM_HANDLERS: Lazy<Mutex<HashMap<String, ParamHandler>>> = Lazy::new(|| {
    Mutex::new({
        let mut m: HashMap<String, ParamHandler> = HashMap::new();

        m.insert("database".to_string(), database);
        m.insert("token".to_string(), token);
        m.insert("token_cmd".to_string(), token_cmd);
        m.insert("token_metadata".to_string(), token_metadata);
        m
    })
});

// TODO: ParamHandler to Fn trait
#[allow(dead_code)]
pub(crate) fn register(param_name: &str, handler: ParamHandler) -> YdbResult<()> {
    let mut lock = PARAM_HANDLERS.lock()?;
    if lock.contains_key(param_name) {
        return Err(YdbError::Custom(
            format!("param handler already exist for '{}'", param_name).into(),
        ));
    };

    lock.insert(param_name.to_string(), handler);
    return Ok(());
}

fn database(uri: &str, mut client_builder: ClientBuilder) -> YdbResult<ClientBuilder> {
    for (key, value) in url::Url::parse(uri)?.query_pairs() {
        if key != "database" {
            continue;
        };

        client_builder.database = value.to_string();
    }
    return Ok(client_builder);
}

fn token(uri: &str, mut client_builder: ClientBuilder) -> YdbResult<ClientBuilder> {
    for (key, value) in url::Url::parse(uri)?.query_pairs() {
        if key != "token" {
            continue;
        };

        client_builder.credentials =
            credencials_ref(crate::credentials::StaticToken::from(value.as_ref()));
    }
    return Ok(client_builder);
}

fn token_cmd(uri: &str, mut client_builder: ClientBuilder) -> YdbResult<ClientBuilder> {
    for (key, value) in url::Url::parse(uri)?.query_pairs() {
        if key != "token_cmd" {
            continue;
        };

        client_builder.credentials = credencials_ref(
            crate::credentials::CommandLineYcToken::from_cmd(value.as_ref())?,
        );
    }
    return Ok(client_builder);
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
    return Ok(client_builder);
}

pub struct ClientBuilder {
    pub(crate) credentials: CredentialsRef,
    pub(crate) database: String,
    discovery_interval: Duration,
    pub(crate) endpoint: String,
    discovery: Option<Box<dyn Discovery>>,
}

impl ClientBuilder {
    pub fn from_str<T: Into<String>>(s: T) -> Result<Self, YdbError> {
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
        return Ok(client_builder);
    }

    pub fn client(self) -> YdbResult<Client> {
        let db_cred = DBCredentials {
            token_cache: TokenCache::new(self.credentials.clone())?,
            database: self.database.clone(),
        };

        let discovery = match self.discovery {
            Some(discovery_box) => discovery_box,
            None => Box::new(TimerDiscovery::new(
                db_cred.clone(),
                self.endpoint.as_str(),
                self.discovery_interval,
            )?),
        };

        return Client::new(db_cred, discovery);
    }

    pub fn with_credentials<T: 'static + Credentials>(mut self, cred: T) -> Self {
        self.credentials = credencials_ref(cred);
        return self;
    }

    pub fn with_database<T: Into<String>>(mut self, database: T) -> Self {
        self.database = database.into();
        return self;
    }

    pub fn with_endpoint<T: Into<String>>(mut self, endpoint: T) -> Self {
        self.endpoint = endpoint.into();
        return self;
    }

    /// Set discovery implementation
    ///
    /// Example:
    /// ```no_run
    /// # use ydb::{ClientBuilder, StaticDiscovery, YdbResult};
    ///
    /// # fn main()->YdbResult<()>{
    /// let discovery = StaticDiscovery::from_str("grpc://localhost:2136")?;
    /// let client = ClientBuilder::from_str("grpc://localhost:2136/?database=/local")?.with_discovery(discovery).client()?;
    /// # return Ok(())
    /// # }
    /// ```
    pub fn with_discovery<T: 'static + Discovery>(mut self, discovery: T) -> Self {
        self.discovery = Some(Box::new(discovery));
        return self;
    }

    fn new() -> Self {
        Self {
            credentials: credencials_ref(StaticToken::from("")),
            database: "/local".to_string(),
            discovery_interval: Duration::from_secs(60),
            endpoint: "grpc://localhost:2135".to_string(),
            discovery: None,
        }
    }

    fn parse_host_and_path(&mut self, s: &str) -> YdbResult<()> {
        let url = url::Url::parse(s)?;

        self.endpoint = format!(
            "{}://{}:{}",
            url.scheme(),
            url.host().unwrap(),
            url.port().unwrap()
        )
        .to_string();
        self.database = url.path().to_string();
        return Ok(());
    }
}

// allow "asd".parse() for create builder
impl FromStr for ClientBuilder {
    type Err = YdbError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ClientBuilder::from_str(s)
    }
}
