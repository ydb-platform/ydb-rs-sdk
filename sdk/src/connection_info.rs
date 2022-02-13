use crate::credentials::{
    credencials_ref, CredentialsRef, GoogleComputeEngineMetadata, StaticToken,
};
use crate::errors::{YdbError, YdbResult};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Mutex;

type ParamHandler = fn(&str, ConnectionInfo) -> YdbResult<ConnectionInfo>;

static PARAM_HANDLERS: Lazy<Mutex<HashMap<String, ParamHandler>>> = Lazy::new(|| {
    Mutex::new({
        let mut m: HashMap<String, ParamHandler> = HashMap::new();

        m.insert("database".to_string(), database);
        m.insert("token_cmd".to_string(), token_cmd);
        m.insert("token_metadata".to_string(), token_metadata);
        m
    })
});

pub(crate) struct ConnectionInfo {
    pub(crate) discovery_endpoint: String, // scheme://host:port, scheme one of grpc/grpcs
    pub(crate) database: String,
    pub(crate) credentials: CredentialsRef,
}

impl ConnectionInfo {
    fn parse_host_and_path(s: &str) -> YdbResult<ConnectionInfo> {
        let url = url::Url::parse(s)?;

        let mut connection_info = ConnectionInfo::default();

        connection_info.discovery_endpoint = format!(
            "{}://{}:{}",
            url.scheme(),
            url.host().unwrap(),
            url.port().unwrap()
        )
        .to_string();
        connection_info.database = url.path().to_string();
        return Ok(connection_info);
    }
}

impl Default for ConnectionInfo {
    fn default() -> Self {
        return Self {
            discovery_endpoint: "".to_string(),
            database: "".to_string(),
            credentials: credencials_ref(StaticToken::from("")),
        };
    }
}

impl FromStr for ConnectionInfo {
    type Err = YdbError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut connection_info = Self::parse_host_and_path(s)?;

        let handlers = PARAM_HANDLERS.lock()?;

        for (key, _) in url::Url::parse(s)?.query_pairs() {
            if let Some(handler) = handlers.get(key.as_ref()) {
                connection_info = handler(s, connection_info)?;
            }
        }
        return Ok(connection_info);
    }
}

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

fn database(uri: &str, mut connection_info: ConnectionInfo) -> YdbResult<ConnectionInfo> {
    for (key, value) in url::Url::parse(uri)?.query_pairs() {
        if key != "database" {
            continue;
        };

        connection_info.database = value.to_string();
    }
    return Ok(connection_info);
}

fn token_cmd(uri: &str, mut connection_info: ConnectionInfo) -> YdbResult<ConnectionInfo> {
    for (key, value) in url::Url::parse(uri)?.query_pairs() {
        if key != "token_cmd" {
            continue;
        };

        connection_info.credentials = credencials_ref(
            crate::credentials::CommandLineYcToken::from_string_cmd(value.as_ref())?,
        );
    }
    return Ok(connection_info);
}

fn token_metadata(uri: &str, mut connection_info: ConnectionInfo) -> YdbResult<ConnectionInfo> {
    for (key, value) in url::Url::parse(uri)?.query_pairs() {
        if key != "token_metadata" {
            continue;
        };

        match value.as_ref() {
            "google" => {
                connection_info.credentials = credencials_ref(GoogleComputeEngineMetadata::new())
            }
            _ => {
                return Err(YdbError::Custom(format!(
                    "unknown metadata format: '{}'",
                    value
                )))
            }
        }
    }
    return Ok(connection_info);
}
