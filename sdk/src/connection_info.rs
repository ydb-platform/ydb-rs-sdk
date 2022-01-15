use ctor::ctor;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Mutex;
use http::Uri;
use once_cell::sync::Lazy;
use url::Url;
use crate::errors::{Error,Result};
use crate::credentials::{Credentials, StaticToken};

type ParamHandler = fn(&str,ConnectionInfo)->Result<ConnectionInfo>;

static PARAM_HANDLERS: Lazy<Mutex<HashMap<String, ParamHandler>>> = Lazy::new( || Mutex::new({
    let mut m :HashMap<String, ParamHandler> = HashMap::new();

    m.insert("database".to_string(), database);
    m.insert("token_cmd".to_string(), token_cmd);

    m
}));

pub struct ConnectionInfo {
    pub discovery_endpoint: String, // scheme://host:port, scheme one of grpc/grpcs
    pub database: String,
    pub credentials: Box<dyn Credentials>,
}

impl Default for ConnectionInfo {
    fn default() -> Self {
        return Self{
            discovery_endpoint: "".to_string(),
            database: "".to_string(),
            credentials: Box::new(StaticToken::from(""))
        }
    }
}

impl ConnectionInfo {
    pub fn parse(uri: &str)->Result<ConnectionInfo>{
        let mut connection_info = ConnectionInfo::default();

        connection_info = Self::parse_endpoint(uri, connection_info)?;

        let mut handlers = PARAM_HANDLERS.lock()?;

        for (key, _) in url::Url::parse(uri)?.query_pairs() {
            if let Some(handler) = handlers.get(key.as_ref()) {
                connection_info = handler(uri, connection_info)?;
            }
        };
        return Ok(connection_info)
    }

    fn parse_endpoint(s: &str, mut connection_info: ConnectionInfo)->Result<ConnectionInfo>{
        let url = url::Url::parse(s)?;
        connection_info.discovery_endpoint =
            format!("{}://{}:{}", url.scheme(), url.host().unwrap(), url.port().unwrap()).to_string();
        return Ok(connection_info);
    }
}


// TODO: ParamHandler to Fn trait
pub fn register(param_name: &str, handler: ParamHandler)->Result<()>{
    let mut lock = PARAM_HANDLERS.lock()?;
    if lock.contains_key(param_name) {
        return Err(Error::Custom(format!("param handler already exist for '{}'", param_name).into()))
    };

    lock.insert(param_name.to_string(), handler);
    return Ok(());
}

fn database(uri: &str, mut connection_info: ConnectionInfo)->Result<ConnectionInfo>{
    for (key, value) in url::Url::parse(uri)?.query_pairs() {
        if key != "database" {
            continue
        };

        connection_info.database = value.to_string();
    };
    return Ok(connection_info)
}

fn token_cmd(uri: &str, mut connection_info: ConnectionInfo)->Result<ConnectionInfo>{
    for (key, value) in url::Url::parse(uri)?.query_pairs() {
        if key != "token_cmd" {
            continue
        };

        connection_info.credentials =
            Box::new(
                crate::credentials::CommandLineYcToken::from_string_cmd(value.as_ref())?
            );
    };
    return Ok(connection_info)
}