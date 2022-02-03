use crate::errors::{YdbError, YdbResult};
use crate::pub_traits::{Credentials, TokenInfo};
use dyn_clone::DynClone;
use serde::Deserialize;
use std::fmt::{format, Debug};
use std::ops::Add;
use std::process::Command;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

pub(crate) type CredentialsRef = Arc<Box<dyn Credentials>>;

pub(crate) fn credencials_ref<T: 'static + Credentials>(cred: T) -> CredentialsRef {
    Arc::new(Box::new(cred))
}

#[derive(Debug, Clone)]
pub struct StaticToken {
    pub token: String,
}

impl StaticToken {
    #[allow(unused)]
    pub fn from(token: &str) -> Self {
        return StaticToken {
            token: token.to_string(),
        };
    }
}

impl Credentials for StaticToken {
    fn create_token(&self) -> YdbResult<TokenInfo> {
        return Ok(TokenInfo::token(self.token.clone()));
    }

    fn debug_string(&self) -> String {
        let (begin, end) = if self.token.len() > 20 {
            (
                &self.token.as_str()[0..3],
                &self.token.as_str()[(self.token.len() - 3)..self.token.len()],
            )
        } else {
            ("xxx", "xxx")
        };
        return format!("static token: {begin}...{end}");
    }
}

#[derive(Clone, Debug)]
pub struct CommandLineYcToken {
    token: Arc<RwLock<String>>,
    command: Arc<Mutex<Command>>,
}

impl CommandLineYcToken {
    #[allow(dead_code)]
    pub fn from_string_cmd(cmd: &str) -> YdbResult<Self> {
        let cmd_parts: Vec<&str> = cmd.split_whitespace().collect();

        if cmd_parts.len() < 1 {
            return Err(YdbError::Custom(
                format!("can't split get token command: '{}'", cmd).into(),
            ));
        }

        let mut command = Command::new(cmd_parts[0]);
        command.args(&cmd_parts.as_slice()[1..]);

        return Ok(CommandLineYcToken {
            token: Arc::new(RwLock::new("".to_string())),
            command: Arc::new(Mutex::new(command)),
        });
    }
}

impl Credentials for CommandLineYcToken {
    fn create_token(&self) -> YdbResult<TokenInfo> {
        {
            let token = self.token.read()?;
            if token.as_str() != "" {
                return Ok(TokenInfo::token(token.clone()));
            }
        }
        {
            let mut token = self.token.write()?;
            if token.as_str() != "" {
                return Ok(TokenInfo::token(token.clone()));
            }
            let result = self.command.lock()?.output()?;
            if !result.status.success() {
                let err = String::from_utf8(result.stderr)?;
                return Err(YdbError::Custom(format!(
                    "can't execute yc ({}): {}",
                    result.status.code().unwrap(),
                    err
                )));
            }
            *token = String::from_utf8(result.stdout)?.trim().to_string();
            return Ok(TokenInfo::token(token.clone()));
        }
    }

    fn debug_string(&self) -> String {
        let token = self.token.read().unwrap();
        let (_begin, _end) = if token.len() > 20 {
            (
                &token.as_str()[0..3],
                &token.as_str()[(token.len() - 3)..token.len()],
            )
        } else {
            ("xxx", "xxx")
        };

        return format!("{:?} ({_begin}..{_end})", self.command.lock().unwrap(),);
    }
}

pub struct GoogleComputeEngineMetadata {
    uri: String,
}

impl GoogleComputeEngineMetadata {
    pub fn new() -> Self {
        Self{
            uri: "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token".parse().unwrap(),
        }
    }
}

impl Credentials for GoogleComputeEngineMetadata {
    fn create_token(&self) -> YdbResult<TokenInfo> {
        http::Request::builder()
            .uri(self.uri.clone())
            .header("Metadata-Flavor", "Google");
        let mut request =
            reqwest::blocking::Request::new(reqwest::Method::GET, self.uri.parse().unwrap());
        request
            .headers_mut()
            .insert("Metadata-Flavor", "Google".parse().unwrap());

        #[derive(Deserialize)]
        struct TokenResponse {
            access_token: String,
            expires_in: u64,
            token_type: String,
        }

        let client = reqwest::blocking::Client::new();
        let res: TokenResponse = client
            .request(reqwest::Method::GET, self.uri.as_str())
            .header("Metadata-Flavor", "Google")
            .send()?
            .json()?;
        return Ok(
            TokenInfo::token(format!("{} {}", res.token_type, res.access_token))
                .with_renew(Instant::now().add(Duration::from_secs(res.expires_in))),
        );
    }

    fn debug_string(&self) -> String {
        return format!("GoogleComputeEngineMetadata from {}", self.uri.as_str());
    }
}
