use crate::client::TimeoutSettings;
use crate::errors::{YdbError, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_auth_service::client::RawAuthClient;
use crate::grpc_wrapper::raw_auth_service::login::RawLoginRequest;
use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};
use crate::pub_traits::{Credentials, TokenInfo};
use http::Uri;
use serde::Deserialize;
use std::fmt::Debug;
use std::ops::Add;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub(crate) type CredentialsRef = Arc<Box<dyn Credentials>>;

pub(crate) fn credencials_ref<T: 'static + Credentials>(cred: T) -> CredentialsRef {
    Arc::new(Box::new(cred))
}

/// Credentials with static token without renewing
///
/// Example:
/// ```no_run
/// # use ydb::{ClientBuilder, StaticToken, YdbResult};
/// # fn main()->YdbResult<()>{
/// let builder = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=/local")?;
/// let client = builder.with_credentials(StaticToken::from("asd")).client()?;
/// # return Ok(());
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct StaticToken {
    pub(crate) token: String,
}

impl StaticToken {
    /// Create static token from string
    ///
    /// Example:
    /// ```
    /// # use ydb::StaticToken;
    /// StaticToken::from("asd");
    /// ```
    pub fn from<T: Into<String>>(token: T) -> Self {
        StaticToken {
            token: token.into(),
        }
    }
}

impl Credentials for StaticToken {
    fn create_token(&self) -> YdbResult<TokenInfo> {
        Ok(TokenInfo::token(self.token.clone()))
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
        format!("static token: {}...{}", begin, end)
    }
}

/// Get from stdout of command
///
/// Example create token from yandex cloud command line utility:
/// ```rust
/// use ydb::CommandLineYcToken;
///
/// let cred = CommandLineYcToken::from_cmd("yc iam create-token").unwrap();
/// ```
#[derive(Debug)]
pub struct CommandLineYcToken {
    command: Arc<Mutex<Command>>,
}

impl CommandLineYcToken {
    /// Command line for create token
    ///
    /// The command will be called every time when token needed (token cache by default and will call rare).
    #[allow(dead_code)]
    pub fn from_cmd<T: Into<String>>(cmd: T) -> YdbResult<Self> {
        let cmd = cmd.into();
        let cmd_parts: Vec<&str> = cmd.split_whitespace().collect();

        if cmd_parts.is_empty() {
            return Err(YdbError::Custom(format!(
                "can't split get token command: '{}'",
                cmd
            )));
        }

        let mut command = Command::new(cmd_parts[0]);
        command.args(&cmd_parts.as_slice()[1..]);

        Ok(CommandLineYcToken {
            command: Arc::new(Mutex::new(command)),
        })
    }
}

impl Credentials for CommandLineYcToken {
    fn create_token(&self) -> YdbResult<TokenInfo> {
        let result = self.command.lock()?.output()?;
        if !result.status.success() {
            let err = String::from_utf8(result.stderr)?;
            return Err(YdbError::Custom(format!(
                "can't execute yc ({}): {}",
                result.status.code().unwrap(),
                err
            )));
        }
        let token = String::from_utf8(result.stdout)?.trim().to_string();
        Ok(TokenInfo::token(token))
    }

    fn debug_string(&self) -> String {
        let token_describe: String = match self.create_token() {
            Ok(token_info) => {
                let token = token_info.token;
                let desc: String = if token.len() > 20 {
                    format!(
                        "{}..{}",
                        &token.as_str()[0..3],
                        &token.as_str()[(token.len() - 3)..token.len()]
                    )
                } else {
                    "short_token".to_string()
                };
                desc
            }
            Err(err) => {
                format!("err: {}", err)
            }
        };

        token_describe
    }
}

/// Get token of service account of instance
///
/// Yandex cloud support GCE token compatible. Use it.
/// Example:
/// ```
/// use ydb::YandexMetadata;
/// let cred = YandexMetadata::new();
/// ```
pub type YandexMetadata = GCEMetadata;

/// Get instance service account token from GCE instance
///
/// Get token from google cloud engine instance metadata.
/// By default from url: http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
///
/// Example:
/// ```
/// use ydb::GCEMetadata;
///
/// let cred = GCEMetadata::new();
/// ```
pub struct GCEMetadata {
    uri: String,
}

impl GCEMetadata {
    /// Create GCEMetadata with default url for receive token
    pub fn new() -> Self {
        Self::from_url("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token").unwrap()
    }

    /// Create GCEMetadata with custom url (may need for debug or spec infrastructure with non standard metadata)
    ///
    /// Example:
    /// ```
    /// # use ydb::YdbResult;
    /// # fn main()->YdbResult<()>{
    /// use ydb::GCEMetadata;
    /// let cred = GCEMetadata::from_url("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token")?;
    /// # return Ok(());
    /// # }
    /// ```
    pub fn from_url<T: Into<String>>(url: T) -> YdbResult<Self> {
        Ok(Self {
            uri: url.into().parse()?,
        })
    }
}

impl Default for GCEMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl Credentials for GCEMetadata {
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
        Ok(
            TokenInfo::token(format!("{} {}", res.token_type, res.access_token))
                .with_renew(Instant::now().add(Duration::from_secs(res.expires_in))),
        )
    }

    fn debug_string(&self) -> String {
        format!("GoogleComputeEngineMetadata from {}", self.uri.as_str())
    }
}

pub struct StaticCredentialsAuth {
    username: String,
    password: String,
    database: String,
    endpoint: Uri,
}

impl StaticCredentialsAuth {
    pub async fn acquire_token(&self) -> YdbResult<String> {
        let static_balancer = StaticLoadBalancer::new(self.endpoint.clone());
        let empty_connection_manager = GrpcConnectionManager::new(
            SharedLoadBalancer::new_with_balancer(Box::new(static_balancer)),
            self.database.clone(),
            MultiInterceptor::new(),
        );

        let mut auth_client = empty_connection_manager
            .get_auth_service(RawAuthClient::new)
            .await
            .unwrap();

        // TODO: add configurable authorization request timeout
        let raw_request = RawLoginRequest {
            operation_params: TimeoutSettings::default().operation_params(),
            user: self.username.clone(),
            password: self.password.clone(),
        };

        let raw_result = auth_client.login(raw_request).await?;
        Ok(raw_result.token)
    }

    pub fn new(username: String, password: String, endpoint: Uri, database: String) -> Self {
        Self {
            username,
            password,
            database,
            endpoint,
        }
    }
}

impl Credentials for StaticCredentialsAuth {
    #[tokio::main(flavor = "current_thread")]
    async fn create_token(&self) -> YdbResult<TokenInfo> {
        Ok(TokenInfo::token(self.acquire_token().await?))
    }
}
