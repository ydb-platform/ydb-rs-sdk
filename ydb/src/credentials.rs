use crate::client::TimeoutSettings;
use crate::errors::{YdbError, YdbResult};
use crate::grpc_connection_manager::GrpcConnectionManager;
use crate::grpc_wrapper::raw_auth_service::client::RawAuthClient;
use crate::grpc_wrapper::raw_auth_service::login::RawLoginRequest;
use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
use crate::load_balancer::{SharedLoadBalancer, StaticLoadBalancer};
use crate::pub_traits::{Credentials, TokenInfo};
use chrono::DateTime;
use http::Uri;

use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::env;
use std::fmt::Debug;
use std::ops::Add;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, trace};

const YDB_ANONYMOUS_CREDENTIALS: &str = "YDB_ANONYMOUS_CREDENTIALS";
const YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS: &str = "YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS";
const YDB_METADATA_CREDENTIALS: &str = "YDB_METADATA_CREDENTIALS";
const YDB_ACCESS_TOKEN_CREDENTIALS: &str = "YDB_ACCESS_TOKEN_CREDENTIALS";

const YC_METADATA_URL: &str =
    "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token";
const GCE_METADATA_URL: &str =
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

const EMPTY_TOKEN: &str = "";

#[deprecated(note = "use AccessTokenCredentials instead")]
pub type StaticToken = AccessTokenCredentials;
#[deprecated(note = "use CommandLineCredentials instead")]
pub type CommandLineYcToken = CommandLineCredentials;
#[deprecated(note = "use StaticCredentials instead")]
pub type StaticCredentialsAuth = StaticCredentials;
#[deprecated(note = "use MetadataUrlCredentials instead")]
pub type YandexMetadata = MetadataUrlCredentials;

pub(crate) type CredentialsRef = Arc<Box<dyn Credentials>>;

pub(crate) fn credencials_ref<T: 'static + Credentials>(cred: T) -> CredentialsRef {
    Arc::new(Box::new(cred))
}

/// Get token of service account of instance
///
/// Yandex cloud support GCE token compatible. Use it.
/// Example:
/// ```
/// use ydb::MetadataUrlCredentials;
/// let cred = MetadataUrlCredentials::new();
/// ```
pub struct MetadataUrlCredentials {
    inner: GCEMetadata,
}

impl MetadataUrlCredentials {
    pub fn new() -> Self {
        Self {
            inner: GCEMetadata::from_url(YC_METADATA_URL).unwrap(),
        }
    }



    /// Create GCEMetadata with custom url (may need for debug or spec infrastructure with non standard metadata)
    ///
    /// Example:
    /// ```
    /// # use ydb::YdbResult;
    /// # fn main()->YdbResult<()>{
    /// use ydb::MetadataUrlCredentials;
    /// let cred = MetadataUrlCredentials::from_url("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token")?;
    /// # return Ok(());
    /// # }
    /// ```
    pub fn from_url<T: Into<String>>(url: T) -> YdbResult<Self> {
        Ok(Self {
            inner: GCEMetadata::from_url(url)?,
        })
    }
}

impl Default for MetadataUrlCredentials {
    fn default() -> Self {
        Self::new()
    }
}

impl Credentials for MetadataUrlCredentials {
    fn create_token(&self) -> YdbResult<TokenInfo> {
        self.inner.create_token()
    }
}

pub struct AnonymousCredentials {
    inner: AccessTokenCredentials,
}

impl AnonymousCredentials {
    pub fn new() -> Self {
        Self {
            inner: AccessTokenCredentials::from(EMPTY_TOKEN),
        }
    }
}

impl Default for AnonymousCredentials {
    fn default() -> Self {
        Self::new()
    }
}

impl Credentials for AnonymousCredentials {
    fn create_token(&self) -> YdbResult<TokenInfo> {
        self.inner.create_token()
    }
}

pub struct FromEnvCredentials {
    inner: Box<dyn Credentials>,
}

/// Select credentials from environment
/// reference: https://ydb.tech/docs/en/reference/ydb-sdk/auth
impl FromEnvCredentials {
    pub fn new() -> YdbResult<Self> {
        Ok(Self {
            inner: get_credentials_from_env()?,
        })
    }
}

impl Credentials for FromEnvCredentials {
    fn create_token(&self) -> YdbResult<TokenInfo> {
        self.inner.create_token()
    }
}

fn get_credentials_from_env() -> YdbResult<Box<dyn Credentials>> {
    if let Ok(file_creds) = env::var(YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS) {
        return Ok(Box::new(ServiceAccountCredentials::from_file(file_creds)?));
    }

    if let Ok(v) = env::var(YDB_ANONYMOUS_CREDENTIALS) {
        if v == "1" {
            return Ok(Box::new(
                // anonymous credentials is empty token
                AnonymousCredentials::new(),
            ));
        }
    }

    if let Ok(v) = env::var(YDB_METADATA_CREDENTIALS) {
        if v == "1" {
            return Ok(Box::new(MetadataUrlCredentials::new()));
        }
    }

    if let Ok(token) = env::var(YDB_ACCESS_TOKEN_CREDENTIALS) {
        return Ok(Box::new(AccessTokenCredentials::from(token)));
    }

    Ok(Box::new(MetadataUrlCredentials::new()))
}

/// Credentials with static token without renewing
///
/// Example:
/// ```no_run
/// # use ydb::{ClientBuilder, AccessTokenCredentials, YdbResult};
/// # fn main()->YdbResult<()>{
/// let builder = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=/local")?;
/// let client = builder.with_credentials(AccessTokenCredentials::from("asd")).client()?;
/// # return Ok(());
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct AccessTokenCredentials {
    pub(crate) token: String,
}

impl AccessTokenCredentials {
    /// Create static token from string
    ///
    /// Example:
    /// ```
    /// # use ydb::AccessTokenCredentials;
    /// AccessTokenCredentials::from("asd");
    /// ```
    pub fn from<T: Into<String>>(token: T) -> Self {
        AccessTokenCredentials {
            token: token.into(),
        }
    }
}

impl Credentials for AccessTokenCredentials {
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
/// use ydb::CommandLineCredentials;
///
/// let cred = CommandLineCredentials::from_cmd("yc iam create-token").unwrap();
/// ```
#[derive(Debug)]
pub struct CommandLineCredentials {
    command: Arc<Mutex<Command>>,
}

impl CommandLineCredentials {
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

        Ok(CommandLineCredentials {
            command: Arc::new(Mutex::new(command)),
        })
    }
}

impl Credentials for CommandLineCredentials {
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
                let token = token_info.token.expose_secret();
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

/// Get service account credentials instance
/// service account key should be:
/// - in the local file and YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS environment variable should point to it
/// - in the local file and it's path is specified
/// - in the json format string
///
/// Example:
/// ```
/// use ydb::ServiceAccountCredentials;
/// let cred = ServiceAccountCredentials::from_env();
/// ```
/// or
/// ```
/// use ydb::ServiceAccountCredentials;
/// let json = "....";
/// let cred = ServiceAccountCredentials::from_json(json);
/// ```
/// or
/// ```
/// use ydb::ServiceAccountCredentials;
/// let cred = ServiceAccountCredentials::new("service_account_id", "key_id", "private_key");
/// ```
/// or
/// ```
/// use ydb::ServiceAccountCredentials;
/// let cred = ServiceAccountCredentials::from_file("/path/to/file");
/// ```
pub struct ServiceAccountCredentials {
    audience_url: String,
    private_key: SecretString,
    service_account_id: String,
    key_id: String,
}

impl ServiceAccountCredentials {
    pub fn new(
        service_account_id: impl Into<String>,
        key_id: impl Into<String>,
        private_key: impl Into<String>,
    ) -> Self {
        Self {
            audience_url: Self::IAM_TOKEN_DEFAULT.to_string(),
            private_key: SecretString::new(private_key.into()),
            service_account_id: service_account_id.into(),
            key_id: key_id.into(),
        }
    }

    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.audience_url = url.into();
        self
    }

    pub fn from_env() -> YdbResult<Self> {
        let path = std::env::var(YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS)?;

        ServiceAccountCredentials::from_file(path)
    }

    pub fn from_file(path: impl AsRef<std::path::Path>) -> YdbResult<Self> {
        let json_key = std::fs::read_to_string(path)?;
        ServiceAccountCredentials::from_json(&json_key)
    }

    pub fn from_json(json_key: &str) -> YdbResult<Self> {
        #[derive(Debug, Serialize, Deserialize)]
        struct JsonKey {
            public_key: String,
            private_key: String,
            service_account_id: String,
            id: String,
        }

        let key: JsonKey = serde_json::from_str(json_key)?;

        Ok(Self {
            audience_url: Self::IAM_TOKEN_DEFAULT.to_string(),
            key_id: key.id,
            service_account_id: key.service_account_id,
            private_key: SecretString::new(key.private_key),
        })
    }

    const IAM_TOKEN_DEFAULT: &'static str = "https://iam.api.cloud.yandex.net/iam/v1/tokens";
    const JWT_TOKEN_LIFE_TIME: usize = 720; // max 3600

    fn build_jwt(&self) -> YdbResult<String> {
        let private_key = self.private_key.expose_secret().as_bytes();

        #[derive(Debug, Serialize, Deserialize)]
        struct Claims {
            aud: String, // Optional. Audience
            exp: usize, // Required (validate_exp defaults to true in validation). Expiration time (as UTC timestamp)
            iat: usize, // Optional. Issued at (as UTC timestamp)
            iss: String, // Optional. Issuer
        }

        let iat = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs() as usize;

        let mut header = Header::new(Algorithm::PS256);
        header.kid = Some(self.key_id.clone());
        header.alg = Algorithm::PS256;
        header.typ = Some("JWT".to_string());

        let claims = Claims {
            exp: iat + Self::JWT_TOKEN_LIFE_TIME,
            aud: self.audience_url.clone(),
            iat,
            iss: self.service_account_id.clone(),
        };
        let token = encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(private_key).map_err(|e| YdbError::custom(e.to_string()))?,
        )
        .map_err(|e| YdbError::custom(format!("can't build jwt: {}", e)))?;

        debug!("Token was built");
        Ok(token)
    }

    fn get_renew_time_for_lifetime(time: chrono::DateTime<chrono::Utc>) -> Instant {
        let duration = time - chrono::Utc::now();
        let seconds = (0.1 * duration.num_seconds() as f64) as u64;
        trace!("renew in: {}", seconds);

        Instant::now() + Duration::from_secs(seconds)
    }
}

impl Credentials for ServiceAccountCredentials {
    fn create_token(&self) -> YdbResult<TokenInfo> {
        use chrono::Utc;
        #[derive(Deserialize)]
        struct TokenResponse {
            #[serde(rename = "iamToken")]
            iam_token: String,
            #[serde(rename = "expiresAt")]
            expires_at: DateTime<Utc>,
        }

        #[derive(Serialize)]
        struct TokenRequest {
            jwt: String,
        }

        let jwt = self.build_jwt()?;

        let req = TokenRequest { jwt };
        let client = reqwest::blocking::Client::new();
        let res: TokenResponse = client
            .request(reqwest::Method::POST, self.audience_url.clone())
            .json(&req)
            .send()?
            .json()?;

        Ok(TokenInfo::token(format!("Bearer {}", res.iam_token))
            .with_renew(Self::get_renew_time_for_lifetime(res.expires_at)))
    }
}

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
        Self::from_url(GCE_METADATA_URL).unwrap()
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

pub struct StaticCredentials {
    username: String,
    password: SecretString,
    database: String,
    endpoint: Uri,
    cert_path: Option<String>,
}

impl StaticCredentials {
    pub async fn acquire_token(&self) -> YdbResult<String> {
        let static_balancer = StaticLoadBalancer::new(self.endpoint.clone());
        let empty_connection_manager = GrpcConnectionManager::new(
            SharedLoadBalancer::new_with_balancer(Box::new(static_balancer)),
            self.database.clone(),
            MultiInterceptor::new(),
            self.cert_path.clone(),
        );

        let mut auth_client = empty_connection_manager
            .get_auth_service(RawAuthClient::new)
            .await
            .unwrap();

        // TODO: add configurable authorization request timeout
        let raw_request = RawLoginRequest {
            operation_params: TimeoutSettings::default().operation_params(),
            user: self.username.clone(),
            password: self.password.expose_secret().clone(),
        };

        let raw_response = auth_client.login(raw_request).await?;
        Ok(raw_response.token)
    }

    pub fn new(username: String,
        password: String,
        endpoint: Uri, database: String) -> Self {
        Self {
            username,
            password: SecretString::new(password),
            database,
            endpoint,
            cert_path: None,
        }
    }

    pub fn new_with_ca(username: String,
        password: String,
        endpoint: Uri, database: String, cert_path: String) -> Self {
        Self {
            username,
            password: SecretString::new(password),
            database,
            endpoint,
            cert_path: Some(cert_path),
        }
    }
}

impl Credentials for StaticCredentials {
    #[tokio::main(flavor = "current_thread")]
    async fn create_token(&self) -> YdbResult<TokenInfo> {
        Ok(TokenInfo::token(self.acquire_token().await?))
    }
}
