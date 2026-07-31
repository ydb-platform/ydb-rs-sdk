use crate::RetrySettings;
use crate::YdbError;
use crate::closure;
use crate::credentials::CredentialsRef;
use crate::errors::Idempotency;
use crate::errors::YdbResult;
use crate::pub_traits::TokenInfo;
use crate::waiter::Waiter;
use secrecy::SecretString;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::watch;
use tracing::trace;

#[derive(Clone, Debug)]
pub(crate) struct DBCredentials {
    pub(crate) database: String,
    pub(crate) token_cache: TokenCache,
}

#[derive(Clone, Debug)]
pub(crate) struct TokenCache {
    pub(crate) credentials: CredentialsRef,
    renewing_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    token_info_sender: watch::Sender<Option<YdbResult<TokenInfo>>>,
}

impl TokenCache {
    pub(crate) fn new(credentials: CredentialsRef) -> Self {
        let (token_info_sender, _receiver) = watch::channel(None);
        let initial_renew_task = {
            let credentials = credentials.clone();
            let sender = token_info_sender.clone();

            tokio::spawn(async move { Self::renew_token_async(credentials, sender).await })
        };

        TokenCache {
            renewing_task: Arc::new(Mutex::new(Some(initial_renew_task))),
            token_info_sender,
            credentials,
        }
    }

    pub(crate) fn token(&self) -> YdbResult<SecretString> {
        let now = Instant::now();

        let token_info = self.token_info_sender.borrow().clone().unwrap_or_else(|| {
            Err(YdbError::InternalError(
                "token cache is not initialized yet".to_owned(),
            ))
        })?;
        if now > token_info.next_renew {
            // if need renew and no renew background in process
            let mut renewing_task = self.renewing_task.lock()?;
            if renewing_task.as_ref().is_none_or(|task| task.is_finished()) {
                *renewing_task = Some(self.renew_token_in_background());
            };
        }
        Ok(token_info.token)
    }

    fn renew_token_in_background(&self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(Self::renew_token_async(
            self.credentials.clone(),
            self.token_info_sender.clone(),
        ))
    }

    async fn renew_token_async(
        credentials: CredentialsRef,
        sender: watch::Sender<Option<YdbResult<TokenInfo>>>,
    ) {
        let result = RetrySettings::with_default_backoff()
            .retry_on_retriable_errors(
                Idempotency::Idempotent,
                closure!([credentials], async |_| {
                    let creds = credentials.clone();
                    tokio::task::spawn_blocking(move || creds.create_token()).await?
                }),
            )
            .await;

        sender.send_replace(Some(
            result
                .inspect(|_| trace!("token renewed"))
                .inspect_err(|err| {
                    trace!("renew token error: {}", err);
                }),
        ));
    }
}

#[async_trait::async_trait]
impl Waiter for TokenCache {
    async fn wait(&self) -> YdbResult<()> {
        self.token_info_sender
            .subscribe()
            .wait_for(Option::is_some)
            .await?
            .clone()
            .transpose()?;

        Ok(())
    }
}
