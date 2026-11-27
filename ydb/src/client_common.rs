use crate::RetrySettings;
use crate::YdbError;
use crate::closure;
use crate::credentials::CredentialsRef;
use crate::errors::{FlattenToYdbError, Idempotency, YdbResult};
use crate::pub_traits::TokenInfo;
use crate::waiter::Waiter;
use secrecy::SecretString;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::watch;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::trace;

#[derive(Clone, Debug)]
pub(crate) struct DBCredentials {
    pub(crate) database: String,
    pub(crate) token_cache: TokenCache,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TokenRenewMode {
    Initialize,
    Refresh,
}

#[derive(Clone, Debug)]
pub(crate) struct TokenCache {
    pub(crate) credentials: CredentialsRef,
    renewing_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    token_info_sender: watch::Sender<Option<YdbResult<TokenInfo>>>,
    cancellation: CancellationToken,
    _cancel_on_drop: Arc<DropGuard>,
}

impl TokenCache {
    pub(crate) fn new(credentials: CredentialsRef) -> Self {
        let (token_info_sender, _receiver) = watch::channel(None);
        let cancellation = CancellationToken::new();
        let _cancel_on_drop = Arc::new(cancellation.clone().drop_guard());

        let initial_renew_task = {
            let credentials = credentials.clone();
            let sender = token_info_sender.clone();
            let cancellation = cancellation.clone();

            tokio::spawn(async move {
                Self::renew_token_async(
                    credentials,
                    sender,
                    cancellation,
                    TokenRenewMode::Initialize,
                )
                .await
            })
        };

        TokenCache {
            renewing_task: Arc::new(Mutex::new(Some(initial_renew_task))),
            token_info_sender,
            credentials,
            cancellation,
            _cancel_on_drop,
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
            self.cancellation.clone(),
            TokenRenewMode::Refresh,
        ))
    }

    async fn renew_token_async(
        credentials: CredentialsRef,
        sender: watch::Sender<Option<YdbResult<TokenInfo>>>,
        cancellation: CancellationToken,
        mode: TokenRenewMode,
    ) {
        let result = RetrySettings::with_default_backoff()
            .with_deadline(cancellation)
            .retry(closure!([credentials, mode], async |_| {
                let creds = credentials.clone();
                let res = tokio::task::spawn_blocking(move || creds.create_token())
                    .await
                    .flatten_err();
                match res {
                    Ok(token_info) => ControlFlow::Break(Ok(token_info)),
                    Err(err) => {
                        trace!("renew token error: {}", err);
                        // Once a token has been acquired, keep serving it and retry every renewal error.
                        if *mode == TokenRenewMode::Refresh
                            || err.is_retriable(Idempotency::Idempotent)
                        {
                            ControlFlow::Continue(err)
                        } else {
                            ControlFlow::Break(Err(err))
                        }
                    }
                }
            }))
            .await;

        if let ControlFlow::Break(result) = result {
            if result.is_ok() {
                trace!("token renewed");
            }
            sender.send_replace(Some(result));
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Credentials;
    use crate::credentials::credentials_ref;
    use secrecy::ExposeSecret;
    use std::sync::mpsc::{Receiver, Sender, channel};
    use std::time::Duration;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

    #[tokio::test]
    async fn renewal_error_keeps_previous_token_and_recovers() -> YdbResult<()> {
        tokio::time::timeout(Duration::from_secs(1), async {
            let (credentials, responses, mut attempts) = scripted_credentials();

            responses
                .send(Ok(
                    TokenInfo::token("token-a".to_owned()).with_renew(Instant::now())
                ))
                .expect("scripted credential receiver must be open");

            let cache = TokenCache::new(credentials);
            cache.wait().await?;
            attempts
                .recv()
                .await
                .expect("initial token attempt must be recorded");

            assert_eq!(cache.token()?.expose_secret(), "token-a");
            attempts
                .recv()
                .await
                .expect("renewal attempt must be recorded");

            responses
                .send(Err(YdbError::Custom(
                    "temporary malformed response".to_owned(),
                )))
                .expect("scripted credential receiver must be open");
            attempts
                .recv()
                .await
                .expect("retry attempt must be recorded");

            assert_eq!(cache.token()?.expose_secret(), "token-a");

            let mut token_updates = cache.token_info_sender.subscribe();
            responses
                .send(Ok(TokenInfo::token("token-b".to_owned())
                    .with_renew(Instant::now() + Duration::from_secs(60))))
                .expect("scripted credential receiver must be open");
            token_updates
                .wait_for(|state| {
                    matches!(
                        state,
                        Some(Ok(token_info)) if token_info.token.expose_secret() == "token-b"
                    )
                })
                .await?;

            assert_eq!(cache.token()?.expose_secret(), "token-b");
            Ok::<(), YdbError>(())
        })
        .await
        .map_err(|_| YdbError::Custom("token renewal test timed out".to_owned()))?
    }

    #[tokio::test]
    async fn initial_non_retriable_error_is_returned() -> YdbResult<()> {
        let (credentials, responses, _attempts) = scripted_credentials();

        responses
            .send(Err(YdbError::Custom("invalid credentials".to_owned())))
            .expect("scripted credential receiver must be open");

        let cache = TokenCache::new(credentials);
        let error = cache
            .wait()
            .await
            .expect_err("initial non-retriable error must be returned");

        assert!(matches!(error, YdbError::Custom(message) if message == "invalid credentials"));

        Ok(())
    }

    #[tokio::test]
    async fn final_cache_owner_cancels_renewal_tasks() -> YdbResult<()> {
        let (credentials, responses, mut attempts) = scripted_credentials();

        responses
            .send(Ok(
                TokenInfo::token("token".to_owned()).with_renew(Instant::now())
            ))
            .expect("scripted credential receiver must be open");

        let cache = TokenCache::new(credentials);
        cache.wait().await?;
        attempts
            .recv()
            .await
            .expect("initial token attempt must be recorded");

        assert_eq!(cache.token()?.expose_secret(), "token");
        attempts
            .recv()
            .await
            .expect("renewal attempt must be recorded");
        responses
            .send(Err(YdbError::Custom("renewal failed".to_owned())))
            .expect("scripted credential receiver must be open");

        attempts
            .recv()
            .await
            .expect("retry attempt must be recorded");

        let cancellation = cache.cancellation.clone();
        let cache_clone = cache.clone();
        let renewal_task = cache
            .renewing_task
            .lock()?
            .take()
            .ok_or_else(|| YdbError::InternalError("renewal task is missing".to_owned()))?;

        drop(cache);
        assert!(!cancellation.is_cancelled());
        assert!(!renewal_task.is_finished());

        drop(cache_clone);
        assert!(cancellation.is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), renewal_task)
            .await
            .map_err(|_| YdbError::Custom("renewal task did not stop".to_owned()))??;

        Ok(())
    }

    #[derive(Debug)]
    struct ScriptedCredentials {
        responses: Mutex<Receiver<YdbResult<TokenInfo>>>,
        attempt_started: UnboundedSender<()>,
    }

    impl Credentials for ScriptedCredentials {
        fn create_token(&self) -> YdbResult<TokenInfo> {
            self.attempt_started
                .send(())
                .map_err(|_| YdbError::Custom("scripted attempt receiver was closed".to_owned()))?;

            self.responses
                .lock()?
                .recv()
                .map_err(|_| YdbError::Custom("scripted credential channel closed".to_owned()))?
        }
    }

    fn scripted_credentials() -> (
        CredentialsRef,
        Sender<YdbResult<TokenInfo>>,
        UnboundedReceiver<()>,
    ) {
        let (response_sender, responses) = channel();
        let (attempt_started, attempts) = unbounded_channel();
        let credentials = credentials_ref(ScriptedCredentials {
            responses: Mutex::new(responses),
            attempt_started,
        });

        (credentials, response_sender, attempts)
    }
}
