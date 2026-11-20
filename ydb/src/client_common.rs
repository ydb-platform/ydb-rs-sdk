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
use tracing::{trace, warn};

#[derive(Clone, Debug)]
pub(crate) struct DBCredentials {
    pub(crate) database: String,
    pub(crate) token_cache: TokenCache,
}

/// Caches the token the credentials create, renewing it in the background.
///
/// A renewal retries as the client's [`RetrySettings`] say, so a client built
/// with a deadline, or without retries, learns that its credentials cannot
/// create a token instead of waiting for one for ever.
#[derive(Clone, Debug)]
pub(crate) struct TokenCache {
    pub(crate) credentials: CredentialsRef,
    retry_settings: RetrySettings,
    renewing_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    token_info_sender: watch::Sender<Option<YdbResult<TokenInfo>>>,
}

impl TokenCache {
    pub(crate) fn new(credentials: CredentialsRef, retry_settings: RetrySettings) -> Self {
        let (token_info_sender, _receiver) = watch::channel(None);
        let initial_renew_task = {
            let credentials = credentials.clone();
            let retry_settings = retry_settings.clone();
            let sender = token_info_sender.clone();

            tokio::spawn(async move {
                Self::renew_token_async(credentials, retry_settings, sender).await
            })
        };

        TokenCache {
            renewing_task: Arc::new(Mutex::new(Some(initial_renew_task))),
            token_info_sender,
            credentials,
            retry_settings,
        }
    }

    pub(crate) fn token(&self) -> YdbResult<SecretString> {
        let now = Instant::now();

        let Some(token_info) = self.token_info_sender.borrow().clone() else {
            return Err(YdbError::InternalError(
                "token cache is not initialized yet".to_owned(),
            ));
        };
        // A failed renewal is tried again on the next request, so a failure
        // does not outlive the retry budget it exhausted.
        let need_renew = match &token_info {
            Ok(token_info) => now > token_info.next_renew,
            Err(_) => true,
        };
        if need_renew {
            // if need renew and no renew background in process
            let mut renewing_task = self.renewing_task.lock()?;
            if renewing_task.as_ref().is_none_or(|task| task.is_finished()) {
                *renewing_task = Some(self.renew_token_in_background());
            };
        }
        Ok(token_info?.token)
    }

    fn renew_token_in_background(&self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(Self::renew_token_async(
            self.credentials.clone(),
            self.retry_settings.clone(),
            self.token_info_sender.clone(),
        ))
    }

    async fn renew_token_async(
        credentials: CredentialsRef,
        retry_settings: RetrySettings,
        sender: watch::Sender<Option<YdbResult<TokenInfo>>>,
    ) {
        let result = retry_settings
            .retry_on_retriable_errors(
                Idempotency::Idempotent,
                closure!([credentials], async |_| {
                    let creds = credentials.clone();
                    tokio::task::spawn_blocking(move || creds.create_token()).await?
                }),
            )
            .await;

        match result {
            Ok(token_info) => {
                trace!("token renewed");
                sender.send_replace(Some(Ok(token_info)));
            }
            Err(err) => {
                warn!("renew token error: {}", err);
                // A token already created is kept until a renewal succeeds,
                // as `Credentials::create_token` promises; only a cache with
                // no token holds the error, for its waiters to report.
                sender.send_if_modified(|token_info| {
                    if matches!(token_info, Some(Ok(_))) {
                        false
                    } else {
                        *token_info = Some(Err(err));
                        true
                    }
                });
            }
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
mod test {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use secrecy::ExposeSecret;

    use super::TokenCache;
    use crate::credentials::credentials_ref;
    use crate::errors::YdbResult;
    use crate::pub_traits::{Credentials, TokenInfo};
    use crate::waiter::Waiter;
    use crate::{RetrySettings, YdbError};

    /// How long a test waits for a renewal before it fails.
    const RENEWAL_DEADLINE: Duration = Duration::from_secs(5);

    /// Credentials answering each call with the next of `outcomes`, then
    /// with its last one.
    struct ScriptedCredentials {
        calls: AtomicUsize,
        outcomes: Vec<Result<&'static str, &'static str>>,
        renew_now: bool,
    }

    impl ScriptedCredentials {
        fn new(outcomes: Vec<Result<&'static str, &'static str>>) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                outcomes,
                renew_now: false,
            }
        }

        /// Every token asks to be renewed at once.
        fn renewed_at_once(mut self) -> Self {
            self.renew_now = true;
            self
        }
    }

    impl Credentials for ScriptedCredentials {
        fn create_token(&self) -> YdbResult<TokenInfo> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            let outcome = self
                .outcomes
                .get(call)
                .or(self.outcomes.last())
                .copied()
                .unwrap_or(Err("no outcome scripted"));
            match outcome {
                Ok(token) if self.renew_now => {
                    Ok(TokenInfo::token(token.to_owned()).with_renew(Instant::now()))
                }
                Ok(token) => Ok(TokenInfo::token(token.to_owned())),
                // An error retried for an idempotent call, as a transport
                // failure such as an untrusted certificate is.
                Err(message) => Err(YdbError::TransportGRPCStatus(Arc::new(
                    tonic::Status::unknown(message),
                ))),
            }
        }
    }

    /// Wait for the renewal `cache` has in flight, the one it started last.
    async fn finish_renewal(cache: &TokenCache) {
        let task = cache
            .renewing_task
            .lock()
            .expect("the lock is not poisoned")
            .take()
            .expect("a renewal was started");
        tokio::time::timeout(RENEWAL_DEADLINE, task)
            .await
            .expect("the renewal finishes before the deadline")
            .expect("the renewal does not panic");
    }

    #[tokio::test]
    async fn a_renewal_failing_within_the_retry_budget_reaches_its_waiters() {
        let cache = TokenCache::new(
            credentials_ref(ScriptedCredentials::new(vec![Err("untrusted certificate")])),
            RetrySettings::dont_retry(),
        );

        let waited = tokio::time::timeout(RENEWAL_DEADLINE, cache.wait())
            .await
            .expect("a client without retries is told rather than kept waiting");

        let err = waited.expect_err("the credentials created no token");
        assert!(
            err.to_string().contains("untrusted certificate"),
            "the waiter sees why: {err}"
        );
    }

    #[tokio::test]
    async fn a_failed_renewal_is_tried_again_on_the_next_request() {
        let cache = TokenCache::new(
            credentials_ref(ScriptedCredentials::new(vec![
                Err("transient failure"),
                Ok("token"),
            ])),
            RetrySettings::dont_retry(),
        );
        finish_renewal(&cache).await;

        let failed = cache.token();
        finish_renewal(&cache).await;

        assert!(failed.is_err(), "the request meets the failure");
        let token = cache
            .token()
            .expect("the renewal the request started succeeds");
        assert_eq!(token.expose_secret(), "token");
    }

    #[tokio::test]
    async fn a_failed_renewal_keeps_the_token_already_created() {
        let cache = TokenCache::new(
            credentials_ref(
                ScriptedCredentials::new(vec![Ok("first"), Err("transient failure")])
                    .renewed_at_once(),
            ),
            RetrySettings::dont_retry(),
        );
        finish_renewal(&cache).await;

        let due = cache.token().expect("the first token is cached");
        finish_renewal(&cache).await;

        assert_eq!(due.expose_secret(), "first");
        let kept = cache
            .token()
            .expect("a failed renewal keeps the token already created");
        assert_eq!(kept.expose_secret(), "first");
    }
}
