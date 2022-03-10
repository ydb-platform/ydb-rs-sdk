use crate::credentials::CredentialsRef;
use crate::errors::YdbResult;
use crate::waiter::Waiter;
use crate::pub_traits::TokenInfo;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use tokio::sync::watch;
use tracing::trace;

#[derive(Clone, Debug)]
pub(crate) struct DBCredentials {
    pub(crate) database: String,
    pub(crate) token_cache: TokenCache,
}

#[derive(Debug)]
struct TokenCacheState {
    pub(crate) credentials: CredentialsRef,
    token_info: TokenInfo,
    token_renewing: Arc<Mutex<()>>,
    token_received: watch::Receiver<bool>,
    token_received_sender: watch::Sender<bool>,
}

#[derive(Clone, Debug)]
pub(crate) struct TokenCache(Arc<RwLock<TokenCacheState>>);

impl TokenCache {
    pub(crate) fn new(credentials: CredentialsRef) -> YdbResult<Self> {
        let (token_received_sender, token_received) = watch::channel(false);
        let token_cache = TokenCache(Arc::new(RwLock::new(TokenCacheState {
            credentials,
            token_info: TokenInfo::token("".to_string()),
            token_renewing: Arc::new(Mutex::new(())),
            token_received,
            token_received_sender,
        })));
        let token_cache_clone = token_cache.clone();
        tokio::task::spawn_blocking(move || token_cache_clone.renew_token_blocking());
        return Ok(token_cache);
    }

    pub(crate) fn token(&self) -> String {
        let now = Instant::now();

        let read = self.0.read().unwrap();
        if now > read.token_info.next_renew {
            // if need renew and no renew background in process
            if let Ok(_) = read.token_renewing.try_lock() {
                let self_clone = self.clone();
                tokio::task::spawn_blocking(move || self_clone.renew_token_blocking());
            };
        };
        return read.token_info.token.clone();
    }

    fn renew_token_blocking(self) {
        let renew_arc = self.0.read().unwrap().token_renewing.clone();
        let _renew_lock = if let Ok(lock) = renew_arc.try_lock() {
            lock
        } else {
            // other renew in process
            return;
        };

        let cred = { self.0.write().unwrap().credentials.clone() };

        let res = std::thread::spawn(move || cred.create_token())
            .join()
            .unwrap();

        // match cred.create_token() {
        match res {
            Ok(token_info) => {
                trace!("token renewed");
                let mut write = self.0.write().unwrap();
                write.token_info = token_info;
                if let Err(_) = write.token_received_sender.send(true) {
                    trace!("send token channel closed");
                    return;
                }
            }
            Err(err) => {
                trace!("renew token error: {}", err)
            }
        };
    }
}

#[async_trait::async_trait]
impl Waiter for TokenCache {
    async fn wait(&self) -> YdbResult<()> {
        let mut ch = self.0.read().unwrap().token_received.clone();
        loop {
            let received = *ch.borrow_and_update();
            if received {
                return Ok(());
            }

            ch.changed().await?;
        }
    }
}
