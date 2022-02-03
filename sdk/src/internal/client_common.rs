use crate::credentials::CredentialsRef;
use crate::errors::YdbResult;
use crate::pub_traits::{Credentials, TokenInfo};
use std::ops::DerefMut;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::thread;
use std::time::Instant;

#[derive(Clone, Debug)]
pub(crate) struct DBCredentials {
    pub database: String,
    pub token_cache: TokenCache,
}

#[derive(Debug)]
struct TokenCacheState {
    pub credentials: CredentialsRef,
    token_info: TokenInfo,
    token_renewing: Arc<Mutex<()>>,
}

#[derive(Clone, Debug)]
pub(crate) struct TokenCache(Arc<RwLock<TokenCacheState>>);

impl TokenCache {
    pub fn new(mut credentials: CredentialsRef) -> YdbResult<Self> {
        let token_info = credentials.create_token()?;
        Ok(TokenCache(Arc::new(RwLock::new(TokenCacheState {
            credentials,
            token_info,
            token_renewing: Arc::new(Mutex::new(())),
        }))))
    }

    pub fn token(&self) -> String {
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
        let mut renew_arc = self.0.read().unwrap().token_renewing.clone();
        let _renew_lock = if let Ok(lock) = renew_arc.try_lock() {
            lock
        } else {
            // other renew in process
            return;
        };

        let mut cred = { self.0.write().unwrap().credentials.clone() };

        let res = std::thread::spawn(move || cred.create_token())
            .join()
            .unwrap();

        // match cred.create_token() {
        match res {
            Ok(token_info) => {
                println!("token renewed");
                let mut write = self.0.write().unwrap();
                write.token_info = token_info
            }
            Err(err) => {
                println!("renew token error: {}", err)
            }
        };
    }
}
