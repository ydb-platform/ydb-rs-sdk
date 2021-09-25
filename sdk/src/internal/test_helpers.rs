use std::sync::Mutex;

use once_cell::sync::Lazy;

use crate::credentials::CommandLineYcToken;

pub(crate) static CRED: Lazy<Mutex<CommandLineYcToken>> =
    Lazy::new(|| Mutex::new(crate::credentials::CommandLineYcToken::new()));

pub(crate) static START_ENDPOINT: Lazy<String> =
    Lazy::new(|| std::env::var("DB_ENDPOINT").unwrap());
pub(crate) static DATABASE: Lazy<String> = Lazy::new(|| std::env::var("DB_NAME").unwrap());
