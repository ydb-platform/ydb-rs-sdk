use std::sync::Mutex;

use once_cell::sync::Lazy;
use crate::connection_info::ConnectionInfo;

use crate::credentials::CommandLineYcToken;

pub(crate) static CONNECTION_INFO: Lazy<ConnectionInfo> = Lazy::new(||
    ConnectionInfo::parse(std::env::var("YDB_CONNECTION_STRING").unwrap().as_str()).unwrap()
);
