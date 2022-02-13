use crate::connection_info::ConnectionInfo;
use once_cell::sync::Lazy;

pub(crate) static CONNECTION_INFO: Lazy<ConnectionInfo> = Lazy::new(|| {
    std::env::var("YDB_CONNECTION_STRING")
        .unwrap()
        .as_str()
        .parse()
        .unwrap()
});
