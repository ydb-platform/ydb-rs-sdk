use crate::credentials::Credentials;

#[derive(Clone)]
pub(crate) struct DBCredentials {
    pub database: String,
    pub credentials: Box<dyn Credentials>,
}
