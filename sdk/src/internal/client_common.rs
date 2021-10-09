use crate::credentials::Credentials;

#[derive(Clone, Debug)]
pub(crate) struct DBCredentials {
    pub database: String,
    pub credentials: Box<dyn Credentials>,
}
