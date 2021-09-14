use crate::credentials::Credencials;
use std::task::{Context, Poll};
use tonic::transport::Channel;

pub(crate) struct AuthService<C>
where
    C: crate::credentials::Credencials,
{
    ch: Channel,
    cred: C,
    database: String,
}

impl<C> AuthService<C>
where
    C: crate::credentials::Credencials,
{
    pub fn new(ch: Channel, cred: C, database: &str) -> Self<C> {
        return AuthService {
            ch,
            cred,
            database: database.to_string(),
        };
    }
}

impl tower::Service<tower::Request> for AuthService<C>
where
    C: Credencials,
{
    type Response = ();
    type Error = ();
    type Future = ();

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.ch.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: tower::Request) -> Self::Future {
        let mut token = String::new();
        self.cred.fill_token(&mut token);

        let token = MetadataValue::from_str(token.as_str()).unwrap();
        let database = MetadataValue::from_str(self.database.as_str()).unwrap();

        println!("rekby-auth");
        req.metadata_mut().insert("x-ydb-auth-ticket", token);
        req.metadata_mut().insert("x-ydb-database", database);
    }
}
