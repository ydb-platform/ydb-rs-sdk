use crate::errors::Result;

pub(crate) struct Session {}

pub(crate) trait SessionPool {
    fn get_session() -> Result<Session>;
}

pub(crate) struct SimpleSessionPool {}
