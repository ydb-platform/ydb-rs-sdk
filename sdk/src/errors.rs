use std::fmt::{Debug, Display, Formatter};

#[derive(Debug)]
pub enum Error {
    Custom(String)
}

impl Error {
    pub fn from_str(s: &str)->Error{
        return Error::Custom(s.to_string())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self::Debug::fmt(self, f)
    }
}

impl std::error::Error for Error {

}