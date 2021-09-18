use crate::errors::{Error, Result};
use dyn_clone::DynClone;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

pub trait Credentials: Debug + DynClone + Send {
    fn create_token(self: &mut Self) -> Result<String>;
}
dyn_clone::clone_trait_object!(Credentials);

#[derive(Debug, Clone)]
pub struct StaticToken {
    token: String,
}

impl StaticToken {
    #[allow(unused)]
    pub fn from(token: &str) -> Self {
        return StaticToken {
            token: token.to_string(),
        };
    }
}

impl Credentials for StaticToken {
    fn create_token(self: &mut Self) -> Result<String> {
        return Ok(self.token.clone());
    }
}

#[derive(Clone, Debug)]
pub struct CommandLineYcToken {
    token: Arc<RwLock<String>>,
}

impl CommandLineYcToken {
    pub fn new() -> Self {
        return CommandLineYcToken {
            token: Arc::new(RwLock::new("".to_string())),
        };
    }
}

impl Credentials for CommandLineYcToken {
    fn create_token(self: &mut Self) -> Result<String> {
        {
            let token = self.token.read()?;
            if token.as_str() != "" {
                return Ok(token.clone());
            }
        }
        {
            let mut token = self.token.write()?;
            if token.as_str() != "" {
                return Ok(token.clone());
            }
            let result = std::process::Command::new("yc")
                .args(["iam", "create-token"])
                .output()?;
            if !result.status.success() {
                let err = String::from_utf8(result.stderr)?;
                return Err(Error::Custom(format!(
                    "can't execute yc ({}): {}",
                    result.status.code().unwrap(),
                    err
                )));
            }
            *token = String::from_utf8(result.stdout)?.trim().to_string();
            return Ok(token.clone());
        }
    }
}
