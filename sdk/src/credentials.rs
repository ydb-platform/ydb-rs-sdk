use crate::errors::{Error, Result};
use dyn_clone::DynClone;
use std::sync::{Arc, RwLock};

pub trait Credencials: DynClone {
    fn create_token(self: &mut Self) -> Result<Arc<String>>;
}
dyn_clone::clone_trait_object!(Credencials);

#[derive(Clone)]
pub struct StaticToken {
    token: Arc<String>,
}

impl StaticToken {
    #[allow(unused)]
    pub fn from(token: &str) -> Self {
        return StaticToken {
            token: Arc::new(token.to_string()),
        };
    }
}

impl Credencials for StaticToken {
    fn create_token(self: &mut Self) -> Result<Arc<String>> {
        return Ok(self.token.clone());
    }
}

#[derive(Clone)]
pub struct CommandLineYcToken {
    token: Arc<RwLock<Arc<String>>>,
}

impl CommandLineYcToken {
    pub fn new() -> Self {
        return CommandLineYcToken {
            token: Arc::new(RwLock::new(Arc::new("".to_string()))),
        };
    }
}

impl Credencials for CommandLineYcToken {
    fn create_token(self: &mut Self) -> Result<Arc<String>> {
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
            *token = Arc::new(String::from_utf8(result.stdout)?.trim().to_string());

            println!("rekby-token: got");
            Ok(token.clone())
        }
    }
}
