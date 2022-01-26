use crate::errors::{YdbError, YdbResult};
use dyn_clone::DynClone;
use std::fmt::Debug;
use std::process::Command;
use std::sync::{Arc, Mutex, RwLock};

pub trait Credentials: Debug + DynClone + Send + Sync {
    fn create_token(self: &mut Self) -> YdbResult<String>;
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
    fn create_token(self: &mut Self) -> YdbResult<String> {
        return Ok(self.token.clone());
    }
}

#[derive(Clone, Debug)]
pub struct CommandLineYcToken {
    token: Arc<RwLock<String>>,
    command: Arc<Mutex<Command>>,
}

impl CommandLineYcToken {
    #[allow(dead_code)]
    pub fn from_string_cmd(cmd: &str) -> YdbResult<Self> {
        let cmd_parts: Vec<&str> = cmd.split_whitespace().collect();

        if cmd_parts.len() < 1 {
            return Err(YdbError::Custom(
                format!("can't split get token command: '{}'", cmd).into(),
            ));
        }

        let mut command = Command::new(cmd_parts[0]);
        command.args(&cmd_parts.as_slice()[1..]);

        return Ok(CommandLineYcToken {
            token: Arc::new(RwLock::new("".to_string())),
            command: Arc::new(Mutex::new(command)),
        });
    }
}

impl Credentials for CommandLineYcToken {
    fn create_token(self: &mut Self) -> YdbResult<String> {
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
            let result = self.command.lock()?.output()?;
            if !result.status.success() {
                let err = String::from_utf8(result.stderr)?;
                return Err(YdbError::Custom(format!(
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
