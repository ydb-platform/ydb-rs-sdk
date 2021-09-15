use crate::errors::Result;
use dyn_clone::DynClone;
use std::sync::Arc;

pub trait Credencials: DynClone {
    fn create_token(self: &mut Self) -> Result<Arc<String>>;
}
dyn_clone::clone_trait_object!(Credencials);

#[derive(Clone)]
pub struct StaticToken {
    token: Arc<String>,
}

impl StaticToken {
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
