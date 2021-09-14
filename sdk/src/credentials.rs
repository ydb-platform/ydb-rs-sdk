use dyn_clone::DynClone;

pub trait Credencials: DynClone {
    fn fill_token(self: &Self, token: &mut String);
}
dyn_clone::clone_trait_object!(Credencials);

#[derive(Clone)]
pub struct StaticToken {
    token: String,
}

impl StaticToken {
    pub fn from(token: &str) -> Self {
        return StaticToken {
            token: token.to_string(),
        };
    }
}

impl Credencials for StaticToken {
    fn fill_token(self: &Self, token: &mut String) {
        token.replace_range(.., self.token.as_str());
    }
}
