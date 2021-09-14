pub trait Credencials: Clone {
    fn fill_token(self: &Self, token: &mut String);
}

#[derive(Clone)]
pub struct StaticToken {
    token: String,
}

impl StaticToken {
    pub fn from(token: String) -> Self {
        return StaticToken { token };
    }
}

impl Credencials for StaticToken {
    fn fill_token(self: &Self, token: &mut String) {
        token.replace_range(.., self.token.as_str());
    }
}
