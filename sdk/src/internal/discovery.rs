use crate::errors::Result;

pub(crate) trait Discovery {
    fn endpoint(self: &Self) -> Result<String>;
}

pub(crate) struct StaticDiscovery {
    pub endpoint: String,
}

impl Discovery for StaticDiscovery {
    fn endpoint(self: &Self) -> Result<String> {
        return Ok(self.endpoint.clone());
    }
}
