use strum::{Display, EnumIter, EnumString};

pub(crate) trait GrpcServiceForDiscovery {
    fn get_grpc_discovery_service() -> Service;
}

#[allow(dead_code)]
#[derive(Clone, Copy, Display, Debug, EnumIter, EnumString, Eq, Hash, PartialEq)]
pub(crate) enum Service {
    #[strum(serialize = "discovery")]
    Discovery,

    #[strum(serialize = "export")]
    Export,

    #[strum(serialize = "import")]
    Import,

    #[strum(serialize = "scripting")]
    Scripting,

    #[strum(serialize = "table_service")]
    Table,

    #[strum(serialize = "scheme_service")]
    Scheme,
}
