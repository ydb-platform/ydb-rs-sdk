#[derive(Debug)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub struct SchemeEntry {
    pub name: String,
    pub owner: String,
    pub r#type: SchemeEntryType,
    pub effective_permissions: Vec<SchemePermissions>,
    pub permissions: Vec<SchemePermissions>,
    pub size_bytes: u64,
}

#[derive(Debug)]
#[cfg_attr(not(feature = "force-exhaustive-all"), non_exhaustive)]
pub enum SchemeEntryType {
    Unspecified,
    Directory,
    Table,
    ColumnStrore,
    ColumnTable,
    PersQueueGroup,
    Database,
    RtmrVolume,
    BlockStoreVolume,
    CoordinationNode,
    Sequence,
    Replication,
    Topic,
    ExternalDataSource,
    ExternalTable,
    View,
    Unknown(i32),
}

#[derive(Debug)]
pub struct SchemePermissions {
    pub subject: String,
    pub permission_names: Vec<String>,
}
