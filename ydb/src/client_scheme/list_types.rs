#[derive(Debug)]
pub struct SchemeEntry {
    pub name: String,
    pub owner: String,
    pub r#type: SchemeEntryType,
    pub effective_permissions: Vec<SchemePermissions>,
    pub permissions: Vec<SchemePermissions>,
    pub size_bytes: u64,
}

#[derive(Debug)]
pub enum SchemeEntryType {
    Unspecified,
    Directory,
    Table,
    PersQueueGroup,
    Database,
    RtmrVolume,
    BlockStoreVolume,
    CoordinationNode,
    Sequence,
    Replication,
    Unknown(i32),
}

#[derive(Debug)]
pub struct SchemePermissions {
    pub subject: String,
    pub permission_names: Vec<String>,
}
