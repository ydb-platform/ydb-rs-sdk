use crate::grpc_wrapper::raw_scheme_client::list_directory_types::{
    RawEntry, RawEntryType, RawPermissions,
};

#[derive(Debug)]
pub struct SchemeEntry {
    pub name: String,
    pub owner: String,
    pub r#type: SchemeEntryType,
    pub effective_permissions: Vec<SchemePermissions>,
    pub permissions: Vec<SchemePermissions>,
    pub size_bytes: u64,
}

impl From<RawEntry> for SchemeEntry {
    fn from(value: RawEntry) -> Self {
        Self {
            name: value.name,
            owner: value.owner,
            r#type: SchemeEntryType::from(value.r#type),
            effective_permissions: value
                .effective_permissions
                .into_iter()
                .map(|item| SchemePermissions::from(item))
                .collect(),
            permissions: vec![],
            size_bytes: value.size_bytes,
        }
    }
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

impl From<RawEntryType> for SchemeEntryType {
    fn from(v: RawEntryType) -> Self {
        match v {
            RawEntryType::Unspecified => SchemeEntryType::Unspecified,
            RawEntryType::Directory => SchemeEntryType::Directory,
            RawEntryType::Table => SchemeEntryType::Table,
            RawEntryType::PersQueueGroup => SchemeEntryType::PersQueueGroup,
            RawEntryType::Database => SchemeEntryType::Database,
            RawEntryType::RtmrVolume => SchemeEntryType::RtmrVolume,
            RawEntryType::BlockStoreVolume => SchemeEntryType::BlockStoreVolume,
            RawEntryType::CoordinationNode => SchemeEntryType::CoordinationNode,
            RawEntryType::Sequence => SchemeEntryType::Sequence,
            RawEntryType::Replication => SchemeEntryType::Replication,
            RawEntryType::Unknown(val) => SchemeEntryType::Unknown(val),
        }
    }
}

#[derive(Debug)]
pub struct SchemePermissions {
    pub(crate) subject: String,
    pub(crate) permission_names: Vec<String>,
}

impl From<RawPermissions> for SchemePermissions {
    fn from(value: RawPermissions) -> Self {
        Self {
            subject: value.subject,
            permission_names: value.permission_names,
        }
    }
}
