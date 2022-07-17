use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_ydb_operation::OperationParams;

pub(crate) struct ListDirectoryRequest {
    pub(crate) operation_params: OperationParams,
    pub(crate) path: String,
}

impl From<ListDirectoryRequest> for ydb_grpc::ydb_proto::scheme::ListDirectoryRequest {
    fn from(v: ListDirectoryRequest) -> Self {
        return Self {
            operation_params: Some(v.operation_params.into()),
            path: v.path,
        };
    }
}

pub(crate) struct ListDirectoryResult {
    pub(crate) self_item: Entry,
    pub(crate) childred: Vec<Entry>,
}

pub(crate) struct Entry {
    pub(crate) name: String,
    pub(crate) owner: String,
    pub(crate) r#type: EntryType,
    pub(crate) effective_permissions: Vec<Permissions>,
    pub(crate) permissions: Vec<Permissions>,
    pub(crate) size_bytes: u64,
}

impl From<ydb_grpc::ydb_proto::scheme::Entry> for Entry {
    fn from(value: ydb_grpc::ydb_proto::scheme::Entry) -> Self {
        Self {
            name: value.name,
            owner: value.owner,
            r#type: EntryType::from(value.r#type),
            effective_permissions: vec![],
            permissions: vec![],
            size_bytes: 0,
        }
    }
}

pub(crate) enum EntryType {
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

impl TryFrom<ydb_grpc::ydb_proto::scheme::ListDirectoryResult> for ListDirectoryResult {
    type Error = RawError;

    fn try_from(
        value: ydb_grpc::ydb_proto::scheme::ListDirectoryResult,
    ) -> Result<Self, Self::Error> {
        let selfEntry = if let (Some(entry)) = value.self_ {
            Entry::from(entry)
        } else {
            return Err(RawError::ProtobufDecodeError(format!(
                "list directory self entry is empty"
            )));
        };

        Ok(Self {
            self_item: selfEntry,
            childred: value
                .children
                .into_iter()
                .map(|entry| Entry::from(entry))
                .collect(),
        })
    }
}

impl From<i32> for EntryType {
    fn from(v: i32) -> Self {
        use ydb_grpc::ydb_proto::scheme::entry::Type as grpcT;
        match grpcT::from_i32(v) {
            Some(grpcT::Unspecified) => EntryType::Unspecified,
            Some(grpcT::Directory) => EntryType::Directory,
            Some(grpcT::Table) => EntryType::Table,
            Some(grpcT::PersQueueGroup) => EntryType::PersQueueGroup,
            Some(grpcT::Database) => EntryType::Database,
            Some(grpcT::RtmrVolume) => EntryType::RtmrVolume,
            Some(grpcT::BlockStoreVolume) => EntryType::BlockStoreVolume,
            Some(grpcT::CoordinationNode) => EntryType::CoordinationNode,
            Some(grpcT::Sequence) => EntryType::Sequence,
            Some(grpcT::Replication) => EntryType::Replication,
            None => EntryType::Unknown(v),
        }
    }
}

impl From<ydb_grpc::ydb_proto::scheme::entry::Type> for EntryType {
    fn from(v: ydb_grpc::ydb_proto::scheme::entry::Type) -> Self {
        return EntryType::from(v as i32);
    }
}

pub(crate) struct Permissions {
    pub(crate) subject: String,
    pub(crate) permission_names: Vec<String>,
}

impl From<ydb_grpc::ydb_proto::scheme::Permissions> for Permissions {
    fn from(value: ydb_grpc::ydb_proto::scheme::Permissions) -> Self {
        Self {
            subject: value.subject,
            permission_names: value.permission_names,
        }
    }
}
