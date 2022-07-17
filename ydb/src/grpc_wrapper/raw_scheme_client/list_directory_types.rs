use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_ydb_operation::OperationParams;

#[derive(Debug)]
pub(crate) struct RawListDirectoryRequest {
    pub(crate) operation_params: OperationParams,
    pub(crate) path: String,
}

impl From<RawListDirectoryRequest> for ydb_grpc::ydb_proto::scheme::ListDirectoryRequest {
    fn from(v: RawListDirectoryRequest) -> Self {
        return Self {
            operation_params: Some(v.operation_params.into()),
            path: v.path,
        };
    }
}

#[derive(Debug)]
pub(crate) struct RawListDirectoryResult {
    pub(crate) self_item: RawEntry,
    pub(crate) children: Vec<RawEntry>,
}

impl TryFrom<ydb_grpc::ydb_proto::scheme::ListDirectoryResult> for RawListDirectoryResult {
    type Error = RawError;

    fn try_from(
        value: ydb_grpc::ydb_proto::scheme::ListDirectoryResult,
    ) -> Result<Self, Self::Error> {
        let selfEntry = if let (Some(entry)) = value.self_ {
            RawEntry::from(entry)
        } else {
            return Err(RawError::ProtobufDecodeError(format!(
                "list directory self entry is empty"
            )));
        };

        Ok(Self {
            self_item: selfEntry,
            children: value
                .children
                .into_iter()
                .map(|entry| RawEntry::from(entry))
                .collect(),
        })
    }
}

#[derive(Debug)]
pub(crate) struct RawEntry {
    pub name: String,
    pub owner: String,
    pub r#type: RawEntryType,
    pub effective_permissions: Vec<RawPermissions>,
    pub permissions: Vec<RawPermissions>,
    pub size_bytes: u64,
}

impl From<ydb_grpc::ydb_proto::scheme::Entry> for RawEntry {
    fn from(value: ydb_grpc::ydb_proto::scheme::Entry) -> Self {
        Self {
            name: value.name,
            owner: value.owner,
            r#type: RawEntryType::from(value.r#type),
            effective_permissions: vec![],
            permissions: vec![],
            size_bytes: 0,
        }
    }
}

#[derive(Debug)]
pub(crate) enum RawEntryType {
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

impl From<i32> for RawEntryType {
    fn from(v: i32) -> Self {
        use ydb_grpc::ydb_proto::scheme::entry::Type as grpcT;
        match grpcT::from_i32(v) {
            Some(grpcT::Unspecified) => RawEntryType::Unspecified,
            Some(grpcT::Directory) => RawEntryType::Directory,
            Some(grpcT::Table) => RawEntryType::Table,
            Some(grpcT::PersQueueGroup) => RawEntryType::PersQueueGroup,
            Some(grpcT::Database) => RawEntryType::Database,
            Some(grpcT::RtmrVolume) => RawEntryType::RtmrVolume,
            Some(grpcT::BlockStoreVolume) => RawEntryType::BlockStoreVolume,
            Some(grpcT::CoordinationNode) => RawEntryType::CoordinationNode,
            Some(grpcT::Sequence) => RawEntryType::Sequence,
            Some(grpcT::Replication) => RawEntryType::Replication,
            None => RawEntryType::Unknown(v),
        }
    }
}

impl From<ydb_grpc::ydb_proto::scheme::entry::Type> for RawEntryType {
    fn from(v: ydb_grpc::ydb_proto::scheme::entry::Type) -> Self {
        return RawEntryType::from(v as i32);
    }
}

#[derive(Debug)]
pub(crate) struct RawPermissions {
    pub(crate) subject: String,
    pub(crate) permission_names: Vec<String>,
}

impl From<ydb_grpc::ydb_proto::scheme::Permissions> for RawPermissions {
    fn from(value: ydb_grpc::ydb_proto::scheme::Permissions) -> Self {
        Self {
            subject: value.subject,
            permission_names: value.permission_names,
        }
    }
}
