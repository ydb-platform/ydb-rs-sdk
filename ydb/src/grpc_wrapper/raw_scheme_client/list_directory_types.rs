use crate::grpc_wrapper::raw_errors::RawError;
use crate::grpc_wrapper::raw_ydb_operation::RawOperationParams;
use crate::{SchemeEntry, SchemeEntryType, SchemePermissions};

#[derive(Debug)]
pub(crate) struct RawListDirectoryRequest {
    pub(crate) operation_params: RawOperationParams,
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
    pub(crate) self_item: crate::SchemeEntry,
    pub(crate) children: Vec<crate::SchemeEntry>,
}

impl TryFrom<ydb_grpc::ydb_proto::scheme::ListDirectoryResult> for RawListDirectoryResult {
    type Error = RawError;

    fn try_from(
        value: ydb_grpc::ydb_proto::scheme::ListDirectoryResult,
    ) -> Result<Self, Self::Error> {
        let self_entry = if let (Some(entry)) = value.self_ {
            from_grpc_to_scheme_entry(entry)
        } else {
            return Err(RawError::ProtobufDecodeError(format!(
                "list directory self entry is empty"
            )));
        };

        Ok(Self {
            self_item: self_entry,
            children: value
                .children
                .into_iter()
                .map(|entry| from_grpc_to_scheme_entry(entry))
                .collect(),
        })
    }
}

fn from_grpc_to_scheme_entry(value: ydb_grpc::ydb_proto::scheme::Entry) -> SchemeEntry {
    SchemeEntry {
        name: value.name,
        owner: value.owner,
        r#type: from_grpc_code_to_scheme_entry_type(value.r#type),
        effective_permissions: value
            .effective_permissions
            .into_iter()
            .map(|item| from_grpc_to_scheme_permissions(item))
            .collect(),
        permissions: value
            .permissions
            .into_iter()
            .map(|item| from_grpc_to_scheme_permissions(item))
            .collect(),
        size_bytes: value.size_bytes,
    }
}

fn from_grpc_code_to_scheme_entry_type(value: i32) -> SchemeEntryType {
    use ydb_grpc::ydb_proto::scheme::entry::Type as grpcT;
    match grpcT::from_i32(value) {
        Some(grpcT::Unspecified) => SchemeEntryType::Unspecified,
        Some(grpcT::Directory) => SchemeEntryType::Directory,
        Some(grpcT::Table) => SchemeEntryType::Table,
        Some(grpcT::PersQueueGroup) => SchemeEntryType::PersQueueGroup,
        Some(grpcT::Database) => SchemeEntryType::Database,
        Some(grpcT::RtmrVolume) => SchemeEntryType::RtmrVolume,
        Some(grpcT::BlockStoreVolume) => SchemeEntryType::BlockStoreVolume,
        Some(grpcT::CoordinationNode) => SchemeEntryType::CoordinationNode,
        Some(grpcT::Sequence) => SchemeEntryType::Sequence,
        Some(grpcT::Replication) => SchemeEntryType::Replication,
        None => SchemeEntryType::Unknown(value),
    }
}

fn from_grpc_to_scheme_permissions(
    value: ydb_grpc::ydb_proto::scheme::Permissions,
) -> SchemePermissions {
    SchemePermissions {
        subject: value.subject,
        permission_names: value.permission_names,
    }
}
