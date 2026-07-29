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
        Self {
            operation_params: Some(v.operation_params.into()),
            path: v.path,
        }
    }
}

#[derive(Debug)]
pub(crate) struct RawListDirectoryResult {
    pub(crate) _self_item: crate::SchemeEntry,
    pub(crate) children: Vec<crate::SchemeEntry>,
}

impl TryFrom<ydb_grpc::ydb_proto::scheme::ListDirectoryResult> for RawListDirectoryResult {
    type Error = RawError;

    fn try_from(
        value: ydb_grpc::ydb_proto::scheme::ListDirectoryResult,
    ) -> Result<Self, Self::Error> {
        let self_entry = if let Some(entry) = value.self_ {
            from_grpc_to_scheme_entry(entry)
        } else {
            return Err(RawError::ProtobufDecodeError(
                "list directory self entry is empty".to_string(),
            ));
        };

        Ok(Self {
            _self_item: self_entry,
            children: value
                .children
                .into_iter()
                .map(from_grpc_to_scheme_entry)
                .collect(),
        })
    }
}

pub(crate) fn from_grpc_to_scheme_entry(value: ydb_grpc::ydb_proto::scheme::Entry) -> SchemeEntry {
    SchemeEntry {
        name: value.name,
        owner: value.owner,
        r#type: from_grpc_code_to_scheme_entry_type(value.r#type),
        effective_permissions: value
            .effective_permissions
            .into_iter()
            .map(from_grpc_to_scheme_permissions)
            .collect(),
        permissions: value
            .permissions
            .into_iter()
            .map(from_grpc_to_scheme_permissions)
            .collect(),
        size_bytes: value.size_bytes,
    }
}

fn from_grpc_code_to_scheme_entry_type(value: i32) -> SchemeEntryType {
    use ydb_grpc::ydb_proto::scheme::entry::Type as grpcT;
    let Ok(grpc_t) = grpcT::try_from(value) else {
        return SchemeEntryType::Unknown(value);
    };

    match grpc_t {
        grpcT::Unspecified => SchemeEntryType::Unspecified,
        grpcT::Directory => SchemeEntryType::Directory,
        grpcT::Table => SchemeEntryType::Table,
        grpcT::ColumnStore => SchemeEntryType::ColumnStore,
        grpcT::ColumnTable => SchemeEntryType::ColumnTable,
        grpcT::PersQueueGroup => SchemeEntryType::PersQueueGroup,
        grpcT::Database => SchemeEntryType::Database,
        grpcT::RtmrVolume => SchemeEntryType::RtmrVolume,
        grpcT::BlockStoreVolume => SchemeEntryType::BlockStoreVolume,
        grpcT::CoordinationNode => SchemeEntryType::CoordinationNode,
        grpcT::Sequence => SchemeEntryType::Sequence,
        grpcT::Replication => SchemeEntryType::Replication,
        grpcT::Topic => SchemeEntryType::Topic,
        grpcT::ExternalDataSource => SchemeEntryType::ExternalDataSource,
        grpcT::ExternalTable => SchemeEntryType::ExternalTable,
        grpcT::View => SchemeEntryType::View,
        // New scheme entry types (ResourcePool, Transfer, SysView) map to Unknown until
        // SchemeEntryType gains dedicated variants. Catch-all stays for forward compatibility
        // with older ydb-grpc releases where all variants are listed above.
        #[allow(unreachable_patterns)]
        _ => SchemeEntryType::Unknown(value),
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
