use ydb_grpc::ydb_proto::scheme;

#[derive(Debug)]
pub enum SchemeIncoming {
    MakeDirectory(scheme::MakeDirectoryRequest),
    RemoveDirectory(scheme::RemoveDirectoryRequest),
    ListDirectory(scheme::ListDirectoryRequest),
    DescribePath(scheme::DescribePathRequest),
    ModifyPermissions(scheme::ModifyPermissionsRequest),
}

#[derive(Debug)]
pub enum SchemeReply {
    MakeDirectory(scheme::MakeDirectoryResponse),
    RemoveDirectory(scheme::RemoveDirectoryResponse),
    ListDirectory(scheme::ListDirectoryResponse),
    DescribePath(scheme::DescribePathResponse),
    ModifyPermissions(scheme::ModifyPermissionsResponse),
}
