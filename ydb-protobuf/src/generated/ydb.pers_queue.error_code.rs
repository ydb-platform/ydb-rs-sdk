///500000..500999
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ErrorCode {
    Ok = 0,
    Initializing = 500001,
    Overload = 500002,
    BadRequest = 500003,
    WrongCookie = 500004,
    SourceidDeleted = 500024,
    WriteErrorPartitionIsFull = 500005,
    WriteErrorDiskIsFull = 500015,
    WriteErrorBadOffset = 500019,
    CreateSessionAlreadyLocked = 500006,
    DeleteSessionNoSession = 500007,
    ReadErrorInProgress = 500008,
    ReadErrorNoSession = 500009,
    ReadErrorTooSmallOffset = 500011,
    ReadErrorTooBigOffset = 500012,
    SetOffsetErrorCommitToFuture = 500013,
    TabletIsDropped = 500014,
    ReadNotDone = 500016,
    UnknownTopic = 500017,
    AccessDenied = 500018,
    ClusterDisabled = 500020,
    WrongPartitionNumber = 500021,
    PreferredClusterMismatched = 500022,
    Error = 500100,
}
