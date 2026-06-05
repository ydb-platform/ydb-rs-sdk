pub(crate) const DEFAULT_GRPC_MESSAGE_SIZE_LIMIT_BYTES: usize = 64_000_000;

pub(crate) trait WithGrpcMaxMessageSize: Sized {
    fn with_grpc_max_message_size(self, bytes: usize) -> Self;
}
