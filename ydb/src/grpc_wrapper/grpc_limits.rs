use crate::grpc_wrapper::grpc_stream_wrapper::GrpcStreamMetrics;

pub(crate) const DEFAULT_GRPC_MESSAGE_SIZE_LIMIT_BYTES: usize = 64_000_000;

pub(crate) trait WithGrpcMaxMessageSize: Sized {
    fn with_grpc_max_message_size(self, bytes: usize) -> Self;

    /// Stamp the gRPC stream-message metrics context onto a client.
    ///
    /// No-op by default: only clients owning gRPC streams (topic, coordination)
    /// override it. The context binds the endpoint of the node the client talks to,
    /// so stream messages are attributed to that endpoint.
    fn with_stream_metrics(self, _metrics: GrpcStreamMetrics) -> Self {
        self
    }
}
