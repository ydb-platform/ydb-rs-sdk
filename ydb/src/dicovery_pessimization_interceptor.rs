use crate::grpc_wrapper::runtime_interceptors::{
    ChannelResponse, GrpcInterceptor, InterceptorError, RequestMetadata,
};

pub(crate) struct DiscoveryPessimizationInterceptor {}

impl GrpcInterceptor for DiscoveryPessimizationInterceptor {
    fn on_feature_poll_ready(
        &self,
        metadata: &mut RequestMetadata,
        res: Result<ChannelResponse, InterceptorError>,
    ) -> Result<ChannelResponse, InterceptorError> {
        res
    }
}
