macro_rules! request_without_result {
    ( $self: ident .service. $method: ident, $RawRequest: ident => $GrpcRequestType: ty) => {
        let req = <$GrpcRequestType>::from($RawRequest);

        trace!(
            " {} request: {}",
            stringify!($ClientType.$method),
            serde_json::to_string(&req).unwrap_or("bad json".into())
        );

        let response = $self.service.$method(req).await?;
        return grpc_read_void_operation_result(response);
    };
}
