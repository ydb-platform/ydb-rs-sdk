macro_rules! request_without_result {
    ( $self: ident .service. $method: ident, $RawRequest: ident => $GrpcRequestType: ty) => {
        let req = <$GrpcRequestType>::from($RawRequest);

        trace!(
            " {} request: {}",
            stringify!($ClientType.$method),
            crate::trace_helpers::ensure_len_string(
                serde_json::to_string(&req).unwrap_or("bad json".into())
            )
        );

        let response = $self.service.$method(req).await?;
        return crate::grpc_wrapper::grpc::grpc_read_void_operation_result(response);
    };
}

macro_rules! request_with_result {
    (
        $self: ident .service. $method: ident,
        $RawRequest: ident => $GrpcRequestType: ty,
        $GrcpResultType: ty => $RawResultType: ty
    ) => {
        let req = <$GrpcRequestType>::from($RawRequest);

        trace!(
            " {} request: {}",
            stringify!($ClientType.$method),
            crate::trace_helpers::ensure_len_string(
                serde_json::to_string(&req).unwrap_or("bad json".into())
            )
        );

        let response = $self.service.$method(req).await?;
        let result: $GrcpResultType =
            crate::grpc_wrapper::grpc::grpc_read_operation_result(response)?;

        trace!(
            "{} result: {}",
            stringify!($ClientType.$method),
            crate::trace_helpers::ensure_len_string(
                serde_json::to_string(&result).unwrap_or("bad json".into())
            )
        );

        return <$RawResultType>::try_from(result);
    };
}

macro_rules! request_with_hidden_result {
    (
        $self: ident .service. $method: ident,
        $RawRequest: ident => $GrpcRequestType: ty,
        $GrcpResultType: ty => $RawResultType: ty
    ) => {
        let req = <$GrpcRequestType>::from($RawRequest);

        trace!(
            " {} request: {}",
            stringify!($ClientType.$method),
            crate::trace_helpers::ensure_len_string(
                serde_json::to_string(&req).unwrap_or("bad json".into())
            )
        );

        let response = $self.service.$method(req).await?;
        let result: $GrcpResultType =
            crate::grpc_wrapper::grpc::grpc_read_operation_result(response)?;

        trace!(
            "{} result hidden",
            stringify!($ClientType.$method),
        );

        return <$RawResultType>::try_from(result);
    };
}

#[allow(unused_macros)]
macro_rules! bidirectional_streaming_request {
    (
        $self: ident .service. $method: ident,
        $StreamingItemRequestType: ty,
        $StreamingItemResponseType: ty
    ) => {
        let (tx, rx): (
            tokio::sync::mpsc::UnboundedSender<$StreamingItemRequestType>,
            tokio::sync::mpsc::UnboundedReceiver<$StreamingItemRequestType>,
        ) = tokio::sync::mpsc::unbounded_channel();

        let request_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        let response_stream = $self.service.$method(request_stream).await?.into_inner();

        return Ok(AsyncGrpcStreamWrapper::<
            $StreamingItemRequestType,
            $StreamingItemResponseType,
        >::new(tx, response_stream));
    };
}
