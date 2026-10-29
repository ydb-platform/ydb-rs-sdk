use std::sync::Arc;

use tokio::sync::mpsc;

use crate::client_metrics::{MetricsRecorder, StreamDirection};
use crate::grpc_wrapper::raw_errors::{RawError, RawResult};

/// Unbound stream-message metrics context: the recorder plus the endpoint of the
/// node the stream is attached to. Stamped into raw clients by the connection
/// manager; bound to concrete `service`/`method` labels at stream creation.
#[derive(Clone)]
pub(crate) struct GrpcStreamMetrics {
    recorder: Arc<dyn MetricsRecorder>,
    endpoint: Arc<str>,
}

impl GrpcStreamMetrics {
    pub(crate) fn new(recorder: Arc<dyn MetricsRecorder>, endpoint: Arc<str>) -> Self {
        Self { recorder, endpoint }
    }

    /// Bind the context to one streaming RPC (`service`/`method` are static).
    pub(crate) fn bind(
        &self,
        service: &'static str,
        method: &'static str,
    ) -> GrpcStreamMetricsBound {
        GrpcStreamMetricsBound {
            recorder: Arc::clone(&self.recorder),
            endpoint: Arc::clone(&self.endpoint),
            service,
            method,
        }
    }
}

/// Stream-message metrics bound to one endpoint and one streaming RPC.
#[derive(Clone)]
pub(crate) struct GrpcStreamMetricsBound {
    recorder: Arc<dyn MetricsRecorder>,
    endpoint: Arc<str>,
    service: &'static str,
    method: &'static str,
}

impl GrpcStreamMetricsBound {
    fn message(&self, direction: StreamDirection) {
        self.recorder
            .grpc_stream_message(&self.endpoint, self.service, self.method, direction);
    }
}

pub(crate) struct AsyncGrpcStreamWrapper<RequestT, ResponseT> {
    from_client_grpc: mpsc::UnboundedSender<RequestT>,
    from_server_grpc: tonic::Streaming<ResponseT>,
    metrics: Option<GrpcStreamMetricsBound>,
}

impl<RequestT, ResponseT> AsyncGrpcStreamWrapper<RequestT, ResponseT> {
    pub(crate) fn new(
        request_stream: mpsc::UnboundedSender<RequestT>,
        response_stream: tonic::Streaming<ResponseT>,
    ) -> Self {
        Self {
            from_client_grpc: request_stream,
            from_server_grpc: response_stream,
            metrics: None,
        }
    }

    /// Like [`Self::new`], but counts sent/received messages as gRPC stream metrics.
    pub(crate) fn new_with_metrics(
        request_stream: mpsc::UnboundedSender<RequestT>,
        response_stream: tonic::Streaming<ResponseT>,
        metrics: GrpcStreamMetricsBound,
    ) -> Self {
        Self {
            from_client_grpc: request_stream,
            from_server_grpc: response_stream,
            metrics: Some(metrics),
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn send<Message>(&self, message: Message) -> RawResult<()>
    where
        Message: Into<RequestT>,
    {
        self.send_nowait(message)
    }

    pub(crate) fn send_nowait<Message>(&self, message: Message) -> RawResult<()>
    where
        Message: Into<RequestT>,
    {
        let result = self.from_client_grpc.send(message.into());
        if result.is_ok()
            && let Some(metrics) = &self.metrics
        {
            metrics.message(StreamDirection::Sent);
        }
        Ok(result?)
    }

    pub(crate) fn clone_sender(&self) -> mpsc::UnboundedSender<RequestT> {
        self.from_client_grpc.clone()
    }

    pub(crate) async fn receive<Message>(&mut self) -> RawResult<Message>
    where
        Message: TryFrom<ResponseT, Error = RawError>,
    {
        let message = self
            .from_server_grpc
            .message()
            .await
            .map_err(RawError::from)?;

        let Some(message) = message else {
            return Err(RawError::from(tonic::Status::unavailable(
                "Grpc stream was closed by sender",
            )));
        };

        if let Some(metrics) = &self.metrics {
            metrics.message(StreamDirection::Received);
        }
        message.try_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_metrics::test_recorder::TestRecorder;
    use ydb_grpc::ydb_proto::topic::stream_write_message;

    fn metrics_with_recorder() -> (GrpcStreamMetrics, Arc<TestRecorder>) {
        let recorder = Arc::new(TestRecorder::new());
        let metrics = GrpcStreamMetrics::new(recorder.clone(), Arc::from("host:2135"));
        (metrics, recorder)
    }

    #[tokio::test]
    async fn sent_messages_are_counted_with_static_labels() {
        let (metrics, recorder) = metrics_with_recorder();
        let bound = metrics.bind("topic_service", "stream_write");
        let (tx, _rx) = mpsc::unbounded_channel::<stream_write_message::FromClient>();
        let wrapper: AsyncGrpcStreamWrapper<
            stream_write_message::FromClient,
            stream_write_message::FromServer,
        > = AsyncGrpcStreamWrapper::new_with_metrics(tx, empty_stream(), bound);

        wrapper
            .send_nowait(stream_write_message::FromClient::default())
            .expect("unbounded send must succeed");
        wrapper
            .send_nowait(stream_write_message::FromClient::default())
            .expect("unbounded send must succeed");

        assert_eq!(
            recorder.counter_value("ydb_grpc_stream_messages_total"),
            2,
            "each accepted send must count one sent message"
        );
        assert_eq!(
            recorder
                .label_of_last("ydb_grpc_stream_messages_total", "direction")
                .as_deref(),
            Some("sent")
        );
        assert_eq!(
            recorder
                .label_of_last("ydb_grpc_stream_messages_total", "service")
                .as_deref(),
            Some("topic_service")
        );
        assert_eq!(
            recorder
                .label_of_last("ydb_grpc_stream_messages_total", "method")
                .as_deref(),
            Some("stream_write")
        );
        assert_eq!(
            recorder
                .label_of_last("ydb_grpc_stream_messages_total", "endpoint")
                .as_deref(),
            Some("host:2135")
        );
    }

    /// Decoder yielding no messages: the receive path is covered by topic cycles.
    struct NullDecoder;

    impl tonic::codec::Decoder for NullDecoder {
        type Item = stream_write_message::FromServer;
        type Error = tonic::Status;

        fn decode(
            &mut self,
            _src: &mut tonic::codec::DecodeBuf<'_>,
        ) -> Result<Option<Self::Item>, Self::Error> {
            Ok(None)
        }
    }

    fn empty_stream() -> tonic::Streaming<stream_write_message::FromServer> {
        tonic::codec::Streaming::new_empty(NullDecoder, tonic::body::Body::default())
    }
}
