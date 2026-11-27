//! gRPC transport metrics interceptor.
//!
//! Sits in [`MultiInterceptor`](crate::grpc_wrapper::runtime_interceptors::MultiInterceptor):
//! `on_call` parses the RPC path and stores an exactly-once completion guard in
//! the free-form `RequestMetadata` channel; `on_feature_poll_ready` moves that
//! guard into the response body, whose terminal trailers record the request
//! counter, full-stream duration, and transport error views.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, RwLock};
use std::task::{Context, Poll};
use std::time::Instant;

use crate::client_metrics::MetricsRecorder;
use crate::grpc_wrapper::runtime_interceptors::{
    ChannelResponse, EndpointLabel, GrpcInterceptor, InterceptorError, InterceptorRequest,
    InterceptorResult, RequestMetadata,
};

pub(crate) const TOPIC_STREAM_READ_PATH: &str = "/Ydb.Topic.V1.TopicService/StreamRead";
pub(crate) const TOPIC_STREAM_WRITE_PATH: &str = "/Ydb.Topic.V1.TopicService/StreamWrite";
pub(crate) const COORDINATION_SESSION_PATH: &str =
    "/Ydb.Coordination.V1.CoordinationService/Session";

/// Exactly-once completion state for one in-flight RPC.
///
/// The guard travels from the channel future into the response body. Dropping
/// either one before a terminal gRPC status records client-side cancellation.
struct CallMeta {
    recorder: Arc<dyn MetricsRecorder>,
    endpoint: Arc<str>,
    service: Arc<str>,
    method: Arc<str>,
    start: Instant,
    completed: bool,
}

impl CallMeta {
    fn new(
        recorder: Arc<dyn MetricsRecorder>,
        endpoint: Arc<str>,
        service: Arc<str>,
        method: Arc<str>,
    ) -> Self {
        Self {
            recorder,
            endpoint,
            service,
            method,
            start: Instant::now(),
            completed: false,
        }
    }

    fn complete(&mut self, code: GrpcCode) {
        if self.completed {
            return;
        }
        self.completed = true;
        self.recorder.grpc_request(
            &self.endpoint,
            &self.service,
            &self.method,
            code.as_label(),
            self.start.elapsed(),
        );
    }
}

impl Drop for CallMeta {
    fn drop(&mut self) {
        self.complete(GrpcCode::Cancelled);
    }
}

/// Response body that completes the RPC on its terminal trailers or clean EOS.
struct MetricsBody {
    inner: tonic::body::Body,
    call: CallMeta,
}

impl http_body::Body for MetricsBody {
    type Data = <tonic::body::Body as http_body::Body>::Data;
    type Error = <tonic::body::Body as http_body::Body>::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(trailers) = frame.trailers_ref() {
                    let code = grpc_status_from_headers(trailers).unwrap_or(GrpcCode::Unknown);
                    this.call.complete(code);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                this.call.complete(GrpcCode::Unknown);
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                this.call.complete(GrpcCode::Unknown);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Records gRPC transport metrics for every RPC flowing through the channel.
pub(crate) struct MetricsInterceptor {
    recorder: Arc<dyn MetricsRecorder>,
}

impl MetricsInterceptor {
    pub(crate) fn new(recorder: Arc<dyn MetricsRecorder>) -> Self {
        Self { recorder }
    }
}

impl GrpcInterceptor for MetricsInterceptor {
    fn on_call(
        &self,
        metadata: &mut RequestMetadata,
        req: InterceptorRequest,
    ) -> InterceptorResult<InterceptorRequest> {
        // The channel stamps the endpoint label into the request extensions;
        // the URI itself is path-only in tonic (no authority to parse).
        let endpoint = req
            .extensions()
            .get::<EndpointLabel>()
            .map(|label| label.0.clone())
            .unwrap_or_else(|| intern_endpoint(authority_label(req.uri())));
        let (service, method) = parse_grpc_path(req.uri().path());
        *metadata = Some(Box::new(CallMeta::new(
            Arc::clone(&self.recorder),
            endpoint,
            service,
            method,
        )));
        Ok(req)
    }

    fn on_feature_poll_ready(
        &self,
        metadata: &mut RequestMetadata,
        res: Result<ChannelResponse, InterceptorError>,
    ) -> Result<ChannelResponse, InterceptorError> {
        let Some(mut meta) = take_call_meta(metadata) else {
            return res;
        };

        match res {
            Ok(response) => {
                if let Some(code) = grpc_status_from_headers(response.headers()) {
                    // Trailers-only responses carry their terminal status in the
                    // response headers and do not need a body wrapper.
                    meta.complete(code);
                    Ok(response)
                } else {
                    let (parts, body) = response.into_parts();
                    let body = tonic::body::Body::new(MetricsBody {
                        inner: body,
                        call: meta,
                    });
                    Ok(http::Response::from_parts(parts, body))
                }
            }
            Err(error) => {
                let code = match &error {
                    InterceptorError::Transport(_) => GrpcCode::Unavailable,
                    InterceptorError::Custom(_) | InterceptorError::Internal(_) => {
                        GrpcCode::Internal
                    }
                };
                meta.complete(code);
                Err(error)
            }
        }
    }
}

fn take_call_meta(metadata: &mut RequestMetadata) -> Option<CallMeta> {
    let boxed = metadata.as_mut()?;
    // The metadata slot is shared between interceptors; only our own value type is
    // present when this interceptor served the call.
    if !boxed.is::<CallMeta>() {
        return None;
    }
    let slot = metadata.take()?;
    slot.downcast::<CallMeta>().ok().map(|meta| *meta)
}

fn authority_label(uri: &http::Uri) -> &str {
    uri.authority().map(|a| a.as_str()).unwrap_or("unknown")
}

type ServiceMethodCache = RwLock<HashMap<String, (Arc<str>, Arc<str>)>>;
type EndpointCache = RwLock<HashMap<String, Arc<str>>>;

/// gRPC status codes with Prometheus-friendly snake-case label names.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum GrpcCode {
    Ok,
    Cancelled,
    Unknown,
    InvalidArgument,
    DeadlineExceeded,
    NotFound,
    AlreadyExists,
    PermissionDenied,
    ResourceExhausted,
    FailedPrecondition,
    Aborted,
    OutOfRange,
    Unimplemented,
    Internal,
    Unavailable,
    DataLoss,
    Unauthenticated,
}

impl GrpcCode {
    pub(crate) fn as_label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Cancelled => "cancelled",
            Self::Unknown => "unknown",
            Self::InvalidArgument => "invalid_argument",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::NotFound => "not_found",
            Self::AlreadyExists => "already_exists",
            Self::PermissionDenied => "permission_denied",
            Self::ResourceExhausted => "resource_exhausted",
            Self::FailedPrecondition => "failed_precondition",
            Self::Aborted => "aborted",
            Self::OutOfRange => "out_of_range",
            Self::Unimplemented => "unimplemented",
            Self::Internal => "internal",
            Self::Unavailable => "unavailable",
            Self::DataLoss => "data_loss",
            Self::Unauthenticated => "unauthenticated",
        }
    }

    /// Inverse of [`Self::as_label`], for mapping label strings back to the
    /// closed code set (used by the metric handle caches). Returns `None` for
    /// values outside the set.
    pub(crate) fn from_label(label: &str) -> Option<Self> {
        let code = match label {
            "ok" => Self::Ok,
            "cancelled" => Self::Cancelled,
            "unknown" => Self::Unknown,
            "invalid_argument" => Self::InvalidArgument,
            "deadline_exceeded" => Self::DeadlineExceeded,
            "not_found" => Self::NotFound,
            "already_exists" => Self::AlreadyExists,
            "permission_denied" => Self::PermissionDenied,
            "resource_exhausted" => Self::ResourceExhausted,
            "failed_precondition" => Self::FailedPrecondition,
            "aborted" => Self::Aborted,
            "out_of_range" => Self::OutOfRange,
            "unimplemented" => Self::Unimplemented,
            "internal" => Self::Internal,
            "unavailable" => Self::Unavailable,
            "data_loss" => Self::DataLoss,
            "unauthenticated" => Self::Unauthenticated,
            _ => return None,
        };
        Some(code)
    }

    fn from_i32(value: i32) -> Self {
        match value {
            0 => Self::Ok,
            1 => Self::Cancelled,
            2 => Self::Unknown,
            3 => Self::InvalidArgument,
            4 => Self::DeadlineExceeded,
            5 => Self::NotFound,
            6 => Self::AlreadyExists,
            7 => Self::PermissionDenied,
            8 => Self::ResourceExhausted,
            9 => Self::FailedPrecondition,
            10 => Self::Aborted,
            11 => Self::OutOfRange,
            12 => Self::Unimplemented,
            13 => Self::Internal,
            14 => Self::Unavailable,
            15 => Self::DataLoss,
            16 => Self::Unauthenticated,
            _ => Self::Unknown,
        }
    }
}

/// Read a terminal gRPC status from response headers or trailers.
///
/// `None` means the status has not arrived yet (or the stream ended without
/// one); it must not be interpreted as a successful RPC.
fn grpc_status_from_headers(headers: &http::HeaderMap) -> Option<GrpcCode> {
    headers.get("grpc-status").map(|value| {
        value
            .to_str()
            .ok()
            .and_then(|value| value.parse::<i32>().ok())
            .map(GrpcCode::from_i32)
            .unwrap_or(GrpcCode::Unknown)
    })
}

/// Interned `(service, method)` pair for a gRPC path, shared by request and
/// stream-message metrics so both expose identical canonical labels.
pub(crate) fn parse_grpc_path(path: &str) -> (Arc<str>, Arc<str>) {
    static CACHE: OnceLock<ServiceMethodCache> = OnceLock::new();
    let cache = CACHE.get_or_init(|| RwLock::new(HashMap::new()));

    if let Some(hit) = cache
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(path)
        .cloned()
    {
        return hit;
    }

    let mut segments = path.trim_start_matches('/').split('/').rev();
    let method = segments.next().unwrap_or("unknown");
    let service = segments.next().unwrap_or("unknown");
    let parsed = (Arc::from(service), Arc::from(method));

    cache
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(path.to_string(), parsed.clone());
    parsed
}

/// Interned endpoint label (authority), shared across calls.
pub(crate) fn intern_endpoint(value: &str) -> Arc<str> {
    static CACHE: OnceLock<EndpointCache> = OnceLock::new();
    let cache = CACHE.get_or_init(|| RwLock::new(HashMap::new()));

    if let Some(hit) = cache
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(value)
        .cloned()
    {
        return hit;
    }

    let interned: Arc<str> = Arc::from(value);
    cache
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(value.to_string(), interned.clone());
    interned
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_metrics::test_recorder::TestRecorder;
    use crate::grpc_wrapper::runtime_interceptors::MultiInterceptor;
    use bytes::Bytes;
    use futures_util::stream;
    use http_body_util::{BodyExt, StreamBody};

    type TestFrame = http_body::Frame<Bytes>;
    type TestFrameResult = Result<TestFrame, tonic::Status>;

    fn interceptor_with_recorder() -> (MultiInterceptor, Arc<TestRecorder>) {
        let recorder = Arc::new(TestRecorder::new());
        let metrics: Arc<dyn MetricsRecorder> = recorder.clone();
        let interceptor =
            MultiInterceptor::new().with_interceptor(MetricsInterceptor::new(metrics));
        (interceptor, recorder)
    }

    fn fake_request(path: &str, authority: &str) -> InterceptorRequest {
        http::Request::builder()
            .uri(
                http::Uri::builder()
                    .scheme("http")
                    .authority(authority)
                    .path_and_query(path)
                    .build()
                    .expect("valid test uri"),
            )
            .body(tonic::body::Body::default())
            .expect("valid test request")
    }

    fn fake_response(headers: &[(&str, &str)]) -> ChannelResponse {
        response_with_frames(headers, Vec::new())
    }

    fn response_with_frames(
        headers: &[(&str, &str)],
        frames: Vec<TestFrameResult>,
    ) -> ChannelResponse {
        let mut builder = http::Response::builder();
        for (key, value) in headers {
            builder = builder.header(*key, *value);
        }
        let body = tonic::body::Body::new(StreamBody::new(stream::iter(frames)));
        builder.body(body).expect("valid test response")
    }

    fn trailers(code: i32) -> TestFrameResult {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "grpc-status",
            http::HeaderValue::from_str(&code.to_string()).expect("valid gRPC status"),
        );
        Ok(http_body::Frame::trailers(headers))
    }

    fn data() -> TestFrameResult {
        Ok(http_body::Frame::data(Bytes::from_static(b"response")))
    }

    async fn consume_body(response: ChannelResponse) {
        response
            .into_body()
            .collect()
            .await
            .expect("test response body must succeed");
    }

    #[tokio::test]
    async fn request_path_and_labels_are_recorded_on_success() {
        let (interceptor, recorder) = interceptor_with_recorder();

        let mut metadata: RequestMetadata = None;
        let req = interceptor
            .on_call(
                &mut metadata,
                fake_request("/ydb.api.v1.TableService/ExecuteQuery", "host:2135"),
            )
            .expect("on_call must pass the request through");
        assert!(
            metadata.is_some(),
            "on_call must store call metadata for poll_ready"
        );
        let _ = req;

        let response = interceptor
            .on_feature_poll_ready(
                &mut metadata,
                Ok(response_with_frames(&[], vec![data(), trailers(0)])),
            )
            .expect("response must pass through");
        assert_eq!(
            recorder.counter_value("ydb_grpc_requests_total"),
            0,
            "response headers must not complete the RPC before its trailers"
        );
        assert_eq!(
            recorder.histogram_count("ydb_grpc_request_duration_seconds"),
            0
        );

        consume_body(response).await;
        assert_eq!(
            recorder.counter_value("ydb_grpc_requests_total"),
            1,
            "each completed RPC must increment ydb_grpc_requests_total exactly once"
        );
        assert_eq!(
            recorder
                .label_of_last("ydb_grpc_requests_total", "service")
                .as_deref(),
            Some("ydb.api.v1.TableService")
        );
        assert_eq!(
            recorder
                .label_of_last("ydb_grpc_requests_total", "method")
                .as_deref(),
            Some("ExecuteQuery")
        );
        assert_eq!(
            recorder
                .label_of_last("ydb_grpc_requests_total", "endpoint")
                .as_deref(),
            Some("host:2135")
        );
        assert_eq!(
            recorder
                .label_of_last("ydb_grpc_requests_total", "grpc_code")
                .as_deref(),
            Some("ok")
        );

        assert_eq!(
            recorder.histogram_count("ydb_grpc_request_duration_seconds"),
            1,
            "duration must be recorded once per RPC"
        );
        assert_eq!(
            recorder.counter_value("ydb_grpc_errors_total"),
            0,
            "successful RPC must not count as transport error"
        );
    }

    #[tokio::test]
    async fn trailers_after_headers_determine_final_error_status() {
        let (interceptor, recorder) = interceptor_with_recorder();
        let mut metadata: RequestMetadata = None;
        let _ = interceptor.on_call(
            &mut metadata,
            fake_request(TOPIC_STREAM_READ_PATH, "host:2135"),
        );

        let response = interceptor
            .on_feature_poll_ready(
                &mut metadata,
                Ok(response_with_frames(&[], vec![data(), trailers(13)])),
            )
            .expect("response must pass through");
        assert_eq!(recorder.counter_value("ydb_grpc_requests_total"), 0);

        consume_body(response).await;
        assert_eq!(
            recorder.counter_value_with_label("ydb_grpc_requests_total", "grpc_code", "internal"),
            1
        );
        assert_eq!(recorder.counter_value("ydb_grpc_requests_total"), 1);
        assert_eq!(
            recorder.histogram_count("ydb_grpc_request_duration_seconds"),
            1
        );
        assert_eq!(recorder.counter_value("ydb_grpc_errors_total"), 1);
    }

    #[tokio::test]
    async fn body_frame_error_without_grpc_status_is_unknown() {
        let (interceptor, recorder) = interceptor_with_recorder();
        let mut metadata: RequestMetadata = None;
        let _ = interceptor.on_call(
            &mut metadata,
            fake_request(TOPIC_STREAM_READ_PATH, "host:2135"),
        );
        let response = interceptor
            .on_feature_poll_ready(
                &mut metadata,
                Ok(response_with_frames(
                    &[],
                    vec![data(), Err(tonic::Status::unavailable("body failed"))],
                )),
            )
            .expect("response must pass through");
        assert_eq!(recorder.counter_value("ydb_grpc_requests_total"), 0);

        let result = response.into_body().collect().await;
        assert!(result.is_err(), "the body frame error must propagate");
        assert_eq!(
            recorder.counter_value_with_label("ydb_grpc_requests_total", "grpc_code", "unknown"),
            1
        );
        assert_eq!(recorder.counter_value("ydb_grpc_requests_total"), 1);
        assert_eq!(
            recorder.histogram_count("ydb_grpc_request_duration_seconds"),
            1
        );
        assert_eq!(recorder.counter_value("ydb_grpc_errors_total"), 1);
    }

    #[tokio::test]
    async fn clean_eos_without_grpc_status_is_unknown() {
        let (interceptor, recorder) = interceptor_with_recorder();
        let mut metadata: RequestMetadata = None;
        let _ = interceptor.on_call(
            &mut metadata,
            fake_request("/ydb.api.v1.QueryService/ExecuteQuery", "host:2135"),
        );
        let response = interceptor
            .on_feature_poll_ready(&mut metadata, Ok(fake_response(&[])))
            .expect("response must pass through");

        consume_body(response).await;
        assert_eq!(
            recorder.counter_value_with_label("ydb_grpc_requests_total", "grpc_code", "unknown"),
            1
        );
        assert_eq!(recorder.counter_value("ydb_grpc_errors_total"), 1);
    }

    #[test]
    fn dropping_call_before_response_records_cancelled_once() {
        let (interceptor, recorder) = interceptor_with_recorder();
        let mut metadata: RequestMetadata = None;
        let _ = interceptor.on_call(
            &mut metadata,
            fake_request("/ydb.api.v1.QueryService/ExecuteQuery", "host:2135"),
        );

        drop(metadata);
        assert_eq!(
            recorder.counter_value_with_label("ydb_grpc_requests_total", "grpc_code", "cancelled"),
            1
        );
        assert_eq!(recorder.counter_value("ydb_grpc_requests_total"), 1);
        assert_eq!(
            recorder.histogram_count("ydb_grpc_request_duration_seconds"),
            1
        );
    }

    #[test]
    fn dropping_response_body_records_cancelled_once() {
        let (interceptor, recorder) = interceptor_with_recorder();
        let mut metadata: RequestMetadata = None;
        let _ = interceptor.on_call(
            &mut metadata,
            fake_request("/ydb.api.v1.QueryService/ExecuteQuery", "host:2135"),
        );
        let response = interceptor
            .on_feature_poll_ready(&mut metadata, Ok(fake_response(&[])))
            .expect("response must pass through");
        assert_eq!(recorder.counter_value("ydb_grpc_requests_total"), 0);

        drop(response);
        assert_eq!(
            recorder.counter_value_with_label("ydb_grpc_requests_total", "grpc_code", "cancelled"),
            1
        );
        assert_eq!(recorder.counter_value("ydb_grpc_requests_total"), 1);
        assert_eq!(
            recorder.histogram_count("ydb_grpc_request_duration_seconds"),
            1
        );
    }

    #[test]
    fn trailers_only_response_records_grpc_status() {
        let (interceptor, recorder) = interceptor_with_recorder();

        let mut metadata: RequestMetadata = None;
        let _ = interceptor.on_call(
            &mut metadata,
            fake_request("/ydb.api.v1.TopicService/StreamRead", "host:2135"),
        );
        let response = Ok(fake_response(&[("grpc-status", "14")]));
        let _ = interceptor.on_feature_poll_ready(&mut metadata, response);

        assert_eq!(
            recorder
                .label_of_last("ydb_grpc_requests_total", "grpc_code")
                .as_deref(),
            Some("unavailable"),
            "grpc-status header must map to the gRPC code label"
        );
        assert_eq!(recorder.counter_value("ydb_grpc_errors_total"), 1);
        assert_eq!(
            recorder
                .label_of_last("ydb_grpc_errors_total", "grpc_code")
                .as_deref(),
            Some("unavailable")
        );
    }

    #[tokio::test]
    async fn transport_error_records_unavailable_and_error_counter() {
        let (interceptor, recorder) = interceptor_with_recorder();

        let mut metadata: RequestMetadata = None;
        let _ = interceptor.on_call(
            &mut metadata,
            fake_request("/ydb.api.v1.DiscoveryService/ListEndpoints", "host:2135"),
        );

        // A real transport error: connecting to a closed local port always fails fast.
        let error = tonic::transport::Endpoint::from_static("http://127.0.0.1:1")
            .connect()
            .await
            .expect_err("connecting to a closed port must fail");
        let _ = interceptor
            .on_feature_poll_ready(&mut metadata, Err(InterceptorError::Transport(error)));

        assert_eq!(recorder.counter_value("ydb_grpc_requests_total"), 1);
        assert_eq!(
            recorder
                .label_of_last("ydb_grpc_requests_total", "grpc_code")
                .as_deref(),
            Some("unavailable"),
            "transport failure must map to the unavailable code"
        );
        assert_eq!(recorder.counter_value("ydb_grpc_errors_total"), 1);
    }

    #[test]
    fn unknown_paths_fall_back_to_unknown_labels() {
        let (interceptor, recorder) = interceptor_with_recorder();

        let mut metadata: RequestMetadata = None;
        let _ = interceptor.on_call(&mut metadata, fake_request("/weird.service/path", "host:1"));
        let _ = interceptor
            .on_feature_poll_ready(&mut metadata, Ok(fake_response(&[("grpc-status", "0")])));

        assert_eq!(
            recorder
                .label_of_last("ydb_grpc_requests_total", "service")
                .as_deref(),
            Some("weird.service")
        );
        assert_eq!(
            recorder
                .label_of_last("ydb_grpc_requests_total", "method")
                .as_deref(),
            Some("path")
        );
    }

    #[test]
    fn path_parsing_and_interning() {
        let (service, method) = parse_grpc_path("/ydb.api.v1.QueryService/ExecuteQuery");
        assert_eq!(&*service, "ydb.api.v1.QueryService");
        assert_eq!(&*method, "ExecuteQuery");

        // Second parse hits the cache and must return the same interned values.
        let (service2, method2) = parse_grpc_path("/ydb.api.v1.QueryService/ExecuteQuery");
        assert!(Arc::ptr_eq(&service, &service2));
        assert!(Arc::ptr_eq(&method, &method2));

        let endpoint = intern_endpoint("host:2135");
        assert!(Arc::ptr_eq(&endpoint, &intern_endpoint("host:2135")));
    }

    #[test]
    fn grpc_code_mapping_covers_known_codes() {
        assert_eq!(GrpcCode::from_i32(0).as_label(), "ok");
        assert_eq!(GrpcCode::from_i32(4).as_label(), "deadline_exceeded");
        assert_eq!(GrpcCode::from_i32(14).as_label(), "unavailable");
        assert_eq!(GrpcCode::from_i32(99).as_label(), "unknown");
    }

    #[test]
    fn grpc_code_from_label_round_trips() {
        for code in [
            GrpcCode::Ok,
            GrpcCode::DeadlineExceeded,
            GrpcCode::Unavailable,
            GrpcCode::Unauthenticated,
        ] {
            assert_eq!(GrpcCode::from_label(code.as_label()), Some(code));
        }
        assert_eq!(GrpcCode::from_label("not-a-grpc-code"), None);
    }
}
