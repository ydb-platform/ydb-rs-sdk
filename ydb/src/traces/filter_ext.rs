use tracing_subscriber::EnvFilter;

/// Extension trait for [`EnvFilter`] providing convenience methods to silence
/// verbose logs emitted by common HTTP/gRPC dependencies (hyper, tonic, tower, h2, reqwest).
///
/// Each method adds a directive that suppresses all events and spans from the
/// corresponding target (crate name). This is useful when the SDK's own
/// `trace`-level instrumentation is the main focus and the underlying transport
/// noise gets in the way.
///
/// # Example
///
/// ```rust
/// use tracing_subscriber::EnvFilter;
/// use ydb::traces::filter_ext::FilterExt;
///
/// let filter = EnvFilter::try_from_default_env()
///     .unwrap_or_else(|_| EnvFilter::new("info"))
///     .without_hyper();
/// ```
pub trait FilterExt {
    /// Suppress all spans and events from the `hyper` crate.
    ///
    /// Hyper is the HTTP/1.1 library used internally by tonic for gRPC
    /// transport. Disabling its logs significantly reduces noise at the
    /// `trace` level.
    fn without_hyper(self) -> Self;

    /// Suppress all spans and events from the `tonic` crate.
    ///
    /// Tonic is the gRPC framework. Its `trace`-level output is typically
    /// only useful when debugging the transport layer itself.
    fn without_tonic(self) -> Self;

    /// Suppress all spans and events from the `h2` crate.
    ///
    /// H2 is the HTTP/2 library. It is even more verbose than hyper and
    /// is a common source of unwanted noise during SDK development.
    fn without_h2(self) -> Self;

    /// Suppress all spans and events from the `reqwest` crate.
    ///
    /// Reqwest is used for IAM token exchange calls. It is only exercised
    /// when the SDK is configured with token-based authentication.
    fn without_reqwest(self) -> Self;

    /// Suppress all spans and events from the `tower` crate.
    ///
    /// Tower is a middleware framework used by tonic. Its `trace`-level output
    /// adds noise when debugging higher-level SDK instrumentation.
    fn without_tower(self) -> Self;

    /// Suppress all spans and events from `hyper`, `tonic`, `h2`, `reqwest`, and `tower`
    /// in a single call.
    ///
    /// Equivalent to chaining all five `without_*` methods. This is the quickest way
    /// to silence every common source of transport-layer noise.
    fn without_transport(self) -> Self;
}

impl FilterExt for EnvFilter {
    fn without_hyper(self) -> Self {
        self.add_directive(
            "hyper=off"
                .parse()
                .expect("invalid filter directive 'hyper=off'"),
        )
    }

    fn without_tonic(self) -> Self {
        self.add_directive(
            "tonic=off"
                .parse()
                .expect("invalid filter directive 'tonic=off'"),
        )
    }

    fn without_h2(self) -> Self {
        self.add_directive("h2=off".parse().expect("invalid filter directive 'h2=off'"))
    }

    fn without_reqwest(self) -> Self {
        self.add_directive(
            "reqwest=off"
                .parse()
                .expect("invalid filter directive 'reqwest=off'"),
        )
    }

    fn without_tower(self) -> Self {
        self.add_directive(
            "tower=off"
                .parse()
                .expect("invalid filter directive 'tower=off'"),
        )
    }

    fn without_transport(self) -> Self {
        self.without_hyper()
            .without_tonic()
            .without_h2()
            .without_reqwest()
            .without_tower()
    }
}
