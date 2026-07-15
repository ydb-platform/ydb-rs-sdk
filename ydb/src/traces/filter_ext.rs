use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::Targets;

/// Extension trait for [`EnvFilter`] that silences verbose logs from HTTP/gRPC
/// dependency crates (hyper, tonic, h2, reqwest, tower).
///
/// These crates emit a large volume of `trace`-level spans that drown out the SDK's
/// own instrumentation. Each method adds a directive that suppresses *all* events
/// and spans from the corresponding target.
///
/// # Example
///
/// ```no_run
/// use tracing_subscriber::EnvFilter;
/// use ydb::traces::filter_ext::EnvFilterExt;
///
/// let filter = EnvFilter::try_from_default_env()
///     .unwrap_or_else(|_| EnvFilter::new("info"))
///     .without_transport();
/// ```
pub trait EnvFilterExt {
    /// Suppress all spans and events from the `hyper` crate.
    fn without_hyper(self) -> Self;
    /// Suppress all spans and events from the `tonic` crate.
    fn without_tonic(self) -> Self;
    /// Suppress all spans and events from the `h2` crate.
    fn without_h2(self) -> Self;
    /// Suppress all spans and events from the `reqwest` crate.
    fn without_reqwest(self) -> Self;
    /// Suppress all spans and events from the `tower` crate.
    fn without_tower(self) -> Self;
    /// Suppress all spans and events from `hyper`, `tonic`, `h2`, `reqwest`,
    /// and `tower` in a single call.
    fn without_transport(self) -> Self;
}

impl EnvFilterExt for EnvFilter {
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

/// Extension trait for [`Targets`] that controls the log level of individual
/// SDK modules.
///
/// The SDK emits `trace`-level spans from many internal modules (session pool,
/// query execution, table operations, topic writer/reader, etc.). These methods
/// let you raise or silence a specific module independently of the rest.
///
/// # Example
///
/// ```no_run
/// use tracing::level_filters::LevelFilter;
/// use tracing_subscriber::filter::Targets;
/// use ydb::traces::filter_ext::TargetFilterExt;
///
/// let targets = Targets::new()
///     .with_target("ydb", LevelFilter::TRACE)
///     .without_session_pool()
///     .without_client_query();
/// ```
pub trait TargetFilterExt {
    /// Set the [`LevelFilter`] for the `ydb::session_pool` target.
    fn with_session_pool_level(self, level: LevelFilter) -> Self;
    /// Suppress all spans and events from the `ydb::session_pool` target.
    fn without_session_pool(self) -> Self;

    /// Set the [`LevelFilter`] for the `ydb::client_query` target
    /// (query execution, retries, transactions).
    fn with_client_query_level(self, level: LevelFilter) -> Self;
    /// Suppress all spans and events from the `ydb::client_query` target.
    fn without_client_query(self) -> Self;

    /// Set the [`LevelFilter`] for the `ydb::client_table` target
    /// (table DDL/DML, bulk upsert, describe).
    fn with_client_table_level(self, level: LevelFilter) -> Self;
    /// Suppress all spans and events from the `ydb::client_table` target.
    fn without_client_table(self) -> Self;

    /// Set the [`LevelFilter`] for the `ydb::client_scheme` target
    /// (directory listing, permissions).
    fn with_client_scheme_level(self, level: LevelFilter) -> Self;
    /// Suppress all spans and events from the `ydb::client_scheme` target.
    fn without_client_scheme(self) -> Self;

    /// Set the [`LevelFilter`] for the `ydb::client_coordination` target.
    fn with_client_coordination_level(self, level: LevelFilter) -> Self;
    /// Suppress all spans and events from the `ydb::client_coordination` target.
    fn without_client_coordination(self) -> Self;

    /// Set the [`LevelFilter`] for the `ydb::client_topic` target
    /// (topic client, writer, reader).
    fn with_client_topic_level(self, level: LevelFilter) -> Self;
    /// Suppress all spans and events from the `ydb::client_topic` target.
    fn without_client_topic(self) -> Self;

    /// Set the [`LevelFilter`] for the `ydb::client_operation` target.
    fn with_client_operation_level(self, level: LevelFilter) -> Self;
    /// Suppress all spans and events from the `ydb::client_operation` target.
    fn without_client_operation(self) -> Self;

    /// Set the [`LevelFilter`] for the `ydb::connection_pool` target
    /// (gRPC channel pool, TLS certificate loading).
    fn with_connection_pool_level(self, level: LevelFilter) -> Self;
    /// Suppress all spans and events from the `ydb::connection_pool` target.
    fn without_connection_pool(self) -> Self;

    /// Set the [`LevelFilter`] for the `ydb::grpc_connection_manager` target
    /// (endpoint resolution, authenticated gRPC client creation).
    fn with_grpc_connection_manager_level(self, level: LevelFilter) -> Self;
    /// Suppress all spans and events from the `ydb::grpc_connection_manager` target.
    fn without_grpc_connection_manager(self) -> Self;

    /// Set the [`LevelFilter`] for the `ydb::discovery` target
    /// (background endpoint discovery).
    fn with_discovery_level(self, level: LevelFilter) -> Self;
    /// Suppress all spans and events from the `ydb::discovery` target.
    fn without_discovery(self) -> Self;

    /// Suppress all SDK internal spans (`ydb::*`) except gRPC transport in a single call.
    fn without_sdk(self) -> Self;
}

impl TargetFilterExt for Targets {
    fn with_session_pool_level(self, level: LevelFilter) -> Self {
        self.with_target("ydb::session_pool", level)
            .with_target("ydb::session_pool::pool", level)
    }

    fn without_session_pool(self) -> Self {
        self.with_session_pool_level(LevelFilter::OFF)
    }

    fn with_client_query_level(self, level: LevelFilter) -> Self {
        self.with_target("ydb::client_query", level)
            .with_target("ydb::client_query::exec", level)
            .with_target("ydb::client_query::builders", level)
            .with_target("ydb::client_query::script", level)
    }

    fn without_client_query(self) -> Self {
        self.with_client_query_level(LevelFilter::OFF)
    }

    fn with_client_table_level(self, level: LevelFilter) -> Self {
        self.with_target("ydb::client_table", level)
            .with_target("ydb::client_table::call_options", level)
    }

    fn without_client_table(self) -> Self {
        self.with_client_table_level(LevelFilter::OFF)
    }

    fn with_client_scheme_level(self, level: LevelFilter) -> Self {
        self.with_target("ydb::client_scheme", level)
    }

    fn without_client_scheme(self) -> Self {
        self.with_client_scheme_level(LevelFilter::OFF)
    }

    fn with_client_coordination_level(self, level: LevelFilter) -> Self {
        self.with_target("ydb::client_coordination", level)
    }

    fn without_client_coordination(self) -> Self {
        self.with_client_coordination_level(LevelFilter::OFF)
    }

    fn with_client_topic_level(self, level: LevelFilter) -> Self {
        self.with_target("ydb::client_topic", level)
            .with_target("ydb::client_topic::client", level)
            .with_target("ydb::client_topic::topicwriter::writer", level)
            .with_target("ydb::client_topic::topicwriter::writer_tx", level)
            .with_target("ydb::client_topic::topicreader::reader", level)
            .with_target("ydb::client_topic::topicreader::reader_tx", level)
    }

    fn without_client_topic(self) -> Self {
        self.with_client_topic_level(LevelFilter::OFF)
    }

    fn with_client_operation_level(self, level: LevelFilter) -> Self {
        self.with_target("ydb::client_operation", level)
    }

    fn without_client_operation(self) -> Self {
        self.with_client_operation_level(LevelFilter::OFF)
    }

    fn with_connection_pool_level(self, level: LevelFilter) -> Self {
        self.with_target("ydb::connection_pool", level)
    }

    fn without_connection_pool(self) -> Self {
        self.with_connection_pool_level(LevelFilter::OFF)
    }

    fn with_grpc_connection_manager_level(self, level: LevelFilter) -> Self {
        self.with_target("ydb::grpc_connection_manager", level)
    }

    fn without_grpc_connection_manager(self) -> Self {
        self.with_grpc_connection_manager_level(LevelFilter::OFF)
    }

    fn with_discovery_level(self, level: LevelFilter) -> Self {
        self.with_target("ydb::discovery", level)
    }

    fn without_discovery(self) -> Self {
        self.with_discovery_level(LevelFilter::OFF)
    }

    fn without_sdk(self) -> Self {
        self.without_session_pool()
            .without_client_query()
            .without_client_table()
            .without_client_scheme()
            .without_client_coordination()
            .without_client_topic()
            .without_client_operation()
            .without_connection_pool()
            .without_grpc_connection_manager()
            .without_discovery()
    }
}
