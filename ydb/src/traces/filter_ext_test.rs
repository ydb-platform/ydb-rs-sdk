use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::Targets;

use super::filter_ext::{EnvFilterExt, TargetFilterExt};

// ── EnvFilterExt ──────────────────────────────────────────────────

#[test]
fn without_hyper() {
    let filter = EnvFilter::new("info").without_hyper();
    let s = format!("{filter}");
    assert!(s.contains("hyper=off"), "expected hyper=off in '{s}'");
}

#[test]
fn without_tonic() {
    let filter = EnvFilter::new("info").without_tonic();
    let s = format!("{filter}");
    assert!(s.contains("tonic=off"), "expected tonic=off in '{s}'");
}

#[test]
fn without_h2() {
    let filter = EnvFilter::new("info").without_h2();
    let s = format!("{filter}");
    assert!(s.contains("h2=off"), "expected h2=off in '{s}'");
}

#[test]
fn without_reqwest() {
    let filter = EnvFilter::new("info").without_reqwest();
    let s = format!("{filter}");
    assert!(s.contains("reqwest=off"), "expected reqwest=off in '{s}'");
}

#[test]
fn without_tower() {
    let filter = EnvFilter::new("info").without_tower();
    let s = format!("{filter}");
    assert!(s.contains("tower=off"), "expected tower=off in '{s}'");
}

#[test]
fn without_transport_contains_all_five_directives() {
    let filter = EnvFilter::new("info").without_transport();
    let s = format!("{filter}");
    for directive in &[
        "hyper=off",
        "tonic=off",
        "h2=off",
        "reqwest=off",
        "tower=off",
    ] {
        assert!(s.contains(directive), "expected {directive} in '{s}'");
    }
}

#[test]
fn chaining_preserves_base_level() {
    let filter = EnvFilter::new("warn").without_hyper().without_tonic();
    let s = format!("{filter}");
    assert!(s.contains("warn"), "expected warn in '{s}'");
    assert!(s.contains("hyper=off"));
    assert!(s.contains("tonic=off"));
}

#[test]
fn try_from_default_env_fallback_without_transport() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"))
        .without_transport();
    let s = format!("{filter}");
    assert!(s.contains("info"), "expected info in '{s}'");
    assert!(s.contains("hyper=off"));
}

#[test]
fn empty_envfilter_accepts_transport_directives() {
    let filter = EnvFilter::new("")
        .without_hyper()
        .without_tonic()
        .without_h2();
    let s = format!("{filter}");
    assert!(s.contains("hyper=off"));
    assert!(s.contains("tonic=off"));
    assert!(s.contains("h2=off"));
}

// ── TargetFilterExt ───────────────────────────────────────────────

#[test]
fn with_session_pool_level() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .with_session_pool_level(LevelFilter::DEBUG);
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::session_pool=debug"),
        "expected ydb::session_pool=debug in '{s}'"
    );
}

#[test]
fn without_session_pool() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .without_session_pool();
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::session_pool=off"),
        "expected ydb::session_pool=off in '{s}'"
    );
}

#[test]
fn with_client_query_level() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .with_client_query_level(LevelFilter::WARN);
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::client_query=warn"),
        "expected ydb::client_query=warn in '{s}'"
    );
}

#[test]
fn without_client_query() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .without_client_query();
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::client_query=off"),
        "expected ydb::client_query=off in '{s}'"
    );
}

#[test]
fn with_client_table_level() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .with_client_table_level(LevelFilter::ERROR);
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::client_table=error"),
        "expected ydb::client_table=error in '{s}'"
    );
}

#[test]
fn without_client_table() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .without_client_table();
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::client_table=off"),
        "expected ydb::client_table=off in '{s}'"
    );
}

#[test]
fn with_client_scheme_level() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .with_client_scheme_level(LevelFilter::INFO);
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::client_scheme=info"),
        "expected ydb::client_scheme=info in '{s}'"
    );
}

#[test]
fn without_client_scheme() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .without_client_scheme();
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::client_scheme=off"),
        "expected ydb::client_scheme=off in '{s}'"
    );
}

#[test]
fn with_client_coordination_level() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .with_client_coordination_level(LevelFilter::DEBUG);
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::client_coordination=debug"),
        "expected ydb::client_coordination=debug in '{s}'"
    );
}

#[test]
fn without_client_coordination() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .without_client_coordination();
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::client_coordination=off"),
        "expected ydb::client_coordination=off in '{s}'"
    );
}

#[test]
fn with_client_topic_level() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .with_client_topic_level(LevelFilter::WARN);
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::client_topic=warn"),
        "expected ydb::client_topic=warn in '{s}'"
    );
}

#[test]
fn without_client_topic() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .without_client_topic();
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::client_topic=off"),
        "expected ydb::client_topic=off in '{s}'"
    );
}

#[test]
fn with_client_operation_level() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .with_client_operation_level(LevelFilter::ERROR);
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::client_operation=error"),
        "expected ydb::client_operation=error in '{s}'"
    );
}

#[test]
fn without_client_operation() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .without_client_operation();
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::client_operation=off"),
        "expected ydb::client_operation=off in '{s}'"
    );
}

#[test]
fn with_connection_pool_level() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .with_connection_pool_level(LevelFilter::INFO);
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::connection_pool=info"),
        "expected ydb::connection_pool=info in '{s}'"
    );
}

#[test]
fn without_connection_pool() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .without_connection_pool();
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::connection_pool=off"),
        "expected ydb::connection_pool=off in '{s}'"
    );
}

#[test]
fn with_grpc_connection_manager_level() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .with_grpc_connection_manager_level(LevelFilter::DEBUG);
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::grpc_connection_manager=debug"),
        "expected ydb::grpc_connection_manager=debug in '{s}'"
    );
}

#[test]
fn without_grpc_connection_manager() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .without_grpc_connection_manager();
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::grpc_connection_manager=off"),
        "expected ydb::grpc_connection_manager=off in '{s}'"
    );
}

#[test]
fn with_discovery_level() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .with_discovery_level(LevelFilter::WARN);
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::discovery=warn"),
        "expected ydb::discovery=warn in '{s}'"
    );
}

#[test]
fn without_discovery() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .without_discovery();
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::discovery=off"),
        "expected ydb::discovery=off in '{s}'"
    );
}

#[test]
fn without_sdk_suppresses_all_modules() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .without_sdk();
    let s = format!("{targets}");

    let expected_modules = [
        "ydb::session_pool",
        "ydb::client_query",
        "ydb::client_table",
        "ydb::client_scheme",
        "ydb::client_coordination",
        "ydb::client_topic",
        "ydb::client_operation",
        "ydb::connection_pool",
        "ydb::grpc_connection_manager",
        "ydb::discovery",
    ];

    for module in &expected_modules {
        let expected = format!("{module}=off");
        assert!(s.contains(&expected), "expected '{expected}' in '{s}'");
    }

    assert!(s.contains("ydb=trace"), "expected base ydb=trace in '{s}'");
}

#[test]
fn override_with_lower_level_then_off() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .with_session_pool_level(LevelFilter::DEBUG)
        .without_session_pool();
    let s = format!("{targets}");
    assert!(
        s.contains("ydb::session_pool=off"),
        "expected off to override debug in '{s}'"
    );
}

#[test]
fn multi_target_combination() {
    let targets = Targets::new()
        .with_target("ydb", LevelFilter::TRACE)
        .with_client_query_level(LevelFilter::WARN)
        .with_client_table_level(LevelFilter::ERROR);
    let s = format!("{targets}");
    assert!(s.contains("ydb::client_query=warn"));
    assert!(s.contains("ydb::client_table=error"));
    assert!(s.contains("ydb=trace"));
}
