use std::collections::HashMap;

use crate::grpc_wrapper::raw_query_service::execute_query::RawExecuteQueryRequest;

use super::exec::{build_client_execute_request_for_test, CallOptions};

fn sample_request() -> RawExecuteQueryRequest {
    RawExecuteQueryRequest::new("", "SELECT 1", HashMap::new(), None, false)
}

#[test]
fn new_execute_request_defaults_concurrent_result_sets_to_false() {
    let req = sample_request();
    assert!(!req.concurrent_result_sets);
    let proto = req.into_proto().expect("valid proto");
    assert!(!proto.concurrent_result_sets);
}

#[test]
fn finish_execute_request_sets_concurrent_result_sets_on_request_and_proto() {
    for want in [true, false] {
        let req = build_client_execute_request_for_test(&CallOptions::default(), want);
        assert_eq!(req.concurrent_result_sets, want);
        let proto = req.into_proto().expect("valid proto");
        assert_eq!(proto.concurrent_result_sets, want);
    }
}

/// Materialized client calls (`exec`, `query_row`, `query_result_set`) pass `true`.
#[test]
fn materialized_client_query_sets_concurrent_result_sets() {
    let req = build_client_execute_request_for_test(&CallOptions::default(), true);
    assert!(req.concurrent_result_sets);
    let proto = req.into_proto().expect("valid proto");
    assert!(proto.concurrent_result_sets);
}

/// Streaming [`QueryExecutor::query`] passes `false`.
#[test]
fn streaming_client_query_clears_concurrent_result_sets() {
    let req = build_client_execute_request_for_test(&CallOptions::default(), false);
    assert!(!req.concurrent_result_sets);
    let proto = req.into_proto().expect("valid proto");
    assert!(!proto.concurrent_result_sets);
}
